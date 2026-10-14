/* eslint-disable camelcase, functional/no-throw-statement, @typescript-eslint/parameter-properties, @typescript-eslint/naming-convention, complexity, max-classes-per-file, @typescript-eslint/no-magic-numbers, @typescript-eslint/member-ordering, max-params, functional/no-try-statement */
// cspell:ignore clickhouse cgapi txid
/**
 * The API's access to ClickHouse: query counters (tests assert one
 * `readSnapshot` per request and per subscription re-run, and count data
 * queries), node-name resolution, and the per-request pinned snapshot.
 */
import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';

import { ClickHouseError } from '@clickhouse/client';
import { GraphQLError } from 'graphql';

import type {
  ClickHouseClient,
  ClickHouseSettings,
  QueryParams,
} from '../store/clickhouse/client.js';
import { isRequestTimeout } from '../store/clickhouse/client.js';
import type {
  MultiNodeSnapshot,
  VisibilitySnapshot,
} from '../store/clickhouse/visibility.js';
import {
  nodeAgnosticId,
  readSnapshot,
  SnapshotCache,
  snapshotForNode,
} from '../store/clickhouse/visibility.js';

import type { AdmissionOptions, StatementRecord, Ticket } from './admission.js';
import {
  Admission,
  disabledAdmissionOptions,
  requestAbortedError,
  statementShape,
} from './admission.js';
import type { Ctx } from './compile.js';
import { maxAuthchainDepth } from './model.js';
import type { UtxoSetPolicy } from './sql.js';
import {
  acceptanceWindowRows,
  authchainTooWideCode,
  candidateWindowRows,
  defaultAuthchainMaxBranches,
  defaultUtxoSetMaxLagRows,
  defaultUtxoSetPolicy,
  readsByIdFrom,
  Sql,
} from './sql.js';

/**
 * Settings of every data query. `enable_materialized_cte`: a root page with
 * relationships is a `WITH page AS MATERIALIZED (…)` CTE (compile.ts).
 */
export const dataQuerySettings = {
  /* the client gave up (request timeout, aborted request): stop the statement too */
  cancel_http_readonly_queries_on_client_close: 1,
  enable_materialized_cte: 1,
  /*
   * The authchain walk (model.ts) takes two recursion steps per segment end,
   * and per link above the spend watermark or without an index row: up to
   * two per link of v1's maximum depth.
   */
  max_recursive_cte_evaluation_depth: 2 * maxAuthchainDepth + 10,
} as unknown as ClickHouseSettings;

/**
 * Statement classes with their own server-side time budget: `aggregate`
 * (`*_aggregate` and `*_groups` roots: one statement over a whole row set,
 * which on mainnet can be the whole chain) and `data` (everything else).
 */
export type StatementClass = 'aggregate' | 'data';

/** Server-side time budgets (`max_execution_time`) per statement class, in ms. */
export interface StatementTimeouts {
  data: number;
  aggregate: number;
}

/**
 * Defaults: a data statement may take a minute (golden mainnet worst
 * catalogue statement: X16 8 s warm, L33 16 s cold); an aggregate 30 s
 * (golden: the whole-chain UTXO count ran 1,171 s and, through the API,
 * timed out at 300 s and was retried, doubling the load).
 */
export const defaultStatementTimeouts: StatementTimeouts = {
  aggregate: 30_000,
  data: 60_000,
};

/**
 * The stored UTXO set policy from the environment: CHAINGRAPH_API_UTXO_SET
 * (`on` | `off`, default `on`: unspent reads use the stored set when the
 * store has one for the node; docs/clickhouse-port/utxo-async.md) and
 * CHAINGRAPH_API_UTXO_SET_MAX_LAG_ROWS (plan bound, default 100,000).
 */
export const utxoSetPolicyFromEnv = (
  env: NodeJS.ProcessEnv = process.env
): UtxoSetPolicy => {
  const flag = env.CHAINGRAPH_API_UTXO_SET ?? 'on';
  if (flag !== 'on' && flag !== 'off' && flag !== '') {
    throw new Error(`CHAINGRAPH_API_UTXO_SET must be 'on' or 'off'.`);
  }
  const raw = env.CHAINGRAPH_API_UTXO_SET_MAX_LAG_ROWS;
  const maxLagRows =
    raw === undefined || raw === '' ? defaultUtxoSetMaxLagRows : Number(raw);
  if (!Number.isInteger(maxLagRows) || maxLagRows < 0) {
    throw new Error(
      'CHAINGRAPH_API_UTXO_SET_MAX_LAG_ROWS must be an integer >= 0.'
    );
  }
  return { enabled: flag !== 'off', maxLagRows };
};

/**
 * The `authchain_branches` cap per base from the environment:
 * CHAINGRAPH_API_AUTHCHAIN_MAX_BRANCHES (default 1,000; owner decision
 * 2026-10-12).
 */
export const authchainMaxBranchesFromEnv = (
  env: NodeJS.ProcessEnv = process.env
) => {
  const raw = env.CHAINGRAPH_API_AUTHCHAIN_MAX_BRANCHES;
  if (raw === undefined || raw === '') return defaultAuthchainMaxBranches;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(
      'CHAINGRAPH_API_AUTHCHAIN_MAX_BRANCHES must be an integer >= 1.'
    );
  }
  return value;
};

/** ClickHouse FUNCTION_THROW_IF_VALUE_IS_NON_ZERO (`throwIf`). */
const throwIfCode = '395';

/** Added to the longest budget for the client's own request timeout, so the server's error arrives first. */
export const requestTimeoutMarginMs = 5_000;

/**
 * Budgets from the environment: CHAINGRAPH_API_QUERY_TIMEOUT_MS (data
 * statements; default 60 s) and CHAINGRAPH_API_AGGREGATE_TIMEOUT_MS
 * (default: the smaller of 30 s and the data budget).
 */
export const statementTimeoutsFromEnv = (
  env: NodeJS.ProcessEnv = process.env
): StatementTimeouts => {
  const positive = (name: string) => {
    const raw = env[name];
    if (raw === undefined || raw === '') return undefined;
    const value = Number(raw);
    if (!Number.isFinite(value) || value <= 0) {
      throw new Error(`${name} must be a positive number of milliseconds.`);
    }
    return value;
  };
  const data =
    positive('CHAINGRAPH_API_QUERY_TIMEOUT_MS') ??
    defaultStatementTimeouts.data;
  const aggregate =
    positive('CHAINGRAPH_API_AGGREGATE_TIMEOUT_MS') ??
    Math.min(defaultStatementTimeouts.aggregate, data);
  return { aggregate, data };
};

/** What a statement knows of the request it belongs to (`requestScope`). */
export interface RequestScope {
  /** Fires when the HTTP client goes away or a graphql-ws operation ends. */
  signal?: AbortSignal;
  /** The request's admission ticket (admission.ts), once admitted. */
  ticket?: Ticket;
  /** An HTTP request (server.ts `listen`): its ticket ends when the response closes. */
  http?: boolean;
  /** The response cache was looked up already (front.ts): the cache plugin only stores. */
  cacheChecked?: boolean;
  /** The request snapshots of the request's executions (a heavy-lane worker reports their marks). */
  snapshots?: PinnedSnapshot[];
}

/**
 * The request a statement belongs to: its abort signal fires when the HTTP
 * client goes away (server.ts `listen`) or a graphql-ws operation ends.
 * Every data statement of the request reads it: unsent statements are never
 * sent, running ones are cancelled (the HTTP request is closed and the
 * statement is killed by its query_id). The admission ticket's class picks
 * the statements' ClickHouse settings, and each statement is recorded on it
 * (its rows, its query_id for the measured CPU).
 */
export const requestScope = new AsyncLocalStorage<RequestScope>();

/** The prefix of every API statement's `query_id` (system.query_log, KILL QUERY). */
export const apiQueryIdPrefix = 'cgapi-';

/** ClickHouse TIMEOUT_EXCEEDED (max_execution_time). */
const timeoutExceededCode = '159';

export interface ApiStats {
  /** Snapshot reads: one per request (`pinnedSnapshot`) and per live re-run (`readSnapshot`). */
  snapshots: number;
  /** Request snapshots served by the cache (watermarks unchanged: one cheap query). */
  snapshotCacheHits: number;
  /** Data queries (everything except snapshot reads, node lookups and watermark polls). */
  dataQueries: number;
  /** Node-name lookups (cached per process). */
  nodeLookups: number;
  /** Watermark polls of the live-query hub. */
  watermarkPolls: number;
  /** Requests run on a cached snapshot the watermark check then found stale, run again (`speculativeSnapshot`). */
  speculativeRetries: number;
}

/** Optional store objects the compiler adapts to (DDL items, the stored UTXO set). */
export interface StoreFeatures {
  /** tx_acceptance has the projection p_node_tx (ddl/optional/p_node_tx.sql). */
  acceptanceByIdProjection: boolean;
  /** Every active part of `input` has p_txid (`Sql.readsById`). */
  inputsByIdProjection?: boolean;
  /** Every active part of `block_transaction` has p_txid (`Sql.readsById`). */
  inclusionsByIdProjection?: boolean;
  /**
   * The database has the stored UTXO set's tables (ddl/090_utxo_set.sql):
   * snapshots then read each node's mark (whether a node has a set yet is
   * per snapshot).
   */
  utxoSet?: boolean;
}

export interface QueryEvent {
  kind: 'data' | 'nodeLookup' | 'snapshot' | 'storeFeatures' | 'watermarkPoll';
  sql: string;
  params: QueryParams;
  label: string;
}

export class ApiDb {
  readonly stats: ApiStats = {
    dataQueries: 0,
    nodeLookups: 0,
    snapshotCacheHits: 0,
    snapshots: 0,
    speculativeRetries: 0,
    watermarkPolls: 0,
  };

  /**
   * Run a request on its cached snapshot while the watermark check runs
   * beside it (`speculativeSnapshot`); CHAINGRAPH_API_SPECULATIVE_SNAPSHOT
   * = off checks first (one round trip more per request).
   */
  speculativeSnapshots =
    process.env.CHAINGRAPH_API_SPECULATIVE_SNAPSHOT !== 'off';

  /** Rows (all levels) one root statement may return (R33): more is an error. */
  maxResultRows = 500_000;

  /**
   * Relationship levels matched by group numbers (fix-pass-10.md §1);
   * CHAINGRAPH_API_GROUP_NUMBERS=off matches them by key texts (fix pass 9's
   * statements: the A/B switch of the next GKE session).
   */
  groupNumbers = process.env.CHAINGRAPH_API_GROUP_NUMBERS !== 'off';

  /** Test hook: every query the API sends (after it is sent). */
  onQuery: ((event: QueryEvent) => void) | undefined;

  private readonly nodeIds = new Map<string, number>();

  /** How long a store-feature check is reused (an optional item added later is noticed). */
  featureTtlMs = 60_000;

  /** Raw acceptance rows bounding an `acceptedInIdOrder` window (tests force the fallback with a small value). */
  acceptanceWindowRows: (need: number) => number = acceptanceWindowRows;

  /** Candidates a `transactions` page in id order checks first (tests force the fallback with a small value). */
  candidateWindowRows: (need: number, residual?: boolean) => number =
    candidateWindowRows;

  /** Tests: fix the features instead of reading them. */
  forceFeatures: StoreFeatures | undefined;

  private features: { at: number; value: Promise<StoreFeatures> } | undefined;

  /**
   * The optional store objects present (one query per `featureTtlMs`, only
   * when a root can use them). Not part of any snapshot: they change plans,
   * never results.
   */
  async storeFeatures(): Promise<StoreFeatures> {
    if (this.forceFeatures !== undefined) return this.forceFeatures;
    const now = Date.now();
    if (
      this.features === undefined ||
      now - this.features.at > this.featureTtlMs
    ) {
      /* by id: the projection in EVERY active part (a part without it would be read whole) */
      const everyPart = (
        table: string
      ) => `(SELECT count() > 0 AND countIf(NOT has(projections, 'p_txid')) = 0
         FROM system.parts WHERE database = currentDatabase() AND table = '${table}' AND active)`;
      const sql = `SELECT
        (SELECT count() FROM system.projections
         WHERE database = currentDatabase() AND table = 'tx_acceptance' AND name = 'p_node_tx') AS n,
        (SELECT count() FROM system.tables
         WHERE database = currentDatabase() AND name = 'utxo_watermark') AS u,
        ${everyPart('input')} AS i,
        ${everyPart('block_transaction')} AS b`;
      const value = this.client
        .query<{
          n: number | string;
          u: number | string;
          i: number | string;
          b: number | string;
        }>(sql)
        .then((rows) => {
          this.onQuery?.({
            kind: 'storeFeatures',
            label: 'storeFeatures',
            params: {},
            sql,
          });
          return {
            acceptanceByIdProjection: Number(rows[0]?.n ?? 0) > 0,
            inclusionsByIdProjection: Number(rows[0]?.b ?? 0) > 0,
            inputsByIdProjection: Number(rows[0]?.i ?? 0) > 0,
            utxoSet: Number(rows[0]?.u ?? 0) > 0,
          };
        });
      this.features = { at: now, value };
      value.catch(() => {
        this.features = undefined;
      });
    }
    return this.features.value;
  }

  /** The snapshot client: every query is reported as a `snapshot` event. */
  private readonly snapshotClient = {
    query: async <T>(sql: string, params?: QueryParams) => {
      const rows = await this.client.query<T>(sql, params);
      this.onQuery?.({
        kind: 'snapshot',
        label: 'readSnapshot',
        params: params ?? {},
        sql,
      });
      return rows;
    },
  };

  private readonly snapshotCache = new SnapshotCache(this.snapshotClient);

  /** Server-side time budget per statement class (`max_execution_time`). */
  timeouts: StatementTimeouts = { ...defaultStatementTimeouts };

  /**
   * Admission control of requests (admission.ts; off unless set: main.ts
   * sets it from the environment, with the server's cores).
   */
  admission = new Admission(disabledAdmissionOptions);

  /** Replace the admission options (a fresh gate, started: loop attribution and the CPU ledger). */
  setAdmission(options: AdmissionOptions) {
    this.admission.stop();
    this.admission = new Admission(options);
    this.admission.start(async (ids) => this.statementCpu(ids));
  }

  /**
   * Measured ClickHouse CPU (user + system, µs) of finished API statements
   * by query_id, from `system.query_log` (the API user needs SELECT on it;
   * rows appear after the log's flush interval). Missing ids: not logged yet.
   */
  async statementCpu(queryIds: readonly string[]) {
    const rows = await this.client.query<{ id: string; us: string }>(
      `SELECT query_id AS id, toString(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) AS us
       FROM system.query_log
       WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE
         AND type != 'QueryStart' AND query_id IN {ids:Array(String)}`,
      { ids: queryIds }
    );
    return new Map(rows.map((row) => [row.id, Number(row.us)]));
  }

  /** Statements cancelled because their request went away (counter). */
  cancelledStatements = 0;

  /** `authchain_branches` cap per base (`authchainMaxBranchesFromEnv`). */
  authchainMaxBranches = defaultAuthchainMaxBranches;

  /** Stored UTXO set use (`utxoSetPolicyFromEnv`). */
  utxoSetPolicy: UtxoSetPolicy = { ...defaultUtxoSetPolicy };

  /** Snapshot read options: the UTXO set marks when the policy and the store allow. */
  private async snapshotOptions() {
    if (!this.utxoSetPolicy.enabled) return {};
    const { utxoSet } = await this.storeFeatures();
    return utxoSet === true ? { utxo: true } : {};
  }

  constructor(readonly client: ClickHouseClient) {}

  /** Exactly one ClickHouse query: a fresh visibility snapshot of `nodeId`. */
  async readSnapshot(nodeId: number): Promise<VisibilitySnapshot> {
    this.stats.snapshots += 1;
    return readSnapshot(
      this.snapshotClient,
      nodeId,
      await this.snapshotOptions()
    );
  }

  /**
   * The snapshot of one request, pinning every node it names
   * (`SnapshotCache`): one cheap watermark query while the watermarks stand
   * still, else that plus one full snapshot read.
   */
  async pinnedSnapshot(nodeIds: readonly number[]): Promise<MultiNodeSnapshot> {
    this.stats.snapshots += 1;
    const { hits } = this.snapshotCache;
    /* the request waits for ClickHouse meanwhile (loop attribution, admission.ts) */
    const ticket = requestScope.getStore()?.ticket;
    if (ticket !== undefined) this.admission.waiting(ticket, 1);
    try {
      const snapshot = await this.snapshotCache.read(
        nodeIds,
        await this.snapshotOptions()
      );
      if (this.snapshotCache.hits > hits) this.stats.snapshotCacheHits += 1;
      return snapshot;
    } finally {
      if (ticket !== undefined) this.admission.waiting(ticket, -1);
    }
  }

  /**
   * The cached snapshot of a request's nodes, used at once, with its
   * watermark check running concurrently (fix-pass-11.md §1): `confirmed`
   * resolves true when the watermarks read (after the request started)
   * equal the snapshot's, the same condition `pinnedSnapshot` checks before
   * using it, so a confirmed result is what the checked path would have
   * answered. Undefined when nothing is cached (the request reads one).
   */
  async speculativeSnapshot(
    nodeIds: readonly number[]
  ): Promise<
    { snapshot: MultiNodeSnapshot; confirmed: Promise<boolean> } | undefined
  > {
    if (!this.speculativeSnapshots) return undefined;
    const options = await this.snapshotOptions();
    const snapshot = this.snapshotCache.peek(nodeIds, options);
    if (snapshot === undefined) return undefined;
    this.stats.snapshots += 1;
    const confirmed = this.snapshotCache
      .verify(nodeIds, options, snapshot)
      .then((same) => {
        if (same) {
          this.snapshotCache.hits += 1;
          this.stats.snapshotCacheHits += 1;
        } else {
          /* the next requests read a checked snapshot (one retry burst per commit) */
          this.snapshotCache.forget(nodeIds, options, snapshot);
        }
        return same;
      })
      .catch(() => false);
    return { confirmed, snapshot };
  }

  /**
   * One data statement, with its class's `max_execution_time`. A statement
   * over budget fails with a QUERY_TIMEOUT error (the server stops it); a
   * client-side request timeout is not retried (`retryTimeouts: false`).
   */
  async query<T>(
    label: string,
    sql: string,
    params: QueryParams,
    statementClass: StatementClass = 'data'
  ) {
    const scope = requestScope.getStore();
    const signal = scope?.signal;
    const ticket = scope?.ticket;
    const budgetMs = this.timeouts[statementClass];
    const { admission } = this;
    const send = async () => {
      if (signal?.aborted === true) throw requestAbortedError();
      this.stats.dataQueries += 1;
      const queryId = `${apiQueryIdPrefix}${randomUUID()}`;
      const statement: StatementRecord = {
        queryId,
        rows: 0,
        shape: admission.options.enabled ? statementShape(label, sql) : '',
      };
      ticket?.statements.push(statement);
      /* waiting for ClickHouse until its response starts (loop attribution) */
      const wait = { open: ticket !== undefined };
      if (ticket !== undefined) admission.waiting(ticket, 1);
      const responded = () => {
        if (!wait.open || ticket === undefined) return;
        wait.open = false;
        admission.waiting(ticket, -1);
      };
      const kill = () => {
        this.cancelledStatements += 1;
        this.client.kill(queryId).catch(() => undefined);
      };
      signal?.addEventListener('abort', kill, { once: true });
      try {
        return await this.client.query<T>(
          sql,
          params,
          {
            ...dataQuerySettings,
            ...(admission.options.enabled
              ? ticket?.settings ?? admission.options.settings.light
              : {}),
            max_execution_time: Math.max(1, Math.ceil(budgetMs / 1000)),
            timeout_overflow_mode: 'throw',
          } as unknown as ClickHouseSettings,
          {
            ...(signal === undefined ? {} : { abortSignal: signal }),
            onResponse: responded,
            onRowsRead: (rows) => {
              statement.rows = rows;
            },
            queryId,
            retryTimeouts: false,
          }
        );
      } finally {
        responded();
        signal?.removeEventListener('abort', kill);
        admission.statementFinished(ticket, statement);
      }
    };
    const rows = await send().catch((error: unknown) => {
      if (signal?.aborted === true && !(error instanceof GraphQLError)) {
        throw requestAbortedError();
      }
      if (
        error instanceof ClickHouseError &&
        error.code === throwIfCode &&
        error.message.includes(`${authchainTooWideCode}:`)
      ) {
        const start = error.message.indexOf(`${authchainTooWideCode}:`);
        const end = error.message.indexOf(': while executing', start);
        throw new GraphQLError(
          `${label}: ${error.message.slice(start, end < 0 ? undefined : end)}`,
          { extensions: { code: authchainTooWideCode } }
        );
      }
      const timedOut =
        (error instanceof ClickHouseError &&
          error.code === timeoutExceededCode) ||
        isRequestTimeout(error);
      if (!timedOut) throw error;
      throw new GraphQLError(
        `${label}: the statement exceeded the API's ${Math.ceil(
          budgetMs / 1000
        )} s limit for ${statementClass} statements and was stopped. Narrow it (filters, node, block window, limit).`,
        { extensions: { code: 'QUERY_TIMEOUT', statementClass } }
      );
    });
    this.onQuery?.({ kind: 'data', label, params, sql });
    return rows;
  }

  /** Watermarks of the given nodes, one query (the live-query hub's poll). */
  async watermarks(nodeIds: readonly number[]) {
    this.stats.watermarkPolls += 1;
    const sql = `SELECT node_internal_id AS node, toString(max(visible_seq)) AS visible
       FROM visibility WHERE node_internal_id IN {nodes:Array(UInt32)} GROUP BY node`;
    const rows = await this.client.query<{ node: number; visible: string }>(
      sql,
      { nodes: nodeIds }
    );
    this.onQuery?.({
      kind: 'watermarkPoll',
      label: 'watermarks',
      params: { nodes: nodeIds },
      sql,
    });
    return new Map(rows.map((row) => [Number(row.node), BigInt(row.visible)]));
  }

  /** A node's internal id by name (node rows are few and permanent: cached). */
  async nodeId(name: string): Promise<number> {
    const cached = this.nodeIds.get(name);
    if (cached !== undefined) return cached;
    this.stats.nodeLookups += 1;
    const [row] = await this.client.query<{ internal_id: number }>(
      'SELECT internal_id FROM node_v WHERE name = {name:String} ORDER BY internal_id LIMIT 1',
      { name }
    );
    if (row === undefined) {
      throw new GraphQLError(`Unknown node: ${name}`, {
        extensions: { code: 'BAD_USER_INPUT' },
      });
    }
    const id = Number(row.internal_id);
    this.nodeIds.set(name, id);
    return id;
  }
}

/** The SQL binder and compile context of one root statement. */
export interface StatementEnv {
  sql: Sql;
  ctx: Ctx;
}

const unknownNode = (name: string) =>
  new GraphQLError(
    `Node ${name} is not named by this operation (every node a request reads must appear as a \`node:\` or \`accepted_by:\` argument).`,
    { extensions: { code: 'BAD_USER_INPUT' } }
  );

/**
 * A statement environment from a single-node snapshot (a live re-run, which
 * pins exactly its subscription's node).
 */
export const singleNodeEnv = (
  snapshot: VisibilitySnapshot,
  nodeName: string | undefined,
  maxRows?: number,
  utxoSetPolicy?: UtxoSetPolicy,
  authchainMaxBranches?: number
): StatementEnv => {
  const sql = new Sql(snapshot, new Map(), utxoSetPolicy);
  const nodeId = (name: string) => {
    if (name !== nodeName) {
      throw new GraphQLError(
        'A subscription pins one node: every `node:` argument of it must name the same node.',
        { extensions: { code: 'BAD_USER_INPUT' } }
      );
    }
    return snapshot.nodeId;
  };
  sql.nodeIdOf = nodeId;
  if (authchainMaxBranches !== undefined) {
    sql.authchainMaxBranches = authchainMaxBranches;
  }
  return {
    ctx: {
      maxRows,
      node: snapshot.nodeId === nodeAgnosticId ? undefined : snapshot.nodeId,
      nodeId,
      sql,
    },
    sql,
  };
};

/**
 * The one snapshot of a request (or of one subscription re-run). The nodes
 * are fixed when the request starts (every `node:` argument of the
 * operation, at any depth); the snapshot is read once, on first use, pinning
 * all of them in one consistent read, and shared by every resolver: each
 * per-node root reads at its own node's `visible(n)`, every root shares
 * visible(0), the tail, void and fence, and a per-node relationship naming
 * another node reads that node's `visible(n)` from the same snapshot.
 * Node-agnostic roots ride on the same snapshot, so a multi-root operation
 * reads one consistent state.
 */
export class PinnedSnapshot {
  private pending: Promise<MultiNodeSnapshot> | undefined;

  /** The snapshot once read (the response cache keys on its watermarks). */
  resolved: MultiNodeSnapshot | undefined;

  private readonly ids = new Map<string, number>();

  constructor(
    private readonly db: ApiDb,
    private readonly nodeNames: readonly string[],
    private readonly preset?: VisibilitySnapshot
  ) {}

  /** For a root that names `node` (undefined: a node-agnostic root). */
  async get(node?: string): Promise<VisibilitySnapshot> {
    if (node !== undefined && !this.nodeNames.includes(node)) {
      throw unknownNode(node);
    }
    if (this.preset !== undefined) {
      if (this.nodeNames.length > 1) {
        throw new GraphQLError(
          'A subscription pins one node: its roots must name the same node.',
          { extensions: { code: 'BAD_USER_INPUT' } }
        );
      }
      return this.preset;
    }
    this.pending ??= this.read();
    const snapshot = await this.pending;
    return snapshotForNode(
      snapshot,
      node === undefined ? nodeAgnosticId : this.ids.get(node)!
    );
  }

  /**
   * The statement environment of a root naming `node` (undefined: a
   * node-agnostic root): its primary node's view parameters, and every other
   * pinned node for per-node relationships that name one.
   */
  async statement(node?: string): Promise<StatementEnv> {
    if (this.preset !== undefined) {
      await this.get(node);
      const env = singleNodeEnv(
        this.preset,
        this.nodeNames[0],
        this.db.maxResultRows,
        this.db.utxoSetPolicy,
        this.db.authchainMaxBranches
      );
      env.sql.readsById = readsByIdFrom(await this.db.storeFeatures());
      env.ctx.groupNumbers = this.db.groupNumbers;
      return env;
    }
    const snapshot = await this.get(node);
    const multi = await this.pending!;
    const others = new Map(
      [...multi.visibleByNode].filter(([id]) => id !== snapshot.nodeId)
    );
    const sql = new Sql(snapshot, others, this.db.utxoSetPolicy);
    const nodeId = (name: string) => {
      const id = this.ids.get(name);
      if (id === undefined) throw unknownNode(name);
      return id;
    };
    sql.nodeIdOf = nodeId;
    sql.authchainMaxBranches = this.db.authchainMaxBranches;
    sql.readsById = readsByIdFrom(await this.db.storeFeatures());
    return {
      ctx: {
        groupNumbers: this.db.groupNumbers,
        maxRows: this.db.maxResultRows,
        node: snapshot.nodeId === nodeAgnosticId ? undefined : snapshot.nodeId,
        nodeId,
        sql,
      },
      sql,
    };
  }

  /** The concurrent watermark check of a speculative snapshot (`ApiDb.speculativeSnapshot`). */
  private confirmation: Promise<boolean> | undefined;

  private speculate = true;

  /**
   * Whether the request's result stands: true unless it ran on a cached
   * snapshot whose watermark check found newer marks (then `refresh` and
   * run it again).
   */
  async confirmed() {
    return this.confirmation === undefined ? true : this.confirmation;
  }

  /** Read the snapshot again, checked first (after a failed speculation). */
  refresh() {
    this.speculate = false;
    this.pending = undefined;
    this.resolved = undefined;
    this.confirmation = undefined;
  }

  private async read() {
    const ids = await Promise.all(
      this.nodeNames.map(async (name) => {
        const id = await this.db.nodeId(name);
        this.ids.set(name, id);
        return id;
      })
    );
    const nodes = ids.length === 0 ? [nodeAgnosticId] : ids;
    const speculative = this.speculate
      ? await this.db.speculativeSnapshot(nodes)
      : undefined;
    if (speculative !== undefined) {
      this.confirmation = speculative.confirmed;
      this.resolved = speculative.snapshot;
      return this.resolved;
    }
    this.resolved = await this.db.pinnedSnapshot(nodes);
    return this.resolved;
  }
}
