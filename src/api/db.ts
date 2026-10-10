/* eslint-disable camelcase, functional/no-throw-statement, @typescript-eslint/parameter-properties, @typescript-eslint/naming-convention, complexity, max-classes-per-file */
// cspell:ignore clickhouse
/**
 * The API's access to ClickHouse: query counters (tests assert one
 * `readSnapshot` per request and per subscription re-run, and count data
 * queries), node-name resolution, and the per-request pinned snapshot.
 */
import { GraphQLError } from 'graphql';

import type {
  ClickHouseClient,
  ClickHouseSettings,
  QueryParams,
} from '../store/clickhouse/client.js';
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

/**
 * Settings of every data query. `enable_materialized_cte`: a root page with
 * relationships is a `WITH page AS MATERIALIZED (…)` CTE (compile.ts).
 */
export const dataQuerySettings = {
  enable_materialized_cte: 1,
} as unknown as ClickHouseSettings;

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
}

export interface QueryEvent {
  kind: 'data' | 'nodeLookup' | 'snapshot' | 'watermarkPoll';
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
    watermarkPolls: 0,
  };

  /** Test hook: every query the API sends (after it is sent). */
  onQuery: ((event: QueryEvent) => void) | undefined;

  private readonly nodeIds = new Map<string, number>();

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

  constructor(readonly client: ClickHouseClient) {}

  /** Exactly one ClickHouse query: a fresh visibility snapshot of `nodeId`. */
  async readSnapshot(nodeId: number): Promise<VisibilitySnapshot> {
    this.stats.snapshots += 1;
    return readSnapshot(this.snapshotClient, nodeId);
  }

  /**
   * The snapshot of one request, pinning every node it names
   * (`SnapshotCache`): one cheap watermark query while the watermarks stand
   * still, else that plus one full snapshot read.
   */
  async pinnedSnapshot(nodeIds: readonly number[]): Promise<MultiNodeSnapshot> {
    this.stats.snapshots += 1;
    const { hits } = this.snapshotCache;
    const snapshot = await this.snapshotCache.read(nodeIds);
    if (this.snapshotCache.hits > hits) this.stats.snapshotCacheHits += 1;
    return snapshot;
  }

  async query<T>(label: string, sql: string, params: QueryParams) {
    this.stats.dataQueries += 1;
    const rows = await this.client.query<T>(sql, params, dataQuerySettings);
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

/**
 * The one snapshot of a request (or of one subscription re-run). The nodes
 * are fixed when the request starts (from the operation's root `node`
 * arguments); the snapshot is read once, on first use, pinning all of them
 * in one consistent read, and shared by every resolver: each per-node root
 * reads at its own node's `visible(n)`, and every root shares visible(0),
 * the tail, void and fence. Node-agnostic roots ride on the same snapshot,
 * so a multi-root operation reads one consistent state.
 */
export class PinnedSnapshot {
  private pending: Promise<MultiNodeSnapshot> | undefined;

  private readonly ids = new Map<string, number>();

  constructor(
    private readonly db: ApiDb,
    private readonly nodeNames: readonly string[],
    private readonly preset?: VisibilitySnapshot
  ) {}

  /** For a root that names `node` (undefined: a node-agnostic root). */
  async get(node?: string): Promise<VisibilitySnapshot> {
    if (node !== undefined && !this.nodeNames.includes(node)) {
      throw new GraphQLError(
        `Node ${node} is not a root node argument of this operation.`
      );
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
      node === undefined
        ? snapshot.nodeIds[0] ?? nodeAgnosticId
        : this.ids.get(node)!
    );
  }

  private async read() {
    const ids = await Promise.all(
      this.nodeNames.map(async (name) => {
        const id = await this.db.nodeId(name);
        this.ids.set(name, id);
        return id;
      })
    );
    return this.db.pinnedSnapshot(ids.length === 0 ? [nodeAgnosticId] : ids);
  }
}
