/* eslint-disable @typescript-eslint/naming-convention, functional/no-throw-statement, complexity, max-params, max-lines */
// cspell:ignore clickhouse unhex unindexed cand txid
/**
 * The v2 root fields: their row sources, acceptance rules, ordering,
 * pagination (keyset, offset) and page caps. Each root is ONE statement
 * (compile.ts). Shared by Query and Subscription (a live re-run calls the same
 * runner with the hub's snapshot).
 *
 * Per-node roots name their node (`node:`): transactions, outputs,
 * unspent_outputs, inputs, blocks, block_tip, mempool, node, watermark and
 * the aggregate/group roots. Node-agnostic roots (lookups by hash,
 * transaction_watermark, nodes) carry no acceptance: a per-node relationship
 * below them must name its node.
 */
import { GraphQLError } from 'graphql';

import type { Ctx, Entity, OrderTerm, Page, Row, Sel } from './compile.js';
import { afterCursor, compileRoot, runPage, runRows } from './compile.js';
import type { ApiDb } from './db.js';
import {
  BlockEntity,
  blockOrder,
  InputEntity,
  inputOrder,
  inputsByIdCovers,
  NodeEntity,
  NodeTransactionEntity,
  nodeTransactionOrder,
  orderTerms,
  OutputEntity,
  outputOrder,
  TransactionEntity,
  transactionOrder,
  withTiebreak,
} from './model.js';
import type {
  BlockFilter,
  InputFilter,
  NodeTransactionFilter,
  OutputFilter,
  Sql,
  TransactionFilter,
} from './sql.js';
import {
  blockCandidates,
  blockPredicate,
  candidatesImplied,
  hexComparison,
  impliesAcceptance,
  inputPredicate,
  invalid,
  maxListChunk,
  nodeTransactionPredicate,
  numericComparison,
  outputFilterIsSelective,
  outputPredicate,
  outputRowColumns,
  requiresOutpoint,
  spendRelationSql,
  spendRowPredicate,
  transactionCandidates,
  transactionConjuncts,
  transactionPredicate,
  unindexed,
  unspentOutputRows,
} from './sql.js';

/** Page-size caps per root (R33); `limit` above the cap is rejected (never silently cut). */
export const maxLimits = {
  blocks: 1_000,
  groups: 1_000,
  inputs: 1_000,
  lookups: 10_000,
  mempool: 5_000,
  outputs: 5_000,
  transactions: 10_000,
};

/** The largest `offset` accepted (offset pagination is kept but discouraged: R16). */
export const maxOffset = 100_000;

export const checkLimit = (limit: number, max: number) => {
  if (!Number.isInteger(limit) || limit < 0 || limit > max) {
    throw invalid(`limit must be between 0 and ${max}.`);
  }
};

const checkOffset = (offset: number | null | undefined) => {
  if (offset === undefined || offset === null) return undefined;
  if (!Number.isInteger(offset) || offset < 0 || offset > maxOffset) {
    throw invalid(`offset must be between 0 and ${maxOffset}.`);
  }
  return offset;
};

const term = (expr: string, dir: 'ASC' | 'DESC' = 'ASC'): OrderTerm => ({
  dir,
  expr,
  kind: 'uint',
});

export interface RootEnv {
  db: ApiDb;
  sql: Sql;
  ctx: Ctx;
}

export interface PageArgs {
  where?: unknown;
  order_by?: unknown;
  after?: unknown;
  offset?: number | null;
  limit: number;
}

/** Order of a root: explicit `order_by` + unique tie-breakers, else the default. */
const rootOrder = (
  explicit: OrderTerm[] | undefined,
  fallback: readonly OrderTerm[],
  tiebreak: readonly OrderTerm[]
) => withTiebreak(explicit ?? fallback, tiebreak);

const keysetBound = (
  sql: Sql,
  order: readonly OrderTerm[],
  after: unknown
): string[] =>
  typeof after === 'string' && after !== ''
    ? [afterCursor(sql, order, after)]
    : [];

const pageOf = async (
  env: RootEnv,
  label: string,
  entity: Entity,
  source: string,
  /** Conjuncts; a function gets the columns the page reads (`RootQuery.where`). */
  where:
    | readonly string[]
    | ((needs: ReadonlySet<string>) => readonly string[]),
  order: readonly OrderTerm[],
  args: PageArgs,
  sels: readonly Sel[],
  max: number,
  cursorOf?: (row: Row, values: string[]) => string,
  extra?: {
    values: { readonly [name: string]: string };
    onExtra: (extra: { [name: string]: unknown } | undefined) => void;
  }
): Promise<Page> => {
  checkLimit(args.limit, max);
  const offset = checkOffset(args.offset);
  const bound = keysetBound(env.sql, order, args.after);
  const compiled = compileRoot(
    env.sql,
    env.ctx,
    {
      entity,
      extra: extra?.values,
      limit: args.limit + 1,
      offset,
      order,
      source,
      where: (needs) =>
        [...(typeof where === 'function' ? where(needs) : where), ...bound]
          .map((part) => `(${part})`)
          .join(' AND '),
    },
    sels
  );
  return runPage(
    env.db,
    label,
    env.sql,
    compiled,
    args.limit,
    cursorOf,
    extra?.onExtra
  );
};

/* ------------------------------------------------------------------ */
/* transactions                                                         */
/* ------------------------------------------------------------------ */

export interface TransactionsArgs extends PageArgs {
  where?: TransactionFilter | null;
  /** The spike's order on internal_id (when `order_by` is not given). */
  order?: 'ASC' | 'DESC' | null;
  /** internal_id bounds: after < internal_id < before. */
  after_internal_id?: bigint | null;
  before?: bigint | null;
  /** Opaque keyset cursor for an `order_by` page. */
  cursor?: string | null;
}

/** A filter with no condition at all (an unselective root). */
const unselective = (where: TransactionFilter | null | undefined) =>
  where === undefined ||
  where === null ||
  Object.values(where).every((value) => value === undefined || value === null);

/**
 * The acceptance condition of an unselective `transactions(node:)` page in
 * internal-id order (owner question 1, decided 2026-10-11): with the
 * optional projection p_node_tx, a window of the node's acceptance rows read
 * in id order (`Sql.acceptedInIdOrder`: exact page and `has_more`); without
 * it, the id range applied to the base acceptance rows below the collapse
 * (no store change; an id window then reads the parts holding those ids).
 */
const acceptedInIdOrder = async (
  env: RootEnv,
  range: { _gt?: bigint; _lt?: bigint },
  args: TransactionsArgs
) => {
  const { sql, ctx } = env;
  /* qualified: the collapsed view's select list aliases any(transaction_internal_id) */
  const onIds = numericComparison(
    sql,
    'tx_acceptance.transaction_internal_id',
    'UInt64',
    range
  );
  const { acceptanceByIdProjection } = await env.db.storeFeatures();
  if (acceptanceByIdProjection) {
    return `hash IN (${sql.acceptedInIdOrder(
      ctx.node,
      onIds,
      args.order ?? 'ASC',
      args.limit + 1 + (args.offset ?? 0),
      env.db.acceptanceWindowRows(args.limit + 1 + (args.offset ?? 0))
    )})`;
  }
  return `hash IN (SELECT transaction_hash FROM ${sql.nodeView(
    'tx_acceptance_at',
    ctx.node,
    onIds.length === 0 ? undefined : onIds.join(' AND ')
  )})`;
};

/**
 * The root condition of a `transactions` page in internal-id order that
 * checks only the candidates the page can contain (fix-pass-6.md §2), over
 * the candidate set `candidates` (a subquery of transaction_hash,
 * transaction_internal_id): the first `window` distinct candidates in `dir`
 * id order (within the id bounds), kept as a statement-level scalar array,
 * i.e. every candidate up to the last of them. The page over them (the
 * transaction rows, the node's acceptance and the rest of the filter, all
 * restricted to the window) is the page if it has `need` rows or the window
 * holds every candidate (`capped` false): no candidate before the window's
 * end was skipped. Otherwise the caller runs the full statement
 * (`runTransactions`). Returns the condition and the `capped` expression.
 *
 * The acceptance and filter reads cost one granule per candidate (per part,
 * by hash), so a page of 10,000 among some 60,000 candidates (mainnet L33)
 * reads a sixth. `checks(among, amongIds)` gets the window's hashes and their
 * internal ids, so each read can take the id path (fix-pass-6.md §5). The
 * candidates are evaluated once (into the array), not as a MATERIALIZED CTE:
 * ClickHouse 26.8 evaluates such a CTE once per subquery scope that reads it
 * (chipnet L33: 18 reads of output / spend). The fallback is a separate
 * statement: spelt out in this one (the candidates again under a constant
 * condition) it multiplied the scalar-cache lookups of the arrays (chipnet
 * L33: 937, ~0.7 s of analysis).
 */
const candidateWindow = (
  sql: Sql,
  candidates: string,
  checks: (among: string, amongIds: string) => string[],
  idBounds: { _gt?: bigint; _lt?: bigint },
  dir: 'ASC' | 'DESC',
  window: number
) => {
  const k = sql.bind(window, 'UInt64');
  const onIds = numericComparison(
    sql,
    'transaction_internal_id',
    'UInt64',
    idBounds
  );
  /*
   * One array of (hash, id) tuples, read by arrayJoin: `tupleElement` of a
   * scalar is folded into a constant holding the whole column, and the
   * analyzer hashes every constant of a subquery's tree each time it
   * resolves it (a reference re-resolves the referenced level and its
   * ancestors): a window of 60 k hashes was ~2 MB hashed per reference of
   * the page (fix-pass-11.md §1).
   */
  const first = sql.scalar(
    '__cand_window',
    `SELECT groupArray(tuple(transaction_hash, transaction_internal_id)) FROM (SELECT transaction_hash, transaction_internal_id FROM (${candidates})${
      onIds.length === 0 ? '' : ` WHERE ${onIds.join(' AND ')}`
    } ORDER BY transaction_internal_id ${dir} LIMIT 1 BY transaction_internal_id LIMIT ${k})`
  );
  return {
    capped: `toUInt8(length(${first}) = ${k})`,
    where: checks(
      `SELECT tupleElement(__w, 1) FROM (SELECT arrayJoin(${first}) AS __w)`,
      `SELECT tupleElement(__w, 2) FROM (SELECT arrayJoin(${first}) AS __w)`
    ),
  };
};

/** A `transactions` page checking every candidate (or the whole acceptance set). */
const fullTransactionsPage = async (
  env: RootEnv,
  label: string,
  args: TransactionsArgs,
  sels: readonly Sel[],
  explicit: OrderTerm[] | undefined,
  order: readonly OrderTerm[],
  range: { _gt?: bigint; _lt?: bigint }
): Promise<Page> => {
  const { sql, ctx } = env;
  const candidates = transactionCandidates(sql, args.where, ctx.node);
  const bounds = numericComparison(sql, 'internal_id', 'UInt64', range);
  const among =
    candidates === undefined
      ? undefined
      : `SELECT transaction_hash FROM ${sql.cte('candidates', candidates)}`;
  return pageOf(
    env,
    label,
    TransactionEntity,
    sql.view('transaction_at'),
    [
      ...(among === undefined ? [] : [`hash IN (${among})`]),
      ...(impliesAcceptance(args.where)
        ? []
        : [
            unselective(args.where) && explicit === undefined
              ? await acceptedInIdOrder(env, range, args)
              : `hash IN (${sql.accepted(ctx.node, among)})`,
          ]),
      ...bounds,
      transactionPredicate(
        sql,
        args.where,
        ctx.node,
        among,
        among === undefined
          ? undefined
          : candidatesImplied(sql.fresh(), args.where, ctx.node)
      ),
    ],
    order,
    { ...args, after: args.cursor },
    sels,
    maxLimits.transactions,
    /* the spike's cursor: the last internal_id (pass as `after`/`before`) */
    explicit === undefined ? (_row, values) => values[0]! : undefined
  );
};

export const runTransactions = async (
  env: RootEnv,
  label: string,
  args: TransactionsArgs,
  sels: readonly Sel[]
): Promise<Page> => {
  const explicit = orderTerms(transactionOrder, args.order_by);
  const order = rootOrder(
    explicit,
    [term('internal_id', args.order ?? 'ASC')],
    [term('internal_id', args.order ?? 'ASC')]
  );
  const range = {
    _gt: args.after_internal_id ?? undefined,
    _lt: args.before ?? undefined,
  };
  const keyset =
    typeof args.cursor === 'string' && args.cursor !== '' ? args.cursor : '';
  /*
   * Candidates (a positive, selective conjunct of the filter): the
   * transaction rows, the acceptance check and every per-transaction
   * semi-join of the filter read only them, by key. A page in id order first
   * checks a window of them (`candidateWindow`); otherwise, or when the
   * window cannot decide, they are materialized once (a statement-level CTE)
   * and all checked.
   */
  if (
    explicit === undefined &&
    (args.offset ?? 0) === 0 &&
    keyset === '' &&
    transactionCandidates(env.sql.fresh(), args.where, env.ctx.node) !==
      undefined
  ) {
    const { sql, ctx } = env;
    const candidates = transactionCandidates(sql, args.where, ctx.node)!;
    const bounds = numericComparison(sql, 'internal_id', 'UInt64', range);
    /*
     * The transaction rows among the window's candidates (hashes and their
     * internal ids: both bound the read, ClickHouse takes the key or p_id,
     * whichever selects fewer granules), the node's acceptance and the filter.
     */
    /* with p_node_tx the window's acceptance is read by id alone (fix-pass-7.md §2) */
    const { acceptanceByIdProjection } = await env.db.storeFeatures();
    const checks = (among: string, amongIds: string) => [
      `hash IN (${among})`,
      `internal_id IN (${amongIds})`,
      ...(impliesAcceptance(args.where)
        ? []
        : [
            `hash IN (${sql.accepted(
              ctx.node,
              among,
              amongIds,
              acceptanceByIdProjection
            )})`,
          ]),
      ...bounds,
      transactionPredicate(
        sql,
        args.where,
        ctx.node,
        among,
        candidatesImplied(sql.fresh(), args.where, ctx.node)
      ),
    ];
    const window = candidateWindow(
      sql,
      candidates,
      checks,
      range,
      args.order ?? 'ASC',
      env.db.candidateWindowRows(
        args.limit + 1,
        transactionConjuncts(args.where) > 1
      )
    );
    const seen: { capped?: unknown } = {};
    const page = await pageOf(
      env,
      label,
      TransactionEntity,
      sql.view('transaction_at'),
      window.where,
      order,
      { ...args, after: args.cursor },
      sels,
      maxLimits.transactions,
      (_row, values) => values[0]!,
      {
        onExtra: (extra) => {
          seen.capped = extra?.cappedWindow;
        },
        values: { cappedWindow: window.capped },
      }
    );
    /*
     * Decided: a full page (limit + 1 rows: has_more), or a window holding
     * every candidate. Not decided: a capped window short of the page, or an
     * empty page without levels (no row carries `cappedWindow`).
     */
    if (
      page.has_more ||
      (seen.capped !== undefined && Number(seen.capped) === 0)
    ) {
      return page;
    }
    /* the window could not decide: every candidate */
    const sqlAll = sql.fresh();
    return fullTransactionsPage(
      { ...env, ctx: { ...ctx, sql: sqlAll }, sql: sqlAll },
      label,
      args,
      sels,
      explicit,
      order,
      range
    );
  }
  return fullTransactionsPage(env, label, args, sels, explicit, order, range);
};

/* ------------------------------------------------------------------ */
/* outputs                                                              */
/* ------------------------------------------------------------------ */

const outputDefaultOrder = [
  term('transaction_internal_id'),
  term('output_index'),
];

export interface OutputsArgs extends PageArgs {
  where?: OutputFilter | null;
}

/** Outputs of transactions the node accepts (search by script, prefix, category …). */
export const runOutputs = async (
  env: RootEnv,
  label: string,
  args: OutputsArgs,
  sels: readonly Sel[]
) => {
  const { sql, ctx } = env;
  const predicate = outputPredicate(
    sql,
    args.where,
    outputRowColumns,
    ctx.node
  );
  const candidates = outputFilterIsSelective(args.where)
    ? `SELECT transaction_hash FROM ${sql.view(
        'output_at'
      )} WHERE ${outputPredicate(sql, args.where, outputRowColumns, ctx.node)}`
    : undefined;
  return pageOf(
    env,
    label,
    OutputEntity,
    sql.view('output_at'),
    [
      `${
        candidates === undefined
          ? 'transaction_hash'
          : unindexed('transaction_hash')
      } IN (${sql.accepted(ctx.node, candidates)})`,
      predicate,
    ],
    rootOrder(
      orderTerms(outputOrder, args.order_by),
      outputDefaultOrder,
      outputDefaultOrder
    ),
    args,
    sels,
    maxLimits.outputs
  );
};

/** The node's unspent outputs (query time, UTXO tables off). */
export const runUnspentOutputs = async (
  env: RootEnv,
  label: string,
  args: OutputsArgs,
  sels: readonly Sel[]
) => {
  const rows = unspentOutputRows(env.sql, args.where, env.ctx.node);
  return pageOf(
    env,
    label,
    OutputEntity,
    rows.source,
    [rows.where],
    rootOrder(
      orderTerms(outputOrder, args.order_by),
      outputDefaultOrder,
      outputDefaultOrder
    ),
    args,
    sels,
    maxLimits.outputs
  );
};

/* ------------------------------------------------------------------ */
/* inputs                                                               */
/* ------------------------------------------------------------------ */

const inputDefaultOrder = [
  term('transaction_internal_id'),
  term('input_index'),
];

export interface InputsArgs extends PageArgs {
  where?: InputFilter | null;
}

/** Inputs of transactions the node accepts, keyset on (transaction_internal_id, input_index). */
export const runInputs = async (
  env: RootEnv,
  label: string,
  args: InputsArgs,
  sels: readonly Sel[]
) => {
  const { sql, ctx } = env;
  const { where } = args;
  const spends =
    where !== undefined && where !== null && requiresOutpoint(where);
  /* the spend relation's rows matching `where` (`columns`), as a subquery */
  const spent = (columns: string) =>
    `SELECT ${columns} FROM ${spendRelationSql(
      sql,
      spendRowPredicate(sql, where, ctx.node)
    )}`;
  const byHash = () => [
    `transaction_hash IN (${sql.accepted(
      ctx.node,
      spends ? spent('transaction_hash') : undefined
    )})`,
    inputPredicate(sql, where, ctx.node),
  ];
  const { acceptanceByIdProjection } =
    spends && sql.readsById.inputs
      ? await env.db.storeFeatures()
      : { acceptanceByIdProjection: false };
  /*
   * Inputs matched through the spend relation (`outpoint` filters) are read
   * by the spends' internal ids when every part has input.p_txid and the
   * page reads no column outside it (fix-pass-13.md §2): the hash sets stay
   * out of key analysis (`identity`), so ClickHouse does not search input's
   * hash key with a set of every matching spend (mainnet L17: ~1,500 hashes,
   * every part spanning every hash, 0.35-0.7 s of key CPU) only to read
   * p_txid's few granules anyway; with p_node_tx the acceptance check reads
   * the ids alone. Exact: one id per hash, and the hash conditions stay.
   */
  const byId = () => {
    const ids = spent('transaction_internal_id');
    return [
      `transaction_internal_id IN (${ids})`,
      `identity(transaction_hash) IN (${sql.accepted(
        ctx.node,
        /* `among` is not read with idsOnly (no second spend read) */
        acceptanceByIdProjection ? ids : spent('transaction_hash'),
        ids,
        acceptanceByIdProjection
      )})`,
      `identity((transaction_hash, input_index)) IN (${spent(
        'transaction_hash, input_index'
      )})`,
    ];
  };
  return pageOf(
    env,
    label,
    InputEntity,
    sql.view('input_at'),
    spends && sql.readsById.inputs
      ? (needs) => (inputsByIdCovers(needs) ? byId() : byHash())
      : byHash(),
    rootOrder(
      orderTerms(inputOrder, args.order_by),
      inputDefaultOrder,
      inputDefaultOrder
    ),
    args,
    sels,
    maxLimits.inputs
  );
};

/* ------------------------------------------------------------------ */
/* blocks                                                               */
/* ------------------------------------------------------------------ */

export interface BlocksArgs extends PageArgs {
  where?: BlockFilter | null;
}

/** Blocks the node accepts (among `candidates`, a subquery of block internal ids, when given). */
const acceptedBlocks = (
  sql: Sql,
  node: number | undefined,
  candidates?: string
) =>
  `internal_id IN (SELECT block_internal_id FROM ${sql.nodeView(
    'node_block_at',
    node
  )}${
    candidates === undefined
      ? ''
      : ` WHERE block_internal_id IN (${candidates})`
  })`;

export const runBlocks = async (
  env: RootEnv,
  label: string,
  args: BlocksArgs,
  sels: readonly Sel[]
) => {
  const { sql, ctx } = env;
  return pageOf(
    env,
    label,
    BlockEntity,
    sql.view('block_at'),
    [
      acceptedBlocks(sql, ctx.node, blockCandidates(sql, args.where)),
      blockPredicate(sql, args.where, ctx.node),
    ],
    rootOrder(
      orderTerms(blockOrder, args.order_by),
      [term('height')],
      [term('internal_id')]
    ),
    args,
    sels,
    maxLimits.blocks
  );
};

/**
 * The node's tip: its accepted block with the greatest height (lowest
 * internal id among equal heights), with `previous_block_hash` for re-org
 * detection.
 */
export const runBlockTip = async (
  env: RootEnv,
  label: string,
  sels: readonly Sel[]
) => {
  const { sql, ctx } = env;
  const compiled = compileRoot(
    sql,
    ctx,
    {
      entity: BlockEntity,
      limit: 1,
      order: [term('height', 'DESC'), term('internal_id')],
      source: sql.view('block_at'),
      where: `internal_id IN (SELECT block_internal_id FROM ${sql.nodeView(
        'node_block_at',
        ctx.node
      )} ORDER BY height DESC LIMIT 1 WITH TIES)`,
    },
    sels
  );
  return (await runRows(env.db, label, sql, compiled))[0] ?? null;
};

/* ------------------------------------------------------------------ */
/* mempool                                                              */
/* ------------------------------------------------------------------ */

export interface MempoolArgs extends PageArgs {
  where?: NodeTransactionFilter | null;
}

/** The node's mempool (node_transaction), e.g. ordered by validated_at. */
export const runMempool = async (
  env: RootEnv,
  label: string,
  args: MempoolArgs,
  sels: readonly Sel[]
) => {
  const { sql, ctx } = env;
  return pageOf(
    env,
    label,
    NodeTransactionEntity,
    sql.nodeView('node_transaction_at', ctx.node),
    [nodeTransactionPredicate(sql, args.where, ctx.node)],
    rootOrder(
      orderTerms(nodeTransactionOrder, args.order_by),
      [term('transaction_internal_id')],
      [term('transaction_internal_id')]
    ),
    args,
    sels,
    maxLimits.mempool
  );
};

/* ------------------------------------------------------------------ */
/* nodes                                                                */
/* ------------------------------------------------------------------ */

const nodeSource =
  'SELECT internal_id, name, protocol_version, user_agent, first_connected_at, latest_connection_began_at FROM node_v';

/** One node (metadata, plus its mempool and accepted blocks). */
export const runNode = async (
  env: RootEnv,
  label: string,
  sels: readonly Sel[]
) => {
  const { sql, ctx } = env;
  const compiled = compileRoot(
    sql,
    ctx,
    {
      entity: NodeEntity,
      order: [],
      source: nodeSource,
      where: `internal_id = ${sql.bind(ctx.node, 'UInt32')}`,
    },
    sels
  );
  return (await runRows(env.db, label, sql, compiled))[0] ?? null;
};

/** Every node's metadata (node-agnostic; per-node relationships need `node(node:)`). */
export const runNodes = async (
  env: RootEnv,
  label: string,
  sels: readonly Sel[]
) => {
  const compiled = compileRoot(
    env.sql,
    env.ctx,
    {
      entity: NodeEntity,
      order: [term('internal_id')],
      source: nodeSource,
      where: '1',
    },
    sels
  );
  return runRows(env.db, label, env.sql, compiled);
};

/* ------------------------------------------------------------------ */
/* node-agnostic lookups                                                */
/* ------------------------------------------------------------------ */

const checkHashes = (hashes: readonly string[]) => {
  if (hashes.length > maxLimits.lookups) {
    throw invalid(
      `At most ${maxLimits.lookups} hashes per lookup (bound in chunks of ${maxListChunk}).`
    );
  }
};

/** Transactions by hash (node-agnostic: no acceptance). */
export const runTransactionsByHash = async (
  env: RootEnv,
  label: string,
  hashes: readonly string[],
  sels: readonly Sel[]
) => {
  checkHashes(hashes);
  const { sql, ctx } = env;
  const compiled = compileRoot(
    sql,
    ctx,
    {
      entity: TransactionEntity,
      order: [term('internal_id')],
      source: sql.view('transaction_at'),
      where: hexComparison(sql, 'hash', 'hash', { _in: hashes }, 'hashes')[0]!,
    },
    sels
  );
  return runRows(env.db, label, sql, compiled);
};

/** Blocks by hash (node-agnostic). */
export const runBlocksByHash = async (
  env: RootEnv,
  label: string,
  hashes: readonly string[],
  sels: readonly Sel[]
) => {
  checkHashes(hashes);
  const { sql, ctx } = env;
  const compiled = compileRoot(
    sql,
    ctx,
    {
      entity: BlockEntity,
      order: [term('height'), term('internal_id')],
      source: sql.view('block_at'),
      where: hexComparison(sql, 'hash', 'hash', { _in: hashes }, 'hashes')[0]!,
    },
    sels
  );
  return runRows(env.db, label, sql, compiled);
};

/** Outputs of transactions by hash (node-agnostic), optionally filtered. */
export const runOutputsByTransactionHash = async (
  env: RootEnv,
  label: string,
  hashes: readonly string[],
  where: OutputFilter | null | undefined,
  sels: readonly Sel[]
) => {
  checkHashes(hashes);
  const { sql, ctx } = env;
  const compiled = compileRoot(
    sql,
    ctx,
    {
      entity: OutputEntity,
      order: [term('transaction_internal_id'), term('output_index')],
      source: sql.view('output_at'),
      where: [
        hexComparison(
          sql,
          'transaction_hash',
          'hash',
          { _in: hashes },
          'transaction_hashes'
        )[0]!,
        outputPredicate(sql, where, outputRowColumns, ctx.node),
      ]
        .map((part) => `(${part})`)
        .join(' AND '),
    },
    sels
  );
  return runRows(env.db, label, sql, compiled);
};

/**
 * The newest transaction internal_id visible in the request snapshot
 * (node-agnostic): read in descending id order (transaction.p_id) until the
 * first visible row, instead of max() over every row (mainnet: 420 M rows,
 * seconds of CPU per L33 request; fix-pass-6.md §7).
 */
export const runTransactionWatermark = async (env: RootEnv, label: string) => {
  const [row] = await env.db.query<{ id: string }>(
    label,
    `SELECT toString(internal_id) AS id FROM ${env.sql.view(
      'transaction_at'
    )} ORDER BY internal_id DESC LIMIT 1`,
    env.sql.params
  );
  return row?.id ?? null;
};

/** The node's visibility watermark (the snapshot the request reads) and newest ids. */
export const runWatermark = (env: RootEnv, nodeName: string) => {
  if (env.ctx.node === undefined) {
    throw new GraphQLError('watermark needs a node.');
  }
  return {
    node: nodeName,
    visible: String(env.sql.snapshot.visible),
    visible0: String(env.sql.snapshot.visible0),
  };
};

/* ------------------------------------------------------------------ */
/* row sets (aggregates and groups)                                     */
/* ------------------------------------------------------------------ */

/** The rows a per-node root reads (before ordering and paging): a source and its conditions. */
export interface RowSet {
  source: string;
  where: string[];
}

const viewSource = (view: string) => `SELECT * FROM ${view}`;

export const rowSets = {
  blocks: (env: RootEnv, where: BlockFilter | null | undefined): RowSet => ({
    source: viewSource(env.sql.view('block_at')),
    where: [
      acceptedBlocks(env.sql, env.ctx.node, blockCandidates(env.sql, where)),
      blockPredicate(env.sql, where, env.ctx.node),
    ],
  }),
  inputs: (env: RootEnv, where: InputFilter | null | undefined): RowSet => {
    const { sql, ctx } = env;
    const candidates =
      where !== undefined && where !== null && requiresOutpoint(where)
        ? `SELECT transaction_hash FROM ${spendRelationSql(
            sql,
            spendRowPredicate(sql, where, ctx.node)
          )}`
        : undefined;
    return {
      source: viewSource(sql.view('input_at')),
      where: [
        `transaction_hash IN (${sql.accepted(ctx.node, candidates)})`,
        inputPredicate(sql, where, ctx.node),
      ],
    };
  },
  mempool: (
    env: RootEnv,
    where: NodeTransactionFilter | null | undefined
  ): RowSet => ({
    source: viewSource(env.sql.nodeView('node_transaction_at', env.ctx.node)),
    where: [nodeTransactionPredicate(env.sql, where, env.ctx.node)],
  }),
  outputs: (env: RootEnv, where: OutputFilter | null | undefined): RowSet => {
    const { sql, ctx } = env;
    const candidates = outputFilterIsSelective(where)
      ? `SELECT transaction_hash FROM ${sql.view(
          'output_at'
        )} WHERE ${outputPredicate(sql, where, outputRowColumns, ctx.node)}`
      : undefined;
    return {
      source: viewSource(sql.view('output_at')),
      where: [
        `${
          candidates === undefined
            ? 'transaction_hash'
            : unindexed('transaction_hash')
        } IN (${sql.accepted(ctx.node, candidates)})`,
        outputPredicate(sql, where, outputRowColumns, ctx.node),
      ],
    };
  },
  /**
   * Spends: inputs of transactions the node accepts with the attributes of
   * the output each spends (the spend relation: index + tail). Inputs whose
   * spent output is not stored (coinbase inputs) are not spends.
   */
  spends: (env: RootEnv, where: InputFilter | null | undefined): RowSet => {
    const { sql, ctx } = env;
    const evaluable =
      where === undefined || where === null || requiresOutpoint(where);
    const relation = spendRelationSql(
      sql,
      evaluable ? spendRowPredicate(sql, where, ctx.node) : '1'
    );
    return {
      source: `SELECT * FROM ${relation}`,
      where: [
        `transaction_hash IN (${sql.accepted(
          ctx.node,
          `SELECT transaction_hash FROM ${relation}`
        )})`,
        ...(evaluable
          ? []
          : [
              `(transaction_hash, input_index) IN (SELECT transaction_hash, input_index FROM ${sql.view(
                'input_at'
              )} WHERE ${inputPredicate(sql, where, ctx.node)})`,
            ]),
      ],
    };
  },
  transactions: (
    env: RootEnv,
    where: TransactionFilter | null | undefined
  ): RowSet => {
    const { sql, ctx } = env;
    const candidates = transactionCandidates(sql, where, ctx.node);
    const among =
      candidates === undefined
        ? undefined
        : `SELECT transaction_hash FROM (${candidates})`;
    return {
      source: viewSource(sql.view('transaction_at')),
      where: [
        ...(among === undefined ? [] : [`hash IN (${among})`]),
        ...(impliesAcceptance(where)
          ? []
          : [`hash IN (${sql.accepted(ctx.node, among)})`]),
        transactionPredicate(sql, where, ctx.node, among),
      ],
    };
  },
  unspentOutputs: (
    env: RootEnv,
    where: OutputFilter | null | undefined
  ): RowSet => {
    const rows = unspentOutputRows(env.sql, where, env.ctx.node);
    return { source: rows.source, where: [rows.where] };
  },
};
