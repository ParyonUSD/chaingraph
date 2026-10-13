/* eslint-disable @typescript-eslint/naming-convention, functional/no-throw-statement, complexity, max-params */
// cspell:ignore clickhouse unhex unindexed
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
  where: readonly string[],
  order: readonly OrderTerm[],
  args: PageArgs,
  sels: readonly Sel[],
  max: number,
  cursorOf?: (row: Row, values: string[]) => string
): Promise<Page> => {
  checkLimit(args.limit, max);
  const offset = checkOffset(args.offset);
  const compiled = compileRoot(
    env.sql,
    env.ctx,
    {
      entity,
      limit: args.limit + 1,
      offset,
      order,
      source,
      where: [...where, ...keysetBound(env.sql, order, args.after)]
        .map((part) => `(${part})`)
        .join(' AND '),
    },
    sels
  );
  return runPage(env.db, label, env.sql, compiled, args.limit, cursorOf);
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
 * The root condition of a `transactions` page in internal-id order over a
 * materialized candidate set `cand` (transaction_hash,
 * transaction_internal_id), checking only the candidates the page can
 * contain (fix-pass-6.md §2). Exact, so the page and `has_more` are too:
 * - the first `window` distinct candidate ids in `dir` order (within the id
 *   bounds) bound a window: every candidate up to the last of them;
 * - the page of the window (the transaction rows, the node's acceptance and
 *   the rest of the filter, all restricted to the window's candidates): if it
 *   has `need` rows, or the window holds every candidate, it is the page,
 *   since no candidate before the window's end was skipped;
 * - else the whole candidate set as before (a constant condition: the branch
 *   not taken reads nothing).
 * The acceptance and filter reads cost one granule per candidate, so a page
 * of 10,000 among 200,000 candidates (mainnet L33) reads a twentieth.
 */
const candidateWindow = (
  sql: Sql,
  cand: string,
  checks: (among: string) => string[],
  idBounds: { _gt?: bigint; _lt?: bigint },
  dir: 'ASC' | 'DESC',
  need: number,
  window: number
) => {
  const k = sql.bind(window, 'UInt64');
  const needed = sql.bind(need, 'UInt64');
  const onIds = numericComparison(
    sql,
    'transaction_internal_id',
    'UInt64',
    idBounds
  );
  const ids = sql.scalar(
    '__cand_ids',
    `SELECT groupArray(transaction_internal_id) FROM (SELECT DISTINCT transaction_internal_id FROM ${cand}${
      onIds.length === 0 ? '' : ` WHERE ${onIds.join(' AND ')}`
    } ORDER BY transaction_internal_id ${dir} LIMIT ${k})`
  );
  const capped = `length(${ids}) = ${k}`;
  const inWindow = `SELECT transaction_hash FROM ${cand} WHERE NOT ${capped} OR transaction_internal_id ${
    dir === 'ASC' ? '<=' : '>='
  } ${ids}[-1]`;
  const page = sql.scalar(
    '__cand_page',
    `SELECT groupArray(hash) FROM (SELECT hash, internal_id FROM ${sql.view(
      'transaction_at'
    )} WHERE ${checks(inWindow)
      .map((part) => `(${part})`)
      .join(' AND ')} ORDER BY internal_id ${dir} LIMIT ${needed})`
  );
  const enough = `(NOT ${capped} OR length(${page}) >= ${needed})`;
  const rest = `SELECT transaction_hash FROM ${cand} WHERE NOT ${enough}`;
  return `(${enough} AND hash IN (SELECT arrayJoin(${page}))) OR (NOT ${enough} AND ${checks(
    rest
  )
    .map((part) => `(${part})`)
    .join(' AND ')})`;
};

export const runTransactions = async (
  env: RootEnv,
  label: string,
  args: TransactionsArgs,
  sels: readonly Sel[]
): Promise<Page> => {
  const { sql, ctx } = env;
  const explicit = orderTerms(transactionOrder, args.order_by);
  const order = rootOrder(
    explicit,
    [term('internal_id', args.order ?? 'ASC')],
    [term('internal_id', args.order ?? 'ASC')]
  );
  const candidates = transactionCandidates(sql, args.where, ctx.node);
  /*
   * Candidates (a positive, selective conjunct of the filter) are
   * materialized once: the transaction rows, the acceptance check and every
   * per-transaction semi-join of the filter read only them, by key.
   */
  const cand =
    candidates === undefined ? undefined : sql.cte('candidates', candidates);
  const range = {
    _gt: args.after_internal_id ?? undefined,
    _lt: args.before ?? undefined,
  };
  const bounds = numericComparison(sql, 'internal_id', 'UInt64', range);
  /* the transaction rows among `among`, the node's acceptance and the filter */
  const checks = (among: string) => [
    `hash IN (${among})`,
    ...(impliesAcceptance(args.where)
      ? []
      : [`hash IN (${sql.accepted(ctx.node, among)})`]),
    ...bounds,
    transactionPredicate(sql, args.where, ctx.node, among),
  ];
  const keyset =
    typeof args.cursor === 'string' && args.cursor !== '' ? args.cursor : '';
  if (
    cand !== undefined &&
    explicit === undefined &&
    (args.offset ?? 0) === 0 &&
    keyset === ''
  ) {
    return pageOf(
      env,
      label,
      TransactionEntity,
      sql.view('transaction_at'),
      [
        candidateWindow(
          sql,
          cand,
          checks,
          range,
          args.order ?? 'ASC',
          args.limit + 1,
          env.db.candidateWindowRows(args.limit + 1)
        ),
        ...bounds,
      ],
      order,
      { ...args, after: args.cursor },
      sels,
      maxLimits.transactions,
      (_row, values) => values[0]!
    );
  }
  const among =
    cand === undefined ? undefined : `SELECT transaction_hash FROM ${cand}`;
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
      transactionPredicate(sql, args.where, ctx.node, among),
    ],
    order,
    { ...args, after: args.cursor },
    sels,
    maxLimits.transactions,
    /* the spike's cursor: the last internal_id (pass as `after`/`before`) */
    explicit === undefined ? (_row, values) => values[0]! : undefined
  );
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
  const candidates =
    args.where !== undefined &&
    args.where !== null &&
    requiresOutpoint(args.where)
      ? `SELECT transaction_hash FROM ${spendRelationSql(
          sql,
          spendRowPredicate(sql, args.where, ctx.node)
        )}`
      : undefined;
  return pageOf(
    env,
    label,
    InputEntity,
    sql.view('input_at'),
    [
      `transaction_hash IN (${sql.accepted(ctx.node, candidates)})`,
      inputPredicate(sql, args.where, ctx.node),
    ],
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

/** The newest transaction internal_id visible in the request snapshot (node-agnostic). */
export const runTransactionWatermark = async (env: RootEnv, label: string) => {
  const [row] = await env.db.query<{ id: string | null }>(
    label,
    `SELECT if(count() = 0, NULL, toString(max(internal_id))) AS id FROM ${env.sql.view(
      'transaction_at'
    )}`,
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
