/* eslint-disable @typescript-eslint/naming-convention, functional/no-throw-statement, complexity, max-params */
// cspell:ignore clickhouse unhex
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
  blockPredicate,
  hexComparison,
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
  unspentOutputPredicate,
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
  const bounds = numericComparison(sql, 'internal_id', 'UInt64', {
    _gt: args.after_internal_id ?? undefined,
    _lt: args.before ?? undefined,
  });
  return pageOf(
    env,
    label,
    TransactionEntity,
    sql.view('transaction_at'),
    [
      `hash IN (${sql.accepted(ctx.node, candidates)})`,
      ...bounds,
      transactionPredicate(sql, args.where, ctx.node),
    ],
    order,
    { ...args, after: args.cursor },
    sels,
    maxLimits.transactions,
    /* the spike's cursor: the last internal_id (pass as `after`/`before`) */
    explicit === undefined ? (row) => String(row.internal_id ?? '') : undefined
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
    [`transaction_hash IN (${sql.accepted(ctx.node, candidates)})`, predicate],
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
) =>
  pageOf(
    env,
    label,
    OutputEntity,
    env.sql.view('output_at'),
    [unspentOutputPredicate(env.sql, args.where, env.ctx.node)],
    rootOrder(
      orderTerms(outputOrder, args.order_by),
      outputDefaultOrder,
      outputDefaultOrder
    ),
    args,
    sels,
    maxLimits.outputs
  );

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

/** Blocks the node accepts. */
const acceptedBlocks = (sql: Sql, node: number | undefined) =>
  `internal_id IN (SELECT block_internal_id FROM ${sql.nodeView(
    'node_block_at',
    node
  )})`;

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
    [acceptedBlocks(sql, ctx.node), blockPredicate(sql, args.where, ctx.node)],
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
