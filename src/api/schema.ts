/* eslint-disable camelcase, @typescript-eslint/naming-convention, @typescript-eslint/no-magic-numbers, functional/no-throw-statement, complexity, max-params */
// cspell:ignore clickhouse pothos unhex
/**
 * The v2 GraphQL schema of the Phase 2 spike (Pothos). Only what the spike
 * cases S1–S6 need; see docs/clickhouse-port/phase2-spike.md.
 *
 * - Per-node roots take `node: String!` (R1). Nothing in the schema accepts
 *   a snapshot/watermark parameter (R2): the snapshot comes from the
 *   request context (`PinnedSnapshot`), read once per request.
 * - Each root field is one ClickHouse statement (compile.ts); nested fields
 *   only read prefetched data.
 */
import SchemaBuilder from '@pothos/core';
import type { GraphQLResolveInfo } from 'graphql';
import { GraphQLError, Kind, print } from 'graphql';

import type { VisibilitySnapshot } from '../store/clickhouse/visibility.js';

import type { Order, Page } from './compile.js';
import {
  prefetched,
  runInputs,
  runTransactions,
  runUnspentOutputs,
} from './compile.js';
import type { ApiDb, PinnedSnapshot } from './db.js';
import type { LiveHub } from './live.js';
import { resultKey } from './live.js';
import type {
  BigIntComparison,
  BlockWindow,
  Capability,
  CapabilityComparison,
  HexComparison,
  InputFilter,
  IntComparison,
  OutputFilter,
  TransactionFilter,
} from './sql.js';
import { Sql, transactionPredicate, unspentOutputPredicate } from './sql.js';

export interface ApiContext {
  db: ApiDb;
  hub: LiveHub;
  /** Set per execution by the snapshot plugin (server.ts). */
  snapshot?: PinnedSnapshot;
}

interface Row {
  [key: string]: unknown;
}

/** Page-size caps per root (R33). */
export const maxLimits = {
  groups: 1_000,
  inputs: 1_000,
  outputs: 5_000,
  transactions: 10_000,
};

const hexPattern = /^(?:[0-9a-fA-F]{2})*$/u;
const bigIntPattern = /^-?\d{1,20}$/u;

const parseHex = (value: unknown) => {
  if (typeof value !== 'string' || !hexPattern.test(value)) {
    throw new GraphQLError('Hex values are even-length hexadecimal strings.');
  }
  return value.toLowerCase();
};

const parseBigInt = (value: unknown) => {
  if (typeof value === 'bigint') return value;
  if (typeof value === 'number' && Number.isSafeInteger(value)) {
    return BigInt(value);
  }
  if (typeof value === 'string' && bigIntPattern.test(value)) {
    return BigInt(value);
  }
  throw new GraphQLError('BigInt values are integers (as strings).');
};

interface Types {
  Context: ApiContext;
  DefaultFieldNullability: false;
  Scalars: {
    Hex: { Input: string; Output: string };
    BigInt: { Input: bigint; Output: bigint | number | string };
  };
}

const builder = new SchemaBuilder<Types>({ defaultFieldNullability: false });

builder.scalarType('Hex', {
  description: 'Bytes as lowercase hexadecimal (no prefix).',
  parseLiteral: (node) => {
    if (node.kind !== Kind.STRING) throw new GraphQLError('Hex is a string.');
    return parseHex(node.value);
  },
  parseValue: parseHex,
  serialize: (value) => String(value),
});

builder.scalarType('BigInt', {
  description: '64-bit integer, serialized as a decimal string.',
  parseLiteral: (node) => {
    if (node.kind !== Kind.STRING && node.kind !== Kind.INT) {
      throw new GraphQLError('BigInt is a string or an integer.');
    }
    return parseBigInt(node.value);
  },
  parseValue: parseBigInt,
  serialize: (value) => String(value),
});

const OrderEnum = builder.enumType('Order', {
  values: { ASC: { value: 'ASC' }, DESC: { value: 'DESC' } } as const,
});

const CapabilityEnum = builder.enumType('TokenCapability', {
  values: {
    MINTING: { value: 'minting' },
    MUTABLE: { value: 'mutable' },
    NONE: { value: 'none' },
  } as const,
});

/* ------------------------------------------------------------------ */
/* filter inputs                                                        */
/* ------------------------------------------------------------------ */

const IntComparisonInput = builder
  .inputRef<IntComparison>('IntComparison')
  .implement({
    fields: (t) => ({
      _eq: t.int(),
      _gt: t.int(),
      _gte: t.int(),
      _in: t.intList(),
      _lt: t.int(),
      _lte: t.int(),
      _neq: t.int(),
    }),
  });

const BigIntComparisonInput = builder
  .inputRef<BigIntComparison>('BigIntComparison')
  .implement({
    fields: (t) => ({
      _eq: t.field({ type: 'BigInt' }),
      _gt: t.field({ type: 'BigInt' }),
      _gte: t.field({ type: 'BigInt' }),
      _in: t.field({ type: ['BigInt'] }),
      _lt: t.field({ type: 'BigInt' }),
      _lte: t.field({ type: 'BigInt' }),
      _neq: t.field({ type: 'BigInt' }),
    }),
  });

const HexComparisonInput = builder
  .inputRef<HexComparison>('HexComparison')
  .implement({
    fields: (t) => ({
      _eq: t.field({ type: 'Hex' }),
      _in: t.field({ type: ['Hex'] }),
      _is_null: t.boolean(),
      _neq: t.field({ type: 'Hex' }),
      _prefix: t.field({ type: 'Hex' }),
    }),
  });

const CapabilityComparisonInput = builder
  .inputRef<CapabilityComparison>('TokenCapabilityComparison')
  .implement({
    fields: (t) => ({
      _eq: t.field({ type: CapabilityEnum }),
      _in: t.field({ type: [CapabilityEnum] }),
      _is_null: t.boolean(),
      _neq: t.field({ type: CapabilityEnum }),
    }),
  });

const OutputFilterInput = builder.inputRef<OutputFilter>('OutputFilter');
OutputFilterInput.implement({
  description:
    'Predicates on one output row (or, as `outpoint`, on the spent output of one input row).',
  fields: (t) => ({
    _and: t.field({ type: [OutputFilterInput] }),
    _not: t.field({ type: OutputFilterInput }),
    _or: t.field({ type: [OutputFilterInput] }),
    fungible_token_amount: t.field({ type: BigIntComparisonInput }),
    locking_bytecode: t.field({ type: HexComparisonInput }),
    nonfungible_token_capability: t.field({ type: CapabilityComparisonInput }),
    nonfungible_token_commitment: t.field({ type: HexComparisonInput }),
    output_index: t.field({ type: IntComparisonInput }),
    token_category: t.field({ type: HexComparisonInput }),
    value_satoshis: t.field({ type: BigIntComparisonInput }),
  }),
});

const InputFilterInput = builder.inputRef<InputFilter>('InputFilter');
InputFilterInput.implement({
  description:
    'Predicates on one input row; `outpoint` fields bind to the same row (same-row conjunction).',
  fields: (t) => ({
    _and: t.field({ type: [InputFilterInput] }),
    _not: t.field({ type: InputFilterInput }),
    _or: t.field({ type: [InputFilterInput] }),
    input_index: t.field({ type: IntComparisonInput }),
    outpoint: t.field({ type: OutputFilterInput }),
    outpoint_transaction_hash: t.field({ type: HexComparisonInput }),
  }),
});

const BlockWindowInput = builder
  .inputRef<BlockWindow>('BlockWindow')
  .implement({
    description:
      "Per node: in a block the root's node accepts with from_height <= height < to_height, or (include_mempool, default true) in its mempool.",
    fields: (t) => ({
      from_height: t.int(),
      include_mempool: t.boolean(),
      to_height: t.int(),
    }),
  });

const TransactionFilterInput =
  builder.inputRef<TransactionFilter>('TransactionFilter');
TransactionFilterInput.implement({
  fields: (t) => ({
    _and: t.field({ type: [TransactionFilterInput] }),
    _not: t.field({ type: TransactionFilterInput }),
    _or: t.field({ type: [TransactionFilterInput] }),
    block_window: t.field({ type: BlockWindowInput }),
    has_input: t.field({
      description: 'EXISTS an input of the transaction matching the filter.',
      type: InputFilterInput,
    }),
    has_output: t.field({
      description: 'EXISTS an output of the transaction matching the filter.',
      type: OutputFilterInput,
    }),
    hash: t.field({ type: HexComparisonInput }),
    internal_id: t.field({ type: BigIntComparisonInput }),
  }),
});

/* ------------------------------------------------------------------ */
/* object types                                                         */
/* ------------------------------------------------------------------ */

const TransactionRef = builder.objectRef<Row>('Transaction');
const OutputRef = builder.objectRef<Row>('Output');
const InputRef = builder.objectRef<Row>('Input');
const SpentOutputRef = builder.objectRef<Row>('SpentOutput');
const BlockInclusionRef = builder.objectRef<Row>('BlockInclusion');
const BlockRef = builder.objectRef<Row>('Block');

const str = (value: unknown) => (value === null ? null : String(value));
const num = (value: unknown) => (value === null ? null : Number(value));
const capability = (value: unknown) =>
  value === null || value === undefined ? null : (value as Capability);
const relation = (parent: Row, info: GraphQLResolveInfo) =>
  prefetched(parent, String(info.path.key)) as Row[];

BlockRef.implement({
  fields: (t) => ({
    hash: t.field({ resolve: (p) => String(p.hash), type: 'Hex' }),
    height: t.int({ resolve: (p) => Number(p.height) }),
    internal_id: t.field({
      resolve: (p) => String(p.internal_id),
      type: 'BigInt',
    }),
    timestamp: t.field({
      resolve: (p) => String(p.timestamp),
      type: 'BigInt',
    }),
  }),
});

BlockInclusionRef.implement({
  description:
    'A block that includes the transaction and that the root node accepts (per node).',
  fields: (t) => ({
    block: t.field({ resolve: (p) => p.block as Row, type: BlockRef }),
    transaction_index: t.int({
      nullable: true,
      resolve: (p) => num(p.transaction_index),
    }),
  }),
});

/** The value/token attributes shared by Output and SpentOutput. */
const valueFields = (
  t: PothosSchemaTypes.ObjectFieldBuilder<
    PothosSchemaTypes.ExtendDefaultTypes<Types>,
    Row
  >
) => ({
  fungible_token_amount: t.field({
    nullable: true,
    resolve: (p) => str(p.fungible_token_amount),
    type: 'BigInt',
  }),
  locking_bytecode: t.field({
    resolve: (p) => String(p.locking_bytecode),
    type: 'Hex',
  }),
  nonfungible_token_capability: t.field({
    nullable: true,
    resolve: (p) => capability(p.nonfungible_token_capability),
    type: CapabilityEnum,
  }),
  nonfungible_token_commitment: t.field({
    nullable: true,
    resolve: (p) => str(p.nonfungible_token_commitment),
    type: 'Hex',
  }),
  token_category: t.field({
    nullable: true,
    resolve: (p) => str(p.token_category),
    type: 'Hex',
  }),
  value_satoshis: t.field({
    resolve: (p) => String(p.value_satoshis),
    type: 'BigInt',
  }),
});

SpentOutputRef.implement({
  description:
    "The output an input spends (from the input row's copy of its attributes).",
  fields: valueFields,
});

OutputRef.implement({
  fields: (t) => ({
    ...valueFields(t),
    output_index: t.int({ resolve: (p) => Number(p.output_index) }),
    transaction: t.field({
      description: 'The creating transaction.',
      resolve: (p, _args, _ctx, info) =>
        prefetched(p, String(info.path.key)) as Row,
      type: TransactionRef,
    }),
    transaction_hash: t.field({
      resolve: (p) => String(p.transaction_hash),
      type: 'Hex',
    }),
    transaction_internal_id: t.field({
      resolve: (p) => String(p.transaction_internal_id),
      type: 'BigInt',
    }),
  }),
});

InputRef.implement({
  fields: (t) => ({
    input_index: t.int({ resolve: (p) => Number(p.input_index) }),
    outpoint: t.field({
      resolve: (p) => p.outpoint as Row,
      type: SpentOutputRef,
    }),
    outpoint_index: t.field({
      description: 'BigInt: a coinbase input has 4294967295.',
      resolve: (p) => String(p.outpoint_index),
      type: 'BigInt',
    }),
    outpoint_transaction_hash: t.field({
      resolve: (p) => String(p.outpoint_transaction_hash),
      type: 'Hex',
    }),
    sequence_number: t.field({
      resolve: (p) => String(p.sequence_number),
      type: 'BigInt',
    }),
    transaction: t.field({
      description: 'The spending transaction.',
      resolve: (p, _args, _ctx, info) =>
        prefetched(p, String(info.path.key)) as Row,
      type: TransactionRef,
    }),
    transaction_hash: t.field({
      resolve: (p) => String(p.transaction_hash),
      type: 'Hex',
    }),
    transaction_internal_id: t.field({
      resolve: (p) => String(p.transaction_internal_id),
      type: 'BigInt',
    }),
    unlocking_bytecode: t.field({
      resolve: (p) => String(p.unlocking_bytecode),
      type: 'Hex',
    }),
  }),
});

TransactionRef.implement({
  fields: (t) => ({
    block_inclusions: t.field({
      description:
        "Blocks that include the transaction and that the root's node accepts.",
      resolve: (p, _args, _ctx, info) => relation(p, info),
      type: [BlockInclusionRef],
    }),
    hash: t.field({ resolve: (p) => String(p.hash), type: 'Hex' }),
    input_count: t.int({ resolve: (p) => Number(p.input_count) }),
    inputs: t.field({
      args: {
        limit: t.arg.int(),
        order: t.arg({ defaultValue: 'ASC', type: OrderEnum }),
        where: t.arg({ type: InputFilterInput }),
      },
      resolve: (p, _args, _ctx, info) => relation(p, info),
      type: [InputRef],
    }),
    internal_id: t.field({
      resolve: (p) => String(p.internal_id),
      type: 'BigInt',
    }),
    is_coinbase: t.boolean({ resolve: (p) => Boolean(p.is_coinbase) }),
    locktime: t.field({ resolve: (p) => String(p.locktime), type: 'BigInt' }),
    output_count: t.int({ resolve: (p) => Number(p.output_count) }),
    output_value_satoshis: t.field({
      resolve: (p) => String(p.output_value_satoshis),
      type: 'BigInt',
    }),
    outputs: t.field({
      args: {
        limit: t.arg.int(),
        order: t.arg({ defaultValue: 'ASC', type: OrderEnum }),
        where: t.arg({ type: OutputFilterInput }),
      },
      resolve: (p, _args, _ctx, info) => relation(p, info),
      type: [OutputRef],
    }),
    size_bytes: t.int({ resolve: (p) => Number(p.size_bytes) }),
    version: t.int({ resolve: (p) => Number(p.version) }),
  }),
});

const pageType = (name: string, item: typeof TransactionRef) =>
  builder.objectRef<Page>(name).implement({
    description:
      'One page; `has_more` signals truncation (R33). Pass `end_cursor` as `after` (or as `before` for a DESC transaction page) for the next page.',
    fields: (t) => ({
      end_cursor: t.string({
        nullable: true,
        resolve: (p) => p.end_cursor,
      }),
      has_more: t.boolean({ resolve: (p) => p.has_more }),
      nodes: t.field({ resolve: (p) => p.nodes, type: [item] }),
    }),
  });

const TransactionPageRef = pageType('TransactionPage', TransactionRef);
const InputPageRef = pageType('InputPage', InputRef);
const OutputPageRef = pageType('OutputPage', OutputRef);

/* ------------------------------------------------------------------ */
/* group-by (S3)                                                        */
/* ------------------------------------------------------------------ */

type GroupColumn = 'LOCKING_BYTECODE' | 'TOKEN_CATEGORY';
type GroupOrderField =
  | 'COUNT'
  | 'KEY'
  | 'SUM_FUNGIBLE_TOKEN_AMOUNT'
  | 'SUM_VALUE_SATOSHIS';

const GroupColumnEnum = builder.enumType('OutputGroupColumn', {
  values: ['LOCKING_BYTECODE', 'TOKEN_CATEGORY'] as const,
});

const GroupOrderFieldEnum = builder.enumType('OutputGroupOrderField', {
  values: [
    'COUNT',
    'KEY',
    'SUM_FUNGIBLE_TOKEN_AMOUNT',
    'SUM_VALUE_SATOSHIS',
  ] as const,
});

const GroupOrderInput = builder
  .inputRef<{ by: GroupOrderField; direction?: Order | null }>(
    'OutputGroupOrder'
  )
  .implement({
    fields: (t) => ({
      by: t.field({ required: true, type: GroupOrderFieldEnum }),
      direction: t.field({ defaultValue: 'DESC', type: OrderEnum }),
    }),
  });

interface GroupRow {
  locking_bytecode: string | null;
  token_category: string | null;
  count: string;
  sum_fungible_token_amount: string;
  sum_value_satoshis: string;
}

const GroupKeyRef = builder.objectRef<GroupRow>('OutputGroupKey').implement({
  fields: (t) => ({
    locking_bytecode: t.field({
      nullable: true,
      resolve: (p) => p.locking_bytecode,
      type: 'Hex',
    }),
    token_category: t.field({
      nullable: true,
      resolve: (p) => p.token_category,
      type: 'Hex',
    }),
  }),
});

const GroupSumRef = builder.objectRef<GroupRow>('OutputGroupSum').implement({
  fields: (t) => ({
    fungible_token_amount: t.field({
      resolve: (p) => p.sum_fungible_token_amount,
      type: 'BigInt',
    }),
    value_satoshis: t.field({
      resolve: (p) => p.sum_value_satoshis,
      type: 'BigInt',
    }),
  }),
});

const GroupAggregateRef = builder
  .objectRef<GroupRow>('OutputGroupAggregate')
  .implement({
    fields: (t) => ({
      count: t.field({ resolve: (p) => p.count, type: 'BigInt' }),
      sum: t.field({ resolve: (p) => p, type: GroupSumRef }),
    }),
  });

const GroupRef = builder.objectRef<GroupRow>('OutputGroup').implement({
  fields: (t) => ({
    aggregate: t.field({ resolve: (p) => p, type: GroupAggregateRef }),
    key: t.field({ resolve: (p) => p, type: GroupKeyRef }),
  }),
});

const zeroCategory = `toFixedString(unhex('${'00'.repeat(32)}'), 32)`;
const groupColumns: { [column in GroupColumn]: readonly [string, string] } = {
  LOCKING_BYTECODE: ['locking_bytecode', 'lower(hex(locking_bytecode))'],
  TOKEN_CATEGORY: [
    'token_category',
    `if(token_category = ${zeroCategory}, NULL, lower(hex(token_category)))`,
  ],
};

const runGroups = async (
  db: ApiDb,
  sql: Sql,
  args: {
    where?: OutputFilter | null;
    group_by: readonly GroupColumn[];
    order_by?: { by: GroupOrderField; direction?: Order | null } | null;
    limit: number;
  }
) => {
  const columns = [...new Set(args.group_by)];
  if (columns.length === 0) {
    throw new GraphQLError('group_by needs at least one column.');
  }
  const raw = columns.map((column) => groupColumns[column][0]);
  const direction = args.order_by?.direction ?? 'DESC';
  const aggregates: { [field in GroupOrderField]: string } = {
    COUNT: 'count()',
    KEY: raw.join(', '),
    SUM_FUNGIBLE_TOKEN_AMOUNT:
      'sum(toInt128(ifNull(fungible_token_amount, 0)))',
    SUM_VALUE_SATOSHIS: 'sum(toInt128(value_satoshis))',
  };
  const orderBy =
    args.order_by === undefined || args.order_by === null
      ? raw.join(', ')
      : `${aggregates[args.order_by.by]} ${direction}, ${raw.join(', ')}`;
  const statement = `SELECT ${columns
    .map(
      (column) =>
        `${groupColumns[column][1]} AS \`g.${groupColumns[column][0]}\``
    )
    .join(', ')},
    toString(count()) AS count,
    toString(sum(toInt128(ifNull(fungible_token_amount, 0)))) AS sum_fungible_token_amount,
    toString(sum(toInt128(value_satoshis))) AS sum_value_satoshis
  FROM ${sql.view('output_at')}
  WHERE ${unspentOutputPredicate(sql, args.where)}
  GROUP BY ${raw.join(', ')}
  ORDER BY ${orderBy}
  LIMIT ${sql.bind(args.limit, 'UInt32')}`;
  const rows = await db.query<{ [key: string]: string | null }>(
    'unspent_output_groups',
    statement,
    sql.params
  );
  return rows.map(
    (row): GroupRow => ({
      count: String(row.count),
      locking_bytecode: row['g.locking_bytecode'] ?? null,
      sum_fungible_token_amount: String(row.sum_fungible_token_amount),
      sum_value_satoshis: String(row.sum_value_satoshis),
      token_category: row['g.token_category'] ?? null,
    })
  );
};

/* ------------------------------------------------------------------ */
/* roots                                                                */
/* ------------------------------------------------------------------ */

const checkLimit = (limit: number, max: number) => {
  if (!Number.isInteger(limit) || limit < 1 || limit > max) {
    throw new GraphQLError(`limit must be between 1 and ${max}.`, {
      extensions: { code: 'BAD_USER_INPUT' },
    });
  }
};

const requestSnapshot = async (context: ApiContext, node?: string) => {
  if (context.snapshot === undefined) {
    throw new GraphQLError('No request snapshot (server misconfigured).');
  }
  return context.snapshot.get(node);
};

/**
 * The root resolvers, shared by Query and Subscription: each takes the
 * snapshot it must read at (the request's, or a live re-run's).
 */
const roots = {
  inputs: async (
    db: ApiDb,
    snapshot: VisibilitySnapshot,
    args: { where?: InputFilter | null; after?: string | null; limit: number },
    info: GraphQLResolveInfo
  ) => {
    checkLimit(args.limit, maxLimits.inputs);
    return runInputs(db, new Sql(snapshot), info, args.where, args);
  },
  transactions: async (
    db: ApiDb,
    snapshot: VisibilitySnapshot,
    args: {
      where?: TransactionFilter | null;
      order?: Order | null;
      after?: bigint | null;
      before?: bigint | null;
      limit: number;
    },
    info: GraphQLResolveInfo
  ) => {
    checkLimit(args.limit, maxLimits.transactions);
    const sql = new Sql(snapshot);
    return runTransactions(
      db,
      sql,
      info,
      transactionPredicate(sql, args.where),
      args
    );
  },
  unspent_outputs: async (
    db: ApiDb,
    snapshot: VisibilitySnapshot,
    args: { where?: OutputFilter | null; after?: string | null; limit: number },
    info: GraphQLResolveInfo
  ) => {
    checkLimit(args.limit, maxLimits.outputs);
    const sql = new Sql(snapshot);
    return runUnspentOutputs(
      db,
      sql,
      info,
      unspentOutputPredicate(sql, args.where),
      args
    );
  },
};

builder.queryType({
  fields: (t) => ({
    inputs: t.field({
      args: {
        after: t.arg.string(),
        limit: t.arg.int({ required: true }),
        node: t.arg.string({ required: true }),
        where: t.arg({ type: InputFilterInput }),
      },
      description:
        'Inputs of transactions the node accepts, keyset-paginated on (transaction_internal_id, input_index).',
      resolve: async (_root, args, context, info) =>
        roots.inputs(
          context.db,
          await requestSnapshot(context, args.node),
          args,
          info
        ),
      type: InputPageRef,
    }),
    transaction_watermark: t.field({
      description:
        'The newest transaction internal_id in the request snapshot (node-agnostic).',
      nullable: true,
      resolve: async (_root, _args, context) => {
        const snapshot = await requestSnapshot(context);
        const sql = new Sql(snapshot);
        const [row] = await context.db.query<{ id: string | null }>(
          'transaction_watermark',
          `SELECT if(count() = 0, NULL, toString(max(internal_id))) AS id FROM ${sql.view(
            'transaction_at'
          )}`,
          sql.params
        );
        return row?.id ?? null;
      },
      type: 'BigInt',
    }),
    transactions: t.field({
      args: {
        after: t.arg({ type: 'BigInt' }),
        before: t.arg({ type: 'BigInt' }),
        limit: t.arg.int({ required: true }),
        node: t.arg.string({ required: true }),
        order: t.arg({ defaultValue: 'ASC', type: OrderEnum }),
        where: t.arg({ type: TransactionFilterInput }),
      },
      description:
        'Transactions the node accepts (in a block it accepts or in its mempool), keyset on internal_id: after < internal_id < before.',
      resolve: async (_root, args, context, info) =>
        roots.transactions(
          context.db,
          await requestSnapshot(context, args.node),
          args,
          info
        ),
      type: TransactionPageRef,
    }),
    unspent_output_groups: t.field({
      args: {
        group_by: t.arg({ required: true, type: [GroupColumnEnum] }),
        limit: t.arg.int({ required: true }),
        node: t.arg.string({ required: true }),
        order_by: t.arg({ type: GroupOrderInput }),
        where: t.arg({ type: OutputFilterInput }),
      },
      description:
        "Group-by aggregate over the node's unspent outputs (computed at query time).",
      resolve: async (_root, args, context) => {
        checkLimit(args.limit, maxLimits.groups);
        const snapshot = await requestSnapshot(context, args.node);
        return runGroups(context.db, new Sql(snapshot), args);
      },
      type: [GroupRef],
    }),
    unspent_outputs: t.field({
      args: {
        after: t.arg.string(),
        limit: t.arg.int({ required: true }),
        node: t.arg.string({ required: true }),
        where: t.arg({ type: OutputFilterInput }),
      },
      description:
        "The node's unspent outputs, computed at query time from outputs minus spending inputs (UTXO tables off), keyset on (transaction_internal_id, output_index).",
      resolve: async (_root, args, context, info) =>
        roots.unspent_outputs(
          context.db,
          await requestSnapshot(context, args.node),
          args,
          info
        ),
      type: OutputPageRef,
    }),
  }),
});

/** Topic key of a live query: the operation, its fragments and variables. */
export const liveTopicKey = (info: GraphQLResolveInfo, node: number) =>
  [
    node,
    info.fieldName,
    print(info.operation),
    ...Object.values(info.fragments).map((fragment) => print(fragment)),
    resultKey(info.variableValues),
  ].join('\n');

builder.subscriptionType({
  description:
    'Live queries: the full result, re-delivered when it changes after the node watermark advances.',
  fields: (t) => ({
    transactions: t.field({
      args: {
        after: t.arg({ type: 'BigInt' }),
        before: t.arg({ type: 'BigInt' }),
        limit: t.arg.int({ required: true }),
        node: t.arg.string({ required: true }),
        order: t.arg({ defaultValue: 'ASC', type: OrderEnum }),
        where: t.arg({ type: TransactionFilterInput }),
      },
      resolve: (payload) => payload as Page,
      subscribe: async (_root, args, context, info) => {
        const nodeId = await context.db.nodeId(args.node);
        return context.hub.subscribe(
          liveTopicKey(info, nodeId),
          nodeId,
          async (snapshot) =>
            roots.transactions(context.db, snapshot, args, info)
        );
      },
      type: TransactionPageRef,
    }),
    unspent_outputs: t.field({
      args: {
        after: t.arg.string(),
        limit: t.arg.int({ required: true }),
        node: t.arg.string({ required: true }),
        where: t.arg({ type: OutputFilterInput }),
      },
      resolve: (payload) => payload as Page,
      subscribe: async (_root, args, context, info) => {
        const nodeId = await context.db.nodeId(args.node);
        return context.hub.subscribe(
          liveTopicKey(info, nodeId),
          nodeId,
          async (snapshot) =>
            roots.unspent_outputs(context.db, snapshot, args, info)
        );
      },
      type: OutputPageRef,
    }),
  }),
});

export const schema = builder.toSchema();
