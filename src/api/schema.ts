/* eslint-disable camelcase, @typescript-eslint/naming-convention, functional/no-throw-statement, complexity, max-params, max-lines, sort-keys */
// cspell:ignore clickhouse pothos unhex authchain authchains authhead bytecode
/**
 * The Chaingraph v2 GraphQL schema (Pothos). Shape and requirement mapping:
 * docs/clickhouse-port/phase2-api.md; committed SDL: docs/clickhouse-port/v2-schema.graphql
 * (checked by schema.spec.ts).
 *
 * - Per-node roots take `node: String!` (R1); per-node relationships take an
 *   optional `node:` (default: the node of the level above). Nothing in the
 *   schema accepts a snapshot/watermark parameter (R2): the snapshot comes
 *   from the request context (`PinnedSnapshot`), read once per request.
 * - Each root field is one ClickHouse statement (compile.ts); nested fields
 *   only read prefetched data.
 */
import SchemaBuilder from '@pothos/core';
import type { GraphQLResolveInfo } from 'graphql';
import { GraphQLError, Kind, print } from 'graphql';

import type { VisibilitySnapshot } from '../store/clickhouse/visibility.js';

import type {
  AggregateRow,
  AggregateSpec,
  GroupOrder,
  KeyKind,
} from './aggregates.js';
import {
  aggregateSpecs,
  requestedAggregates,
  runAggregate,
  runGroups,
} from './aggregates.js';
import {
  encodeBlock,
  encodeBlockHeader,
  encodeTransaction,
  parseBytecodePattern,
  parseBytecodePatternRedeem,
} from './bytecode.js';
import type { Page, Row, Sel } from './compile.js';
import { pageNodeSels, prefetched, rootSels } from './compile.js';
import type { ApiDb, PinnedSnapshot } from './db.js';
import { singleNodeEnv } from './db.js';
import type { LiveHub } from './live.js';
import { resultKey } from './live.js';
import type {
  BlocksArgs,
  InputsArgs,
  MempoolArgs,
  OutputsArgs,
  RootEnv,
  TransactionsArgs,
} from './roots.js';
import {
  rowSets,
  runBlocks,
  runBlocksByHash,
  runBlockTip,
  runInputs,
  runMempool,
  runNode,
  runNodes,
  runOutputs,
  runOutputsByTransactionHash,
  runTransactions,
  runTransactionsByHash,
  runTransactionWatermark,
  runUnspentOutputs,
  runWatermark,
} from './roots.js';
import { sendTransaction } from './send.js';
import type {
  BigIntComparison,
  BlockFilter,
  BlockInclusionFilter,
  BlockTransactionFilter,
  BlockWindow,
  BooleanComparison,
  Capability,
  CapabilityComparison,
  HexComparison,
  InputFilter,
  IntComparison,
  NodeBlockFilter,
  NodeTransactionFilter,
  OutputFilter,
  TimestampComparison,
  TransactionFilter,
} from './sql.js';

export { maxLimits } from './roots.js';

export interface ApiContext {
  db: ApiDb;
  hub: LiveHub;
  /** The agent's internal API (send_transaction, R38). */
  agentUrl?: string;
  /** Set per execution by the snapshot plugin (server.ts). */
  snapshot?: PinnedSnapshot;
}

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

const parseTimestamp = (value: unknown) => {
  if (typeof value !== 'string' || Number.isNaN(Date.parse(value))) {
    throw new GraphQLError('Timestamp values are ISO-8601 strings.');
  }
  return value;
};

interface Types {
  Context: ApiContext;
  DefaultFieldNullability: false;
  Scalars: {
    Hex: { Input: string; Output: string };
    BigInt: { Input: bigint; Output: bigint | number | string };
    Timestamp: { Input: string; Output: number | string };
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

builder.scalarType('Timestamp', {
  description: 'A point in time, ISO-8601 in UTC with milliseconds.',
  parseLiteral: (node) => {
    if (node.kind !== Kind.STRING) {
      throw new GraphQLError('Timestamp is a string.');
    }
    return parseTimestamp(node.value);
  },
  parseValue: parseTimestamp,
  serialize: (value) => new Date(Number(value)).toISOString(),
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
/* comparisons                                                          */
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
      _nin: t.intList(),
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
      _is_null: t.boolean(),
      _lt: t.field({ type: 'BigInt' }),
      _lte: t.field({ type: 'BigInt' }),
      _neq: t.field({ type: 'BigInt' }),
      _nin: t.field({ type: ['BigInt'] }),
    }),
  });

const HexComparisonInput = builder
  .inputRef<HexComparison>('HexComparison')
  .implement({
    description:
      'Byte comparisons (lexicographic order). `_prefix` / `_prefix_in` match leading bytes; on locking bytecode and NFT commitments they use the sort-key projections. Lists may hold up to 10,000 values (bound in chunks of 1,000).',
    fields: (t) => ({
      _eq: t.field({ type: 'Hex' }),
      _gt: t.field({ type: 'Hex' }),
      _gte: t.field({ type: 'Hex' }),
      _in: t.field({ type: ['Hex'] }),
      _is_null: t.boolean(),
      _lt: t.field({ type: 'Hex' }),
      _lte: t.field({ type: 'Hex' }),
      _neq: t.field({ type: 'Hex' }),
      _nin: t.field({ type: ['Hex'] }),
      _prefix: t.field({ type: 'Hex' }),
      _prefix_in: t.field({ type: ['Hex'] }),
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
      _nin: t.field({ type: [CapabilityEnum] }),
    }),
  });

const BooleanComparisonInput = builder
  .inputRef<BooleanComparison>('BooleanComparison')
  .implement({
    fields: (t) => ({ _eq: t.boolean(), _neq: t.boolean() }),
  });

const TimestampComparisonInput = builder
  .inputRef<TimestampComparison>('TimestampComparison')
  .implement({
    fields: (t) => ({
      _eq: t.field({ type: 'Timestamp' }),
      _gt: t.field({ type: 'Timestamp' }),
      _gte: t.field({ type: 'Timestamp' }),
      _is_null: t.boolean(),
      _lt: t.field({ type: 'Timestamp' }),
      _lte: t.field({ type: 'Timestamp' }),
      _neq: t.field({ type: 'Timestamp' }),
    }),
  });

/* ------------------------------------------------------------------ */
/* filters                                                              */
/* ------------------------------------------------------------------ */

const OutputFilterInput = builder.inputRef<OutputFilter>('OutputFilter');
const InputFilterInput = builder.inputRef<InputFilter>('InputFilter');
const TransactionFilterInput =
  builder.inputRef<TransactionFilter>('TransactionFilter');
const BlockFilterInput = builder.inputRef<BlockFilter>('BlockFilter');

OutputFilterInput.implement({
  description:
    'Predicates on one output row (or, as `outpoint`, on the spent output of one input row). All fields of one object bind to the same row.',
  fields: (t) => ({
    _and: t.field({ type: [OutputFilterInput] }),
    _not: t.field({ type: OutputFilterInput }),
    _or: t.field({ type: [OutputFilterInput] }),
    fungible_token_amount: t.field({ type: BigIntComparisonInput }),
    locking_bytecode: t.field({ type: HexComparisonInput }),
    nonfungible_token_capability: t.field({ type: CapabilityComparisonInput }),
    nonfungible_token_commitment: t.field({ type: HexComparisonInput }),
    output_index: t.field({ type: IntComparisonInput }),
    spent_by: t.field({
      description:
        'EXISTS a stored input spending this output (node-agnostic: conflicting spends included), matching. For a spend a node accepts, add `transaction: { block_window: {} }` (the node of the level).',
      type: InputFilterInput,
    }),
    token_category: t.field({ type: HexComparisonInput }),
    transaction: t.field({
      description: 'The creating transaction matches.',
      type: TransactionFilterInput,
    }),
    transaction_hash: t.field({ type: HexComparisonInput }),
    transaction_internal_id: t.field({ type: BigIntComparisonInput }),
    value_satoshis: t.field({ type: BigIntComparisonInput }),
  }),
});

InputFilterInput.implement({
  description:
    'Predicates on one input row; `outpoint` fields bind to the same row (same-row conjunction).',
  fields: (t) => ({
    _and: t.field({ type: [InputFilterInput] }),
    _not: t.field({ type: InputFilterInput }),
    _or: t.field({ type: [InputFilterInput] }),
    input_index: t.field({ type: IntComparisonInput }),
    outpoint: t.field({
      description: 'The spent output matches (no match for a coinbase input).',
      type: OutputFilterInput,
    }),
    outpoint_index: t.field({ type: BigIntComparisonInput }),
    outpoint_transaction_hash: t.field({ type: HexComparisonInput }),
    sequence_number: t.field({ type: BigIntComparisonInput }),
    transaction: t.field({
      description: 'The spending transaction matches.',
      type: TransactionFilterInput,
    }),
    transaction_hash: t.field({ type: HexComparisonInput }),
    transaction_internal_id: t.field({ type: BigIntComparisonInput }),
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

TransactionFilterInput.implement({
  description:
    'Predicates on one transaction. `has_output` / `has_input` are EXISTS over one output/input row; `block_window`, `in_block` and `in_mempool` are per node.',
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
    in_block: t.field({
      description: 'In a block the node accepts that matches (per node).',
      type: BlockFilterInput,
    }),
    in_mempool: t.boolean({
      description: "In (true) or not in (false) the node's mempool.",
    }),
    input_count: t.field({ type: IntComparisonInput }),
    internal_id: t.field({ type: BigIntComparisonInput }),
    is_coinbase: t.field({ type: BooleanComparisonInput }),
    locktime: t.field({ type: BigIntComparisonInput }),
    output_count: t.field({ type: IntComparisonInput }),
    output_value_satoshis: t.field({ type: BigIntComparisonInput }),
    size_bytes: t.field({ type: IntComparisonInput }),
    version: t.field({ type: IntComparisonInput }),
  }),
});

BlockFilterInput.implement({
  fields: (t) => ({
    _and: t.field({ type: [BlockFilterInput] }),
    _not: t.field({ type: BlockFilterInput }),
    _or: t.field({ type: [BlockFilterInput] }),
    bits: t.field({ type: BigIntComparisonInput }),
    has_transaction: t.field({
      description: 'EXISTS a transaction of the block matching.',
      type: TransactionFilterInput,
    }),
    hash: t.field({ type: HexComparisonInput }),
    height: t.field({ type: IntComparisonInput }),
    internal_id: t.field({ type: BigIntComparisonInput }),
    merkle_root: t.field({ type: HexComparisonInput }),
    nonce: t.field({ type: BigIntComparisonInput }),
    previous_block_hash: t.field({ type: HexComparisonInput }),
    size_bytes: t.field({ type: IntComparisonInput }),
    timestamp: t.field({ type: BigIntComparisonInput }),
    transaction_count: t.field({ type: IntComparisonInput }),
    version: t.field({ type: IntComparisonInput }),
  }),
});

const NodeTransactionFilterInput = builder.inputRef<NodeTransactionFilter>(
  'NodeTransactionFilter'
);
NodeTransactionFilterInput.implement({
  description: "A mempool entry: the node's validation of a transaction.",
  fields: (t) => ({
    _and: t.field({ type: [NodeTransactionFilterInput] }),
    _not: t.field({ type: NodeTransactionFilterInput }),
    _or: t.field({ type: [NodeTransactionFilterInput] }),
    transaction: t.field({ type: TransactionFilterInput }),
    transaction_hash: t.field({ type: HexComparisonInput }),
    transaction_internal_id: t.field({ type: BigIntComparisonInput }),
    validated_at: t.field({ type: TimestampComparisonInput }),
  }),
});

const NodeBlockFilterInput =
  builder.inputRef<NodeBlockFilter>('NodeBlockFilter');
NodeBlockFilterInput.implement({
  description: "A node's acceptance of a block.",
  fields: (t) => ({
    _and: t.field({ type: [NodeBlockFilterInput] }),
    _not: t.field({ type: NodeBlockFilterInput }),
    _or: t.field({ type: [NodeBlockFilterInput] }),
    accepted_at: t.field({ type: TimestampComparisonInput }),
    block: t.field({ type: BlockFilterInput }),
    block_internal_id: t.field({ type: BigIntComparisonInput }),
    height: t.field({ type: IntComparisonInput }),
  }),
});

const BlockInclusionFilterInput = builder.inputRef<BlockInclusionFilter>(
  'BlockInclusionFilter'
);
BlockInclusionFilterInput.implement({
  fields: (t) => ({
    _and: t.field({ type: [BlockInclusionFilterInput] }),
    _not: t.field({ type: BlockInclusionFilterInput }),
    _or: t.field({ type: [BlockInclusionFilterInput] }),
    block: t.field({ type: BlockFilterInput }),
    transaction_index: t.field({ type: IntComparisonInput }),
  }),
});

const BlockTransactionFilterInput = builder.inputRef<BlockTransactionFilter>(
  'BlockTransactionFilter'
);
BlockTransactionFilterInput.implement({
  fields: (t) => ({
    _and: t.field({ type: [BlockTransactionFilterInput] }),
    _not: t.field({ type: BlockTransactionFilterInput }),
    _or: t.field({ type: [BlockTransactionFilterInput] }),
    transaction: t.field({ type: TransactionFilterInput }),
    transaction_index: t.field({ type: IntComparisonInput }),
  }),
});

/* ------------------------------------------------------------------ */
/* order_by inputs                                                      */
/* ------------------------------------------------------------------ */

interface OrderBy {
  [field: string]: unknown;
}

const orderInput = (
  name: string,
  fields: readonly string[],
  nested: {
    [field: string]: PothosSchemaTypes.InputObjectRef<
      PothosSchemaTypes.ExtendDefaultTypes<Types>,
      OrderBy
    >;
  } = {},
  description?: string
) =>
  builder.inputRef<OrderBy>(name).implement({
    ...(description === undefined ? {} : { description }),
    fields: (t) => ({
      ...Object.fromEntries(
        fields.map((field) => [field, t.field({ type: OrderEnum })])
      ),
      ...Object.fromEntries(
        Object.entries(nested).map(([field, type]) => [
          field,
          t.field({ type }),
        ])
      ),
    }),
  });

const TransactionKeyOrderBy = orderInput(
  'TransactionKeyOrderBy',
  ['hash', 'internal_id'],
  {},
  'Ordering through the transaction relationship (its key columns, stored on the row).'
);
const BlockKeyOrderBy = orderInput(
  'BlockKeyOrderBy',
  ['hash', 'height', 'internal_id'],
  {},
  'Ordering through the block relationship (its key columns, stored on the row).'
);
const TransactionOrderBy = orderInput('TransactionOrderBy', [
  'hash',
  'input_count',
  'internal_id',
  'locktime',
  'output_count',
  'output_value_satoshis',
  'size_bytes',
  'version',
]);
const OutputOrderBy = orderInput(
  'OutputOrderBy',
  [
    'fungible_token_amount',
    'locking_bytecode',
    'output_index',
    'token_category',
    'transaction_hash',
    'transaction_internal_id',
    'value_satoshis',
  ],
  { transaction: TransactionKeyOrderBy }
);
const InputOrderBy = orderInput(
  'InputOrderBy',
  [
    'input_index',
    'outpoint_index',
    'outpoint_transaction_hash',
    'sequence_number',
    'transaction_hash',
    'transaction_internal_id',
  ],
  { transaction: TransactionKeyOrderBy }
);
const BlockOrderBy = orderInput('BlockOrderBy', [
  'hash',
  'height',
  'internal_id',
  'size_bytes',
  'timestamp',
  'transaction_count',
]);
const NodeTransactionOrderBy = orderInput(
  'NodeTransactionOrderBy',
  ['transaction_internal_id', 'validated_at'],
  { transaction: TransactionKeyOrderBy }
);
const NodeBlockOrderBy = orderInput(
  'NodeBlockOrderBy',
  ['accepted_at', 'block_internal_id', 'height'],
  { block: BlockKeyOrderBy }
);
const BlockInclusionOrderBy = orderInput(
  'BlockInclusionOrderBy',
  ['block_internal_id', 'transaction_index'],
  { block: BlockKeyOrderBy }
);
const BlockTransactionOrderBy = orderInput(
  'BlockTransactionOrderBy',
  ['transaction_index', 'transaction_internal_id'],
  { transaction: TransactionKeyOrderBy }
);
const TimelineOrderBy = orderInput('NodeTransactionHistoryOrderBy', [
  'internal_id',
  'replaced_at',
  'validated_at',
]);
const MigrationOrderBy = orderInput('AuthchainMigrationOrderBy', [
  'migration_index',
]);

/* ------------------------------------------------------------------ */
/* object types                                                         */
/* ------------------------------------------------------------------ */

const TransactionRef = builder.objectRef<Row>('Transaction');
const OutputRef = builder.objectRef<Row>('Output');
const InputRef = builder.objectRef<Row>('Input');
const BlockRef = builder.objectRef<Row>('Block');
const BlockInclusionRef = builder.objectRef<Row>('BlockInclusion');
const BlockTransactionRef = builder.objectRef<Row>('BlockTransaction');
const NodeTransactionRef = builder.objectRef<Row>('NodeTransaction');
const TimelineRef = builder.objectRef<Row>('NodeTransactionHistory');
const NodeBlockRef = builder.objectRef<Row>('NodeBlock');
const NodeRef = builder.objectRef<Row>('Node');
const AuthchainRef = builder.objectRef<Row>('Authchain');
const MigrationRef = builder.objectRef<Row>('AuthchainMigration');

type FieldBuilder = PothosSchemaTypes.ObjectFieldBuilder<
  PothosSchemaTypes.ExtendDefaultTypes<Types>,
  Row
>;

const str = (value: unknown) =>
  value === null || value === undefined ? null : String(value);
const num = (value: unknown) =>
  value === null || value === undefined ? null : Number(value);
const capability = (value: unknown) =>
  value === null || value === undefined || value === ''
    ? null
    : (value as Capability);
const many = (parent: Row, info: GraphQLResolveInfo) =>
  prefetched(parent, String(info.path.key)) as Row[];
const one = (parent: Row, info: GraphQLResolveInfo) =>
  (prefetched(parent, String(info.path.key)) ?? null) as Row | null;
const hidden = (parent: Row, key: string) =>
  (prefetched(parent, key) ?? null) as Row | null;

const hexField = (t: FieldBuilder, name: string, description?: string) =>
  t.field({
    ...(description === undefined ? {} : { description }),
    resolve: (p) => String(p[name]),
    type: 'Hex',
  });
const nullableHex = (t: FieldBuilder, name: string, description?: string) =>
  t.field({
    ...(description === undefined ? {} : { description }),
    nullable: true,
    resolve: (p) => str(p[name]),
    type: 'Hex',
  });
const bigIntField = (t: FieldBuilder, name: string, description?: string) =>
  t.field({
    ...(description === undefined ? {} : { description }),
    resolve: (p) => String(p[name]),
    type: 'BigInt',
  });
const intField = (t: FieldBuilder, name: string, description?: string) =>
  t.int({
    ...(description === undefined ? {} : { description }),
    resolve: (p) => Number(p[name]),
  });
const timestampField = (t: FieldBuilder, name: string, description?: string) =>
  t.field({
    ...(description === undefined ? {} : { description }),
    nullable: true,
    resolve: (p) => str(p[name]),
    type: 'Timestamp',
  });

const nodeArg = (t: FieldBuilder) =>
  t.arg.string({
    description:
      'The node whose view this relationship reads (default: the node of the level above; required below a node-agnostic root). It must be pinned by the request: any `node:` argument of the operation is.',
  });

const acceptedByArg = (t: FieldBuilder) =>
  t.arg.string({
    description:
      'Opt-in acceptance: only rows the named node accepts (default: every stored row, node-agnostic). It must be pinned by the request: any `node:` or `accepted_by:` argument of the operation is.',
  });

/** `accepted(node:)`: per-node exactness, the node is required. */
const acceptedField = (t: FieldBuilder, description: string) =>
  t.boolean({
    args: {
      node: t.arg.string({
        description:
          'The node (required: no field answers "accepted by any node"). Pinned by the request snapshot.',
        required: true,
      }),
    },
    description,
    resolve: (p, _a, _c, info) => many(p, info).length > 0,
  });

const outputListArgs = (t: FieldBuilder) => ({
  limit: t.arg.int(),
  offset: t.arg.int(),
  order: t.arg({ defaultValue: 'ASC', type: OrderEnum }),
  order_by: t.arg({ type: [OutputOrderBy] }),
  where: t.arg({ type: OutputFilterInput }),
});

/* ---- Transaction ---- */

const encodedTransaction = (row: Row) =>
  encodeTransaction({
    inputs: (hidden(row, '__enc_inputs') as unknown as Row[]).map((input) => ({
      outpoint_index: String(input.outpoint_index),
      outpoint_transaction_hash: String(input.outpoint_transaction_hash),
      sequence_number: String(input.sequence_number),
      unlocking_bytecode: String(input.unlocking_bytecode),
    })),
    locktime: String(row.locktime),
    outputs: (hidden(row, '__enc_outputs') as unknown as Row[]).map(
      (output) => ({
        fungible_token_amount: str(output.fungible_token_amount),
        locking_bytecode: String(output.locking_bytecode),
        nonfungible_token_capability: str(output.nonfungible_token_capability),
        nonfungible_token_commitment: str(output.nonfungible_token_commitment),
        token_category: str(output.token_category),
        value_satoshis: String(output.value_satoshis),
      })
    ),
    version: String(row.version),
  });

const inputValue = (row: Row) =>
  BigInt(String(hidden(row, '__input_value')?.value ?? '0'));

TransactionRef.implement({
  fields: (t) => ({
    accepted: acceptedField(
      t,
      'Whether the node accepts the transaction (in a block it accepts or in its mempool).'
    ),
    authchain_branches: t.field({
      args: { limit: t.arg.int(), offset: t.arg.int() },
      description:
        'Node-agnostic: every branch of the identity-output tree from this transaction, over all stored spends of output 0 (conflicting spends and stale blocks included): one Authchain per path to an output 0 no stored transaction spends (`unspent_authhead`) or to the depth limit. For fork analysis; the chain one node accepts is `authchains(node:)`. Longest first.',
      resolve: (p, _a, _c, info) => many(p, info),
      type: [AuthchainRef],
    }),
    authchains: t.field({
      args: { node: nodeArg(t) },
      description:
        "Per node: the authchain from this transaction (v1 `collect_authchains` at the node's view: output 0 followed through spends the node accepts, to the unspent authhead). At most one element.",
      resolve: (p, _a, _c, info) => many(p, info),
      type: [AuthchainRef],
    }),
    block_inclusions: t.field({
      args: {
        accepted_by: acceptedByArg(t),
        limit: t.arg.int(),
        offset: t.arg.int(),
        order_by: t.arg({ type: [BlockInclusionOrderBy] }),
        where: t.arg({ type: BlockInclusionFilterInput }),
      },
      description:
        'Every stored block that includes the transaction (node-agnostic: blocks of stale forks included), in the order the blocks were stored (`order_by: { block: { height: ASC } }` for height order). `accepted_by: node` keeps the blocks that node accepts; `accepted(node:)` on each inclusion tells them apart.',
      resolve: (p, _a, _c, info) => many(p, info),
      type: [BlockInclusionRef],
    }),
    data_carrier_outputs: t.field({
      args: outputListArgs(t),
      description:
        'Outputs with value 0 or a locking bytecode beginning with OP_RETURN.',
      resolve: (p, _a, _c, info) => many(p, info),
      type: [OutputRef],
    }),
    encoded_hex: t.field({
      description: 'The transaction in the P2P network format.',
      resolve: encodedTransaction,
      type: 'Hex',
    }),
    fee_satoshis: t.field({
      description:
        'Input value minus output value (0 for a coinbase transaction).',
      resolve: (p) =>
        p.is_coinbase === true
          ? '0'
          : String(inputValue(p) - BigInt(String(p.output_value_satoshis))),
      type: 'BigInt',
    }),
    hash: hexField(t, 'hash'),
    identity_output: t.field({
      args: { where: t.arg({ type: OutputFilterInput }) },
      description: 'Output 0.',
      nullable: true,
      resolve: (p, _a, _c, info) => one(p, info),
      type: OutputRef,
    }),
    input_count: intField(t, 'input_count'),
    input_value_satoshis: t.field({
      description:
        'The value of the outputs the inputs spend (outputs not stored count 0).',
      resolve: (p) => String(inputValue(p)),
      type: 'BigInt',
    }),
    inputs: t.field({
      args: {
        limit: t.arg.int(),
        offset: t.arg.int(),
        order: t.arg({ defaultValue: 'ASC', type: OrderEnum }),
        order_by: t.arg({ type: [InputOrderBy] }),
        where: t.arg({ type: InputFilterInput }),
      },
      resolve: (p, _a, _c, info) => many(p, info),
      type: [InputRef],
    }),
    internal_id: bigIntField(t, 'internal_id'),
    is_coinbase: t.boolean({
      resolve: (p) => p.is_coinbase === true || p.is_coinbase === 1,
    }),
    locktime: bigIntField(t, 'locktime'),
    node_validation_timeline: t.field({
      args: {
        limit: t.arg.int(),
        node: nodeArg(t),
        offset: t.arg.int(),
        order_by: t.arg({ type: [TimelineOrderBy] }),
      },
      description:
        "The node's past validations of the transaction (mempool history: replaced or confirmed).",
      resolve: (p, _a, _c, info) => many(p, info),
      type: [TimelineRef],
    }),
    node_validations: t.field({
      args: {
        limit: t.arg.int(),
        node: nodeArg(t),
        where: t.arg({ type: NodeTransactionFilterInput }),
      },
      description:
        "The node's current validation (the transaction is in its mempool).",
      resolve: (p, _a, _c, info) => many(p, info),
      type: [NodeTransactionRef],
    }),
    output_count: intField(t, 'output_count'),
    output_value_satoshis: bigIntField(t, 'output_value_satoshis'),
    outputs: t.field({
      args: outputListArgs(t),
      resolve: (p, _a, _c, info) => many(p, info),
      type: [OutputRef],
    }),
    signing_output: t.field({
      args: { where: t.arg({ type: OutputFilterInput }) },
      description: 'Output 1 (null if there is none).',
      nullable: true,
      resolve: (p, _a, _c, info) => one(p, info),
      type: OutputRef,
    }),
    size_bytes: intField(t, 'size_bytes'),
    version: intField(t, 'version'),
  }),
});

/* ---- Output / Input ---- */

OutputRef.implement({
  fields: (t) => ({
    fungible_token_amount: t.field({
      nullable: true,
      resolve: (p) => str(p.fungible_token_amount),
      type: 'BigInt',
    }),
    locking_bytecode: hexField(t, 'locking_bytecode'),
    locking_bytecode_pattern: t.field({
      description:
        'The first byte of each instruction of the locking bytecode (pushed data excluded).',
      resolve: (p) => parseBytecodePattern(String(p.locking_bytecode)),
      type: 'Hex',
    }),
    nonfungible_token_capability: t.field({
      nullable: true,
      resolve: (p) => capability(p.nonfungible_token_capability),
      type: CapabilityEnum,
    }),
    nonfungible_token_commitment: nullableHex(
      t,
      'nonfungible_token_commitment'
    ),
    output_index: intField(t, 'output_index'),
    spent_by: t.field({
      args: {
        accepted_by: acceptedByArg(t),
        limit: t.arg.int(),
        order_by: t.arg({ type: [InputOrderBy] }),
        where: t.arg({ type: InputFilterInput }),
      },
      description:
        'Every stored input spending this output (node-agnostic: conflicting spends included). `accepted_by: node` keeps the inputs of transactions that node accepts (at most one unless its mempool view holds a double spend); `accepted(node:)` on each input tells them apart.',
      resolve: (p, _a, _c, info) => many(p, info),
      type: [InputRef],
    }),
    token_category: nullableHex(t, 'token_category'),
    transaction: t.field({
      description: 'The creating transaction.',
      resolve: (p, _a, _c, info) => one(p, info)!,
      type: TransactionRef,
    }),
    transaction_hash: hexField(t, 'transaction_hash'),
    transaction_internal_id: bigIntField(t, 'transaction_internal_id'),
    value_satoshis: bigIntField(t, 'value_satoshis'),
  }),
});

InputRef.implement({
  fields: (t) => ({
    accepted: acceptedField(
      t,
      'Whether the node accepts the spending transaction (in a block it accepts or in its mempool).'
    ),
    input_index: intField(t, 'input_index'),
    outpoint: t.field({
      description:
        'The output this input spends (null when it is not stored, e.g. for a coinbase input).',
      nullable: true,
      resolve: (p, _a, _c, info) => one(p, info),
      type: OutputRef,
    }),
    outpoint_index: bigIntField(
      t,
      'outpoint_index',
      'BigInt: a coinbase input has 4294967295.'
    ),
    outpoint_transaction_hash: hexField(t, 'outpoint_transaction_hash'),
    redeem_bytecode_pattern: t.field({
      description:
        'If the last instruction of the unlocking bytecode is a push, the pattern of its contents (P2SH redeem bytecode); else null.',
      nullable: true,
      resolve: (p) => parseBytecodePatternRedeem(String(p.unlocking_bytecode)),
      type: 'Hex',
    }),
    sequence_number: bigIntField(t, 'sequence_number'),
    transaction: t.field({
      description: 'The spending transaction.',
      resolve: (p, _a, _c, info) => one(p, info)!,
      type: TransactionRef,
    }),
    transaction_hash: hexField(t, 'transaction_hash'),
    transaction_internal_id: bigIntField(t, 'transaction_internal_id'),
    unlocking_bytecode: hexField(t, 'unlocking_bytecode'),
    unlocking_bytecode_pattern: t.field({
      resolve: (p) => parseBytecodePattern(String(p.unlocking_bytecode)),
      type: 'Hex',
    }),
    value_satoshis: t.field({
      description:
        'The value of the spent output (null when it is not stored, e.g. for a coinbase input).',
      nullable: true,
      resolve: (p) => str(hidden(p, '__outpoint_value')?.value_satoshis),
      type: 'BigInt',
    }),
  }),
});

/* ---- Block ---- */

const headerOf = (p: Row) => ({
  bits: String(p.bits),
  merkle_root: String(p.merkle_root),
  nonce: String(p.nonce),
  previous_block_hash: String(p.previous_block_hash),
  timestamp: String(p.timestamp),
  version: String(p.version),
});
const blockValues = (p: Row) => {
  const values = hidden(p, '__block_values');
  return {
    input: BigInt(String(values?.input_value ?? '0')),
    noncoinbaseOutput: BigInt(String(values?.noncoinbase_output_value ?? '0')),
  };
};

BlockRef.implement({
  fields: (t) => ({
    accepted_by: t.field({
      args: { node: nodeArg(t), where: t.arg({ type: NodeBlockFilterInput }) },
      description:
        "The node's acceptance of this block (per node; empty if it does not accept it).",
      resolve: (p, _a, _c, info) => many(p, info),
      type: [NodeBlockRef],
    }),
    bits: bigIntField(t, 'bits'),
    encoded_hex: t.field({
      description: 'The block in the P2P network format.',
      resolve: (p) =>
        encodeBlock(
          headerOf(p),
          (hidden(p, '__enc_transactions') as unknown as Row[]).map((entry) =>
            encodedTransaction(hidden(entry, 'transaction')!)
          )
        ),
      type: 'Hex',
    }),
    fee_satoshis: t.field({
      description: 'The fees of the transactions in the block.',
      resolve: (p) => {
        const values = blockValues(p);
        return String(values.input - values.noncoinbaseOutput);
      },
      type: 'BigInt',
    }),
    generated_value_satoshis: t.field({
      description: 'Output value minus input value of the block.',
      resolve: (p) =>
        String(BigInt(String(p.output_value_satoshis)) - blockValues(p).input),
      type: 'BigInt',
    }),
    hash: hexField(t, 'hash'),
    header: t.field({
      description: 'The 80-byte block header.',
      resolve: (p) => encodeBlockHeader(headerOf(p)),
      type: 'Hex',
    }),
    height: intField(t, 'height'),
    input_count: t.field({
      resolve: (p) => String(hidden(p, '__block_counts')?.input_count ?? '0'),
      type: 'BigInt',
    }),
    input_value_satoshis: t.field({
      resolve: (p) => String(blockValues(p).input),
      type: 'BigInt',
    }),
    internal_id: bigIntField(t, 'internal_id'),
    merkle_root: hexField(t, 'merkle_root'),
    nonce: bigIntField(t, 'nonce'),
    output_count: t.field({
      resolve: (p) => String(hidden(p, '__block_counts')?.output_count ?? '0'),
      type: 'BigInt',
    }),
    output_value_satoshis: bigIntField(t, 'output_value_satoshis'),
    previous_block_hash: hexField(t, 'previous_block_hash'),
    size_bytes: intField(t, 'size_bytes'),
    timestamp: bigIntField(t, 'timestamp'),
    transaction_count: intField(t, 'transaction_count'),
    transactions: t.field({
      args: {
        limit: t.arg.int(),
        offset: t.arg.int(),
        order_by: t.arg({ type: [BlockTransactionOrderBy] }),
        where: t.arg({ type: BlockTransactionFilterInput }),
      },
      description: 'The transactions of the block, by transaction_index.',
      resolve: (p, _a, _c, info) => many(p, info),
      type: [BlockTransactionRef],
    }),
    version: intField(t, 'version'),
  }),
});

BlockInclusionRef.implement({
  description:
    'A stored block that includes the transaction (a structural fact; acceptance is per node: `accepted(node:)`, or `block { accepted_by(node:) { accepted_at } }`).',
  fields: (t) => ({
    accepted: acceptedField(t, 'Whether the node accepts the block.'),
    block: t.field({
      resolve: (p, _a, _c, info) => one(p, info)!,
      type: BlockRef,
    }),
    block_internal_id: bigIntField(t, 'block_internal_id'),
    transaction_index: t.int({
      nullable: true,
      resolve: (p) => num(p.transaction_index),
    }),
  }),
});

BlockTransactionRef.implement({
  fields: (t) => ({
    block: t.field({
      resolve: (p, _a, _c, info) => one(p, info)!,
      type: BlockRef,
    }),
    transaction: t.field({
      resolve: (p, _a, _c, info) => one(p, info)!,
      type: TransactionRef,
    }),
    transaction_hash: hexField(t, 'transaction_hash'),
    transaction_index: intField(t, 'transaction_index'),
    transaction_internal_id: bigIntField(t, 'transaction_internal_id'),
  }),
});

/* ---- per-node facts ---- */

NodeTransactionRef.implement({
  description:
    "A node's validation of a transaction (the transaction is in its mempool).",
  fields: (t) => ({
    node: t.field({
      resolve: (p, _a, _c, info) => one(p, info)!,
      type: NodeRef,
    }),
    node_internal_id: intField(t, 'node_internal_id'),
    transaction: t.field({
      resolve: (p, _a, _c, info) => one(p, info)!,
      type: TransactionRef,
    }),
    transaction_hash: hexField(t, 'transaction_hash'),
    transaction_internal_id: bigIntField(t, 'transaction_internal_id'),
    validated_at: timestampField(t, 'validated_at'),
  }),
});

TimelineRef.implement({
  description:
    "A past validation of a transaction by a node: replaced_at is when it left the node's mempool (null: confirmed).",
  fields: (t) => ({
    node: t.field({
      resolve: (p, _a, _c, info) => one(p, info)!,
      type: NodeRef,
    }),
    replaced_at: timestampField(t, 'replaced_at'),
    transaction_internal_id: bigIntField(t, 'transaction_internal_id'),
    validated_at: timestampField(t, 'validated_at'),
  }),
});

NodeBlockRef.implement({
  description: "A node's acceptance of a block.",
  fields: (t) => ({
    accepted_at: timestampField(
      t,
      'accepted_at',
      'Null if the block is known only from a historical sync.'
    ),
    block: t.field({
      resolve: (p, _a, _c, info) => one(p, info)!,
      type: BlockRef,
    }),
    block_hash: hexField(t, 'block_hash'),
    block_internal_id: bigIntField(t, 'block_internal_id'),
    height: intField(t, 'height'),
    node: t.field({
      resolve: (p, _a, _c, info) => one(p, info)!,
      type: NodeRef,
    }),
    node_internal_id: intField(t, 'node_internal_id'),
  }),
});

NodeRef.implement({
  description:
    'A node Chaingraph follows. Its mempool and accepted blocks are per node: select them under `node(node:)` (or a relationship naming the node).',
  fields: (t) => ({
    accepted_blocks: t.field({
      args: {
        limit: t.arg.int(),
        offset: t.arg.int(),
        order_by: t.arg({ type: [NodeBlockOrderBy] }),
        where: t.arg({ type: NodeBlockFilterInput }),
      },
      resolve: (p, _a, _c, info) => many(p, info),
      type: [NodeBlockRef],
    }),
    first_connected_at: timestampField(t, 'first_connected_at'),
    latest_connection_began_at: timestampField(t, 'latest_connection_began_at'),
    name: t.string({ resolve: (p) => String(p.name) }),
    protocol_version: intField(t, 'protocol_version'),
    unconfirmed_transaction_count: t.field({
      resolve: (p) => String(hidden(p, '__mempool_count')?.count ?? '0'),
      type: 'BigInt',
    }),
    unconfirmed_transactions: t.field({
      args: {
        limit: t.arg.int(),
        offset: t.arg.int(),
        order_by: t.arg({ type: [NodeTransactionOrderBy] }),
        where: t.arg({ type: NodeTransactionFilterInput }),
      },
      resolve: (p, _a, _c, info) => many(p, info),
      type: [NodeTransactionRef],
    }),
    user_agent: t.string({ resolve: (p) => String(p.user_agent) }),
  }),
});

/* ---- authchains ---- */

AuthchainRef.implement({
  fields: (t) => ({
    authchain_length: intField(
      t,
      'authchain_length',
      'Transactions in the chain, the base included.'
    ),
    authhead: t.field({
      resolve: (p, _a, _c, info) => one(p, info)!,
      type: TransactionRef,
    }),
    authhead_transaction_hash: hexField(t, 'authhead_transaction_hash'),
    migrations: t.field({
      args: {
        limit: t.arg.int(),
        offset: t.arg.int(),
        order_by: t.arg({ type: [MigrationOrderBy] }),
      },
      description: 'The chain, base first (migration_index 0).',
      resolve: (p, _a, _c, info) => many(p, info),
      type: [MigrationRef],
    }),
    unspent_authhead: t.boolean({
      description:
        'False only if the chain exceeded the depth limit (100,000).',
      resolve: (p) => p.unspent_authhead === true || p.unspent_authhead === 1,
    }),
  }),
});

MigrationRef.implement({
  fields: (t) => ({
    migration_index: intField(t, 'migration_index'),
    transaction: t.field({
      resolve: (p, _a, _c, info) => one(p, info)!,
      type: TransactionRef,
    }),
    transaction_hash: hexField(t, 'transaction_hash'),
  }),
});

/* ---- pages ---- */

const pageType = (name: string, item: typeof TransactionRef) =>
  builder.objectRef<Page>(name).implement({
    description:
      'One page; `has_more` signals truncation (R33). Pass `end_cursor` as `after` (transactions: `after`/`before` for the default order, `cursor` for an `order_by` page) for the next page.',
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
const BlockPageRef = pageType('BlockPage', BlockRef);
const NodeTransactionPageRef = pageType(
  'NodeTransactionPage',
  NodeTransactionRef
);

interface WatermarkRow {
  node: string;
  visible: string;
  visible0: string;
}
const WatermarkRef = builder.objectRef<WatermarkRow>('Watermark').implement({
  description:
    "The visibility watermark the request reads at (R32): it moves when the node's view changes (block, re-org, mempool).",
  fields: (t) => ({
    node: t.string({ resolve: (p) => p.node }),
    visible: t.field({
      description:
        "The node's watermark (a commit sequence; compare for equality / order only).",
      resolve: (p) => p.visible,
      type: 'BigInt',
    }),
    visible0: t.field({
      description: 'The node-agnostic watermark.',
      resolve: (p) => p.visible0,
      type: 'BigInt',
    }),
  }),
});

/* ------------------------------------------------------------------ */
/* aggregates (R24) and group-by (R25)                                  */
/* ------------------------------------------------------------------ */

interface NumericParent {
  row: AggregateRow;
  fn: 'max' | 'min' | 'sum';
}

const keyType = (kind: KeyKind) =>
  kind === 'hex'
    ? 'Hex'
    : kind === 'int'
    ? 'Int'
    : kind === 'bool'
    ? 'Boolean'
    : 'String';

/** The aggregate, group and group-page types of one entity (aggregates.ts spec). */
const aggregateTypes = (spec: AggregateSpec, keyEnumName?: string) => {
  const fields = Object.keys(spec.numeric);
  const Numeric = builder
    .objectRef<NumericParent>(`${spec.name}AggregateNumeric`)
    .implement({
      fields: (t) =>
        Object.fromEntries(
          fields.map((field) => [
            field,
            t.field({
              nullable: true,
              resolve: (p) => p.row.values[`${p.fn}.${field}`] as string | null,
              type: 'BigInt',
            }),
          ])
        ),
    });
  const Avg = builder
    .objectRef<AggregateRow>(`${spec.name}AggregateAvg`)
    .implement({
      fields: (t) =>
        Object.fromEntries(
          fields.map((field) => [
            field,
            t.float({
              nullable: true,
              resolve: (p) => p.values[`avg.${field}`] as number | null,
            }),
          ])
        ),
    });
  const Fields = builder
    .objectRef<AggregateRow>(`${spec.name}AggregateFields`)
    .implement({
      description:
        'count, and sum/min/max/avg of the numeric fields (null over no rows, as in SQL).',
      fields: (t) => ({
        avg: t.field({ resolve: (p) => p, type: Avg }),
        count: t.field({ resolve: (p) => p.count, type: 'BigInt' }),
        max: t.field({
          resolve: (row): NumericParent => ({ fn: 'max', row }),
          type: Numeric,
        }),
        min: t.field({
          resolve: (row): NumericParent => ({ fn: 'min', row }),
          type: Numeric,
        }),
        sum: t.field({
          resolve: (row): NumericParent => ({ fn: 'sum', row }),
          type: Numeric,
        }),
      }),
    });
  const Aggregate = builder
    .objectRef<AggregateRow>(`${spec.name}Aggregate`)
    .implement({
      fields: (t) => ({
        aggregate: t.field({ resolve: (p) => p, type: Fields }),
      }),
    });
  const keys = Object.entries(spec.keys);
  if (keys.length === 0) return { Aggregate, Fields };
  const KeyEnum = builder.enumType(keyEnumName ?? `${spec.name}GroupColumn`, {
    values: keys.map(([key]) => key) as [string, ...string[]],
  });
  const OrderField = builder.enumType(`${spec.name}GroupOrderField`, {
    values: [
      'COUNT',
      'KEY',
      ...fields.flatMap((field) =>
        ['SUM', 'MIN', 'MAX', 'AVG'].map((fn) => `${fn}_${field.toUpperCase()}`)
      ),
    ] as [string, ...string[]],
  });
  const Order = builder
    .inputRef<GroupOrder>(`${spec.name}GroupOrder`)
    .implement({
      fields: (t) => ({
        by: t.field({ required: true, type: OrderField }),
        direction: t.field({ defaultValue: 'DESC', type: OrderEnum }),
      }),
    });
  const Key = builder
    .objectRef<AggregateRow>(`${spec.name}GroupKey`)
    .implement({
      description: 'The group key (only the group_by columns are set).',
      fields: (t) =>
        Object.fromEntries(
          keys.map(([, key]) => [
            key.field,
            t.field({
              nullable: true,
              resolve: ((p: AggregateRow) => {
                const value = p.key[key.field];
                if (value === null || value === undefined) return null;
                if (key.kind === 'int') return Number(value);
                if (key.kind === 'bool') return value === true || value === 1;
                return String(value);
              }) as never,
              type: keyType(key.kind) as never,
            }),
          ])
        ),
    });
  const Group = builder.objectRef<AggregateRow>(`${spec.name}Group`).implement({
    fields: (t) => ({
      aggregate: t.field({ resolve: (p) => p, type: Fields }),
      key: t.field({ resolve: (p) => p, type: Key }),
    }),
  });
  const GroupPage = builder
    .objectRef<{ nodes: AggregateRow[]; has_more: boolean }>(
      `${spec.name}GroupPage`
    )
    .implement({
      description: 'One page of groups; `has_more` signals more groups (R33).',
      fields: (t) => ({
        has_more: t.boolean({ resolve: (p) => p.has_more }),
        nodes: t.field({ resolve: (p) => p.nodes, type: [Group] }),
      }),
    });
  return { Aggregate, Fields, Group, GroupPage, KeyEnum, Order };
};

const OutputAggregates = aggregateTypes(
  aggregateSpecs.Output,
  'OutputGroupColumn'
);
const SpendAggregates = aggregateTypes(aggregateSpecs.Spend);
const TransactionAggregates = aggregateTypes(aggregateSpecs.Transaction);
const BlockAggregates = aggregateTypes(aggregateSpecs.Block);
const MempoolAggregates = aggregateTypes(aggregateSpecs.Mempool);

/** The aggregates requested under `aggregate` (aggregate roots) or `nodes { aggregate }` (groups). */
const aggregateSelection = (sels: readonly Sel[]) =>
  requestedAggregates(
    sels
      .filter((sel) => sel.name === 'aggregate')
      .flatMap((sel) => sel.sels ?? [])
  );

/* ------------------------------------------------------------------ */
/* roots                                                                */
/* ------------------------------------------------------------------ */

const requestEnv = async (
  context: ApiContext,
  node?: string
): Promise<RootEnv> => {
  if (context.snapshot === undefined) {
    throw new GraphQLError('No request snapshot (server misconfigured).');
  }
  const env = await context.snapshot.statement(node);
  return { ...env, db: context.db };
};

interface Args {
  [name: string]: unknown;
}

/** A per-node root: run on the request snapshot (query) or the hub's (subscription). */
interface PerNodeRoot {
  run: (
    env: RootEnv,
    label: string,
    args: Args,
    info: GraphQLResolveInfo
  ) => Promise<unknown>;
}

const perNodeRoots: { [name: string]: PerNodeRoot } = {
  block_tip: {
    run: async (env, label, _args, info) =>
      runBlockTip(env, label, rootSels(info)),
  },
  blocks: {
    run: async (env, label, args, info) =>
      runBlocks(env, label, args as unknown as BlocksArgs, pageNodeSels(info)),
  },
  inputs: {
    run: async (env, label, args, info) =>
      runInputs(env, label, args as unknown as InputsArgs, pageNodeSels(info)),
  },
  mempool: {
    run: async (env, label, args, info) =>
      runMempool(
        env,
        label,
        args as unknown as MempoolArgs,
        pageNodeSels(info)
      ),
  },
  node: {
    run: async (env, label, _args, info) => runNode(env, label, rootSels(info)),
  },
  outputs: {
    run: async (env, label, args, info) =>
      runOutputs(
        env,
        label,
        args as unknown as OutputsArgs,
        pageNodeSels(info)
      ),
  },
  transactions: {
    run: async (env, label, args, info) =>
      runTransactions(
        env,
        label,
        {
          ...(args as unknown as TransactionsArgs),
          after_internal_id: args.after as bigint | null | undefined,
        },
        pageNodeSels(info)
      ),
  },
  unspent_outputs: {
    run: async (env, label, args, info) =>
      runUnspentOutputs(
        env,
        label,
        args as unknown as OutputsArgs,
        pageNodeSels(info)
      ),
  },
};

const pageArgs = (
  t: PothosSchemaTypes.QueryFieldBuilder<
    PothosSchemaTypes.ExtendDefaultTypes<Types>,
    unknown
  >
) => ({
  after: t.arg.string({
    description: 'The end_cursor of the previous page (keyset).',
  }),
  limit: t.arg.int({ required: true }),
  node: t.arg.string({ required: true }),
  offset: t.arg.int({
    description:
      'Rows to skip (offset pagination: discouraged, capped at 100,000; prefer `after`).',
  }),
});

type RootFieldBuilder = PothosSchemaTypes.QueryFieldBuilder<
  PothosSchemaTypes.ExtendDefaultTypes<Types>,
  unknown
>;

/** Root field definitions shared by Query and Subscription (arguments, type, description). */
const rootFields = (t: RootFieldBuilder) => ({
  block_tip: {
    args: { node: t.arg.string({ required: true }) },
    description:
      "The node's tip: its accepted block with the greatest height (with previous_block_hash for re-org detection).",
    nullable: true,
    type: BlockRef,
  },
  blocks: {
    args: {
      ...pageArgs(t),
      order_by: t.arg({ type: [BlockOrderBy] }),
      where: t.arg({ type: BlockFilterInput }),
    },
    description:
      'Blocks the node accepts (default order: height). Tip: `order_by: { height: DESC }, limit: 1`; by height: `where: { height: { _eq } }`; range: `_gte`/`_lt`; checked heights: `_in`.',
    type: BlockPageRef,
  },
  inputs: {
    args: {
      ...pageArgs(t),
      order_by: t.arg({ type: [InputOrderBy] }),
      where: t.arg({ type: InputFilterInput }),
    },
    description:
      'Inputs of transactions the node accepts (default keyset order: transaction_internal_id, input_index).',
    type: InputPageRef,
  },
  mempool: {
    args: {
      ...pageArgs(t),
      order_by: t.arg({ type: [NodeTransactionOrderBy] }),
      where: t.arg({ type: NodeTransactionFilterInput }),
    },
    description:
      "The node's mempool (node_transaction), e.g. `order_by: [{ validated_at: DESC }, { transaction_internal_id: DESC }]`.",
    type: NodeTransactionPageRef,
  },
  node: {
    args: { node: t.arg.string({ required: true }) },
    description: 'One node: metadata, mempool, accepted blocks.',
    nullable: true,
    type: NodeRef,
  },
  outputs: {
    args: {
      ...pageArgs(t),
      order_by: t.arg({ type: [OutputOrderBy] }),
      where: t.arg({ type: OutputFilterInput }),
    },
    description:
      'Outputs of transactions the node accepts (search by locking bytecode set or prefix, category, commitment prefix …; default keyset order: transaction_internal_id, output_index).',
    type: OutputPageRef,
  },
  transactions: {
    args: {
      after: t.arg({
        description:
          'internal_id lower bound (exclusive); the default order’s end_cursor.',
        type: 'BigInt',
      }),
      before: t.arg({
        description: 'internal_id upper bound (exclusive).',
        type: 'BigInt',
      }),
      cursor: t.arg.string({
        description: 'The end_cursor of the previous `order_by` page.',
      }),
      limit: t.arg.int({ required: true }),
      node: t.arg.string({ required: true }),
      offset: t.arg.int({
        description: 'Rows to skip (discouraged; capped at 100,000).',
      }),
      order: t.arg({ defaultValue: 'ASC', type: OrderEnum }),
      order_by: t.arg({ type: [TransactionOrderBy] }),
      where: t.arg({ type: TransactionFilterInput }),
    },
    description:
      'Transactions the node accepts (in a block it accepts or in its mempool), keyset on internal_id: after < internal_id < before (or any `order_by` with `cursor`).',
    type: TransactionPageRef,
  },
  unspent_outputs: {
    args: {
      ...pageArgs(t),
      order_by: t.arg({ type: [OutputOrderBy] }),
      where: t.arg({ type: OutputFilterInput }),
    },
    description:
      "The node's unspent outputs, computed at query time from outputs minus spending inputs (UTXO tables off), keyset on (transaction_internal_id, output_index).",
    type: OutputPageRef,
  },
});

builder.queryType({
  fields: (t) => {
    const defs = rootFields(t);
    const perNode = Object.fromEntries(
      Object.entries(defs).map(([name, def]) => [
        name,
        t.field({
          ...(def as object),
          resolve: async (
            _root: unknown,
            args: Args,
            context: ApiContext,
            info: GraphQLResolveInfo
          ) =>
            perNodeRoots[name]!.run(
              await requestEnv(context, args.node as string),
              name,
              args,
              info
            ),
        } as never),
      ])
    );
    return {
      ...perNode,
      blocks_by_hash: t.field({
        args: { hashes: t.arg({ required: true, type: ['Hex'] }) },
        description:
          'Blocks by hash (node-agnostic: per-node relationships below need `node:`). Up to 10,000 hashes.',
        resolve: async (_root, args, context, info) =>
          runBlocksByHash(
            await requestEnv(context),
            'blocks_by_hash',
            args.hashes,
            rootSels(info)
          ),
        type: [BlockRef],
      }),
      nodes: t.field({
        description:
          "Every node's metadata (node-agnostic; mempool and accepted blocks: `node(node:)`).",
        resolve: async (_root, _args, context, info) =>
          runNodes(await requestEnv(context), 'nodes', rootSels(info)),
        type: [NodeRef],
      }),
      outputs_by_transaction_hash: t.field({
        args: {
          transaction_hashes: t.arg({ required: true, type: ['Hex'] }),
          where: t.arg({ type: OutputFilterInput }),
        },
        description:
          'Outputs of transactions by hash (node-agnostic: no acceptance; per-node relationships below need `node:`). Up to 10,000 hashes.',
        resolve: async (_root, args, context, info) =>
          runOutputsByTransactionHash(
            await requestEnv(context),
            'outputs_by_transaction_hash',
            args.transaction_hashes,
            args.where,
            rootSels(info)
          ),
        type: [OutputRef],
      }),
      transaction_watermark: t.field({
        description:
          'The newest transaction internal_id in the request snapshot (node-agnostic).',
        nullable: true,
        resolve: async (_root, _args, context) =>
          runTransactionWatermark(
            await requestEnv(context),
            'transaction_watermark'
          ),
        type: 'BigInt',
      }),
      transactions_by_hash: t.field({
        args: { hashes: t.arg({ required: true, type: ['Hex'] }) },
        description:
          'Transactions by hash (node-agnostic: no acceptance; per-node relationships below need `node:`). Up to 10,000 hashes.',
        resolve: async (_root, args, context, info) =>
          runTransactionsByHash(
            await requestEnv(context),
            'transactions_by_hash',
            args.hashes,
            rootSels(info)
          ),
        type: [TransactionRef],
      }),
      blocks_aggregate: t.field({
        args: {
          node: t.arg.string({ required: true }),
          where: t.arg({ type: BlockFilterInput }),
        },
        description: 'Aggregates over the blocks the node accepts (R24).',
        resolve: async (_root, args, context, info) => {
          const env = await requestEnv(context, args.node);
          return runAggregate(
            env,
            'blocks_aggregate',
            aggregateSpecs.Block,
            rowSets.blocks(env, args.where),
            aggregateSelection(rootSels(info))
          );
        },
        type: BlockAggregates.Aggregate,
      }),
      block_groups: t.field({
        args: {
          group_by: t.arg({ required: true, type: [BlockAggregates.KeyEnum!] }),
          limit: t.arg.int({ required: true }),
          node: t.arg.string({ required: true }),
          offset: t.arg.int(),
          order_by: t.arg({ type: [BlockAggregates.Order!] }),
          where: t.arg({ type: BlockFilterInput }),
        },
        description:
          'Group-by over the blocks the node accepts (e.g. block statistics per DAY / MONTH, R25).',
        resolve: async (_root, args, context, info) => {
          const env = await requestEnv(context, args.node);
          return runGroups(
            env,
            'block_groups',
            aggregateSpecs.Block,
            rowSets.blocks(env, args.where),
            args,
            aggregateSelection(pageNodeSels(info))
          );
        },
        type: BlockAggregates.GroupPage!,
      }),
      mempool_aggregate: t.field({
        args: {
          node: t.arg.string({ required: true }),
          where: t.arg({ type: NodeTransactionFilterInput }),
        },
        description: "Aggregates over the node's mempool.",
        resolve: async (_root, args, context, info) => {
          const env = await requestEnv(context, args.node);
          return runAggregate(
            env,
            'mempool_aggregate',
            aggregateSpecs.Mempool,
            rowSets.mempool(env, args.where),
            aggregateSelection(rootSels(info))
          );
        },
        type: MempoolAggregates.Aggregate,
      }),
      output_groups: t.field({
        args: {
          group_by: t.arg({
            required: true,
            type: [OutputAggregates.KeyEnum!],
          }),
          limit: t.arg.int({ required: true }),
          node: t.arg.string({ required: true }),
          offset: t.arg.int(),
          order_by: t.arg({ type: [OutputAggregates.Order!] }),
          unspent: t.arg.boolean({
            defaultValue: false,
            description: "Only the node's unspent outputs.",
          }),
          where: t.arg({ type: OutputFilterInput }),
        },
        description:
          'Group-by over the outputs the node accepts (or its unspent outputs): holder balances per locking bytecode, supply per category, … (R25).',
        resolve: async (_root, args, context, info) => {
          const env = await requestEnv(context, args.node);
          return runGroups(
            env,
            'output_groups',
            aggregateSpecs.Output,
            args.unspent === true
              ? rowSets.unspentOutputs(env, args.where)
              : rowSets.outputs(env, args.where),
            args,
            aggregateSelection(pageNodeSels(info))
          );
        },
        type: OutputAggregates.GroupPage!,
      }),
      outputs_aggregate: t.field({
        args: {
          node: t.arg.string({ required: true }),
          where: t.arg({ type: OutputFilterInput }),
        },
        description:
          'Aggregates over the outputs the node accepts (search roots: count of a locking bytecode prefix, …).',
        resolve: async (_root, args, context, info) => {
          const env = await requestEnv(context, args.node);
          return runAggregate(
            env,
            'outputs_aggregate',
            aggregateSpecs.Output,
            rowSets.outputs(env, args.where),
            aggregateSelection(rootSels(info))
          );
        },
        type: OutputAggregates.Aggregate,
      }),
      spend_groups: t.field({
        args: {
          group_by: t.arg({ required: true, type: [SpendAggregates.KeyEnum!] }),
          limit: t.arg.int({ required: true }),
          node: t.arg.string({ required: true }),
          offset: t.arg.int(),
          order_by: t.arg({ type: [SpendAggregates.Order!] }),
          where: t.arg({ type: InputFilterInput }),
        },
        description:
          'Group-by over spends (inputs of transactions the node accepts, with the outputs they spend; coinbase inputs are not spends): e.g. value paid per spent token category or per transaction.',
        resolve: async (_root, args, context, info) => {
          const env = await requestEnv(context, args.node);
          return runGroups(
            env,
            'spend_groups',
            aggregateSpecs.Spend,
            rowSets.spends(env, args.where),
            args,
            aggregateSelection(pageNodeSels(info))
          );
        },
        type: SpendAggregates.GroupPage!,
      }),
      spends_aggregate: t.field({
        args: {
          node: t.arg.string({ required: true }),
          where: t.arg({ type: InputFilterInput }),
        },
        description:
          'Aggregates over spends (inputs of transactions the node accepts, with the spent outputs).',
        resolve: async (_root, args, context, info) => {
          const env = await requestEnv(context, args.node);
          return runAggregate(
            env,
            'spends_aggregate',
            aggregateSpecs.Spend,
            rowSets.spends(env, args.where),
            aggregateSelection(rootSels(info))
          );
        },
        type: SpendAggregates.Aggregate,
      }),
      transaction_groups: t.field({
        args: {
          group_by: t.arg({
            required: true,
            type: [TransactionAggregates.KeyEnum!],
          }),
          limit: t.arg.int({ required: true }),
          node: t.arg.string({ required: true }),
          offset: t.arg.int(),
          order_by: t.arg({ type: [TransactionAggregates.Order!] }),
          where: t.arg({ type: TransactionFilterInput }),
        },
        description:
          'Group-by over the transactions the node accepts (e.g. per BLOCK_DAY or BLOCK_HEIGHT of the block the node accepts them in; null in its mempool).',
        resolve: async (_root, args, context, info) => {
          const env = await requestEnv(context, args.node);
          return runGroups(
            env,
            'transaction_groups',
            aggregateSpecs.Transaction,
            rowSets.transactions(env, args.where),
            args,
            aggregateSelection(pageNodeSels(info))
          );
        },
        type: TransactionAggregates.GroupPage!,
      }),
      transactions_aggregate: t.field({
        args: {
          node: t.arg.string({ required: true }),
          where: t.arg({ type: TransactionFilterInput }),
        },
        description: 'Aggregates over the transactions the node accepts (R24).',
        resolve: async (_root, args, context, info) => {
          const env = await requestEnv(context, args.node);
          return runAggregate(
            env,
            'transactions_aggregate',
            aggregateSpecs.Transaction,
            rowSets.transactions(env, args.where),
            aggregateSelection(rootSels(info))
          );
        },
        type: TransactionAggregates.Aggregate,
      }),
      unspent_output_groups: t.field({
        args: {
          group_by: t.arg({
            required: true,
            type: [OutputAggregates.KeyEnum!],
          }),
          limit: t.arg.int({ required: true }),
          node: t.arg.string({ required: true }),
          order_by: t.arg({ type: [OutputAggregates.Order!] }),
          where: t.arg({ type: OutputFilterInput }),
        },
        description:
          "Group-by over the node's unspent outputs (the spike's S3 root; output_groups(unspent: true) pages).",
        resolve: async (_root, args, context, info) => {
          const env = await requestEnv(context, args.node);
          return (
            await runGroups(
              env,
              'unspent_output_groups',
              aggregateSpecs.Output,
              rowSets.unspentOutputs(env, args.where),
              args,
              aggregateSelection(rootSels(info))
            )
          ).nodes;
        },
        type: [OutputAggregates.Group!],
      }),
      unspent_outputs_aggregate: t.field({
        args: {
          node: t.arg.string({ required: true }),
          where: t.arg({ type: OutputFilterInput }),
        },
        description: "Aggregates over the node's unspent outputs.",
        resolve: async (_root, args, context, info) => {
          const env = await requestEnv(context, args.node);
          return runAggregate(
            env,
            'unspent_outputs_aggregate',
            aggregateSpecs.Output,
            rowSets.unspentOutputs(env, args.where),
            aggregateSelection(rootSels(info))
          );
        },
        type: OutputAggregates.Aggregate,
      }),
      watermark: t.field({
        args: { node: t.arg.string({ required: true }) },
        description:
          'The visibility watermark the request reads for the node (R32; equal watermarks = equal results).',
        resolve: async (_root, args, context) =>
          runWatermark(await requestEnv(context, args.node), args.node),
        type: WatermarkRef,
      }),
    };
  },
});

interface SendResult {
  transaction_hash: string;
  validation_success: boolean;
  validation_error_message: string | null;
  transmission_success: boolean;
  transmission_error_message: string | null;
}

const SendTransactionResultRef = builder
  .objectRef<SendResult>('SendTransactionResult')
  .implement({
    fields: (t) => ({
      transaction_hash: t.field({
        resolve: (p) => p.transaction_hash,
        type: 'Hex',
      }),
      transmission_error_message: t.string({
        nullable: true,
        resolve: (p) => p.transmission_error_message,
      }),
      transmission_success: t.boolean({
        resolve: (p) => p.transmission_success,
      }),
      validation_error_message: t.string({
        nullable: true,
        resolve: (p) => p.validation_error_message,
      }),
      validation_success: t.boolean({ resolve: (p) => p.validation_success }),
    }),
  });

builder.mutationType({
  fields: (t) => ({
    send_transaction: t.field({
      args: {
        encoded_hex: t.arg({ required: true, type: 'Hex' }),
        node: t.arg.string({ required: true }),
      },
      description:
        'Relay an encoded transaction to the named node over P2P (through the agent). R38.',
      resolve: async (_root, args, context) =>
        sendTransaction(
          context.agentUrl,
          await context.db.nodeId(args.node),
          args.encoded_hex
        ),
      type: SendTransactionResultRef,
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

/** A live re-run's statement environment: the hub's single-node snapshot. */
const liveEnv = (
  db: ApiDb,
  snapshot: VisibilitySnapshot,
  nodeName: string
): RootEnv => ({ ...singleNodeEnv(snapshot, nodeName, db.maxResultRows), db });

builder.subscriptionType({
  description:
    'Live queries: the full result, re-delivered when it changes after the node watermark advances (one snapshot and one statement per advance, shared by every subscriber of the same document and variables).',
  fields: (t) => {
    const defs = rootFields(t as unknown as RootFieldBuilder);
    return Object.fromEntries(
      Object.entries(defs).map(([name, def]) => [
        name,
        t.field({
          ...(def as object),
          resolve: (payload: unknown) => payload,
          subscribe: async (
            _root: unknown,
            args: Args,
            context: ApiContext,
            info: GraphQLResolveInfo
          ) => {
            const nodeName = args.node as string;
            const nodeId = await context.db.nodeId(nodeName);
            return context.hub.subscribe(
              liveTopicKey(info, nodeId),
              nodeId,
              async (snapshot) =>
                perNodeRoots[name]!.run(
                  liveEnv(context.db, snapshot, nodeName),
                  name,
                  args,
                  info
                )
            );
          },
        } as never),
      ])
    );
  },
});

export const schema = builder.toSchema();

/** Exported for tests: the selections of a root (compile.ts). */
export type { Sel };
