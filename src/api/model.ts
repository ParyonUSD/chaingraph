/* eslint-disable camelcase, @typescript-eslint/naming-convention, max-lines, functional/no-throw-statement, max-params, complexity */
// cspell:ignore clickhouse unhex authchain authchains authhead subquery
/**
 * The v2 entities: per GraphQL object type, the raw columns of its rows, the
 * scalar fields (SQL over those columns), the computed fields (resolved from
 * hidden selections) and the relationships (compile.ts). Every relationship
 * reads a pinned view restricted to the parent level's keys.
 *
 * Relationship edges (inventory R20):
 *  1. Transaction.outputs                       7. Transaction.node_validations (per node)
 *  2. Transaction.inputs, Input.outpoint        8. Transaction.node_validation_timeline (per node)
 *  3. Output.spent_by → Input.transaction       9. Node.unconfirmed_transactions (per node)
 *     (per node)                               10. Node.accepted_blocks (per node)
 *  4. Output.transaction, Input.transaction    11. Block.transactions → BlockTransaction.transaction
 *  5. Transaction.block_inclusions → block     12. Transaction.authchains → authhead / migrations
 *     (per node)                                   (per node)
 *  6. Block.accepted_by → node (per node)      13. Transaction.identity_output, signing_output,
 *                                                  data_carrier_outputs
 */
import type {
  Ctx,
  Entity,
  OrderTerm,
  RelationField,
  ScalarField,
  Sel,
  ValueKind,
} from './compile.js';
import type {
  BlockFilter,
  BlockInclusionFilter,
  BlockTransactionFilter,
  InputFilter,
  NodeBlockFilter,
  NodeTransactionFilter,
  OutputFilter,
} from './sql.js';
import {
  blockInclusionPredicate,
  blockPredicate,
  blockTransactionPredicate,
  inputPredicate,
  invalid,
  nodeBlockPredicate,
  nodeTransactionPredicate,
  outputPredicate,
  outputRowColumns,
  zeroCategory,
} from './sql.js';

/* ------------------------------------------------------------------ */
/* helpers                                                              */
/* ------------------------------------------------------------------ */

const hex = (column: string) => `lower(hex(${column}))`;
const str = (column: string) => `toString(${column})`;
const scalar = (sql: string): ScalarField => ({ kind: 'scalar', sql });
/** Milliseconds since the epoch as text (the Timestamp scalar formats it). */
const time = (column: string) =>
  `if(isNull(${column}), NULL, toString(toUnixTimestamp64Milli(assumeNotNull(${column}))))`;

const sel = (name: string, sels?: Sel[], key = name): Sel => ({
  args: {},
  key,
  name,
  ...(sels === undefined ? {} : { sels }),
});

const term = (
  expr: string,
  dir: 'ASC' | 'DESC' = 'ASC',
  kind: ValueKind = 'uint'
): OrderTerm => ({
  dir,
  expr,
  kind,
});

/* ------------------------------------------------------------------ */
/* ordering inputs                                                      */
/* ------------------------------------------------------------------ */

/** Order fields of one entity: name → SQL over its raw columns; nested = through an object relationship. */
export interface OrderSpec {
  fields: { [name: string]: { expr: string; kind: ValueKind } };
  nested?: { [name: string]: OrderSpec };
}

interface OrderInput {
  [name: string]: unknown;
}

const collectTerms = (
  spec: OrderSpec,
  input: OrderInput,
  into: OrderTerm[],
  path: string
) => {
  Object.entries(input).forEach(([name, value]) => {
    if (value === undefined || value === null) return;
    const field = spec.fields[name];
    if (field !== undefined) {
      into.push(term(field.expr, value as 'ASC' | 'DESC', field.kind));
      return;
    }
    const nested = spec.nested?.[name];
    if (nested !== undefined && typeof value === 'object') {
      collectTerms(nested, value as OrderInput, into, `${path}${name}.`);
      return;
    }
    throw invalid(`Cannot order by ${path}${name}.`);
  });
};

/** `order_by: [{ a: DESC }, { b: ASC }]` → terms, in list order. */
export const orderTerms = (
  spec: OrderSpec,
  list: unknown
): OrderTerm[] | undefined => {
  if (list === undefined || list === null) return undefined;
  const items = Array.isArray(list) ? list : [list];
  const terms: OrderTerm[] = [];
  items.forEach((item) => {
    collectTerms(spec, item as OrderInput, terms, '');
  });
  return terms;
};

/** Append tie-breakers (a unique key) not already in the order. */
export const withTiebreak = (
  terms: readonly OrderTerm[],
  tiebreak: readonly OrderTerm[]
) => [
  ...terms,
  ...tiebreak.filter(
    (extra) => !terms.some((existing) => existing.expr === extra.expr)
  ),
];

export const transactionOrder: OrderSpec = {
  fields: {
    hash: { expr: 'hash', kind: 'hex' },
    input_count: { expr: 'input_count', kind: 'uint' },
    internal_id: { expr: 'internal_id', kind: 'uint' },
    locktime: { expr: 'locktime', kind: 'uint' },
    output_count: { expr: 'output_count', kind: 'uint' },
    output_value_satoshis: { expr: 'output_value_satoshis', kind: 'int' },
    size_bytes: { expr: 'size_bytes', kind: 'uint' },
    version: { expr: 'version', kind: 'int' },
  },
};

/** Ordering through `transaction` uses the denormalised transaction key columns of the row. */
const throughTransaction: OrderSpec = {
  fields: {
    hash: { expr: 'transaction_hash', kind: 'hex' },
    internal_id: { expr: 'transaction_internal_id', kind: 'uint' },
  },
};

export const outputOrder: OrderSpec = {
  fields: {
    fungible_token_amount: {
      expr: 'ifNull(fungible_token_amount, 0)',
      kind: 'int',
    },
    locking_bytecode: { expr: 'locking_bytecode', kind: 'hex' },
    output_index: { expr: 'output_index', kind: 'uint' },
    token_category: { expr: 'token_category', kind: 'hex' },
    transaction_hash: { expr: 'transaction_hash', kind: 'hex' },
    transaction_internal_id: { expr: 'transaction_internal_id', kind: 'uint' },
    value_satoshis: { expr: 'value_satoshis', kind: 'int' },
  },
  nested: { transaction: throughTransaction },
};

export const inputOrder: OrderSpec = {
  fields: {
    input_index: { expr: 'input_index', kind: 'uint' },
    outpoint_index: { expr: 'outpoint_index', kind: 'uint' },
    outpoint_transaction_hash: {
      expr: 'outpoint_transaction_hash',
      kind: 'hex',
    },
    sequence_number: { expr: 'sequence_number', kind: 'uint' },
    transaction_hash: { expr: 'transaction_hash', kind: 'hex' },
    transaction_internal_id: { expr: 'transaction_internal_id', kind: 'uint' },
  },
  nested: { transaction: throughTransaction },
};

export const blockOrder: OrderSpec = {
  fields: {
    hash: { expr: 'hash', kind: 'hex' },
    height: { expr: 'height', kind: 'uint' },
    internal_id: { expr: 'internal_id', kind: 'uint' },
    size_bytes: { expr: 'size_bytes', kind: 'uint' },
    timestamp: { expr: 'timestamp', kind: 'uint' },
    transaction_count: { expr: 'transaction_count', kind: 'uint' },
  },
};

export const nodeTransactionOrder: OrderSpec = {
  fields: {
    transaction_internal_id: { expr: 'transaction_internal_id', kind: 'uint' },
    validated_at: {
      expr: "ifNull(validated_at, toDateTime64(0, 3, 'UTC'))",
      kind: 'time',
    },
  },
  nested: {
    transaction: {
      fields: {
        hash: { expr: 'transaction_hash', kind: 'hex' },
        internal_id: { expr: 'transaction_internal_id', kind: 'uint' },
      },
    },
  },
};

export const nodeBlockOrder: OrderSpec = {
  fields: {
    accepted_at: {
      expr: "ifNull(accepted_at, toDateTime64(0, 3, 'UTC'))",
      kind: 'time',
    },
    block_internal_id: { expr: 'block_internal_id', kind: 'uint' },
    height: { expr: 'height', kind: 'uint' },
  },
  nested: {
    block: {
      fields: {
        hash: { expr: 'block_hash', kind: 'hex' },
        height: { expr: 'height', kind: 'uint' },
        internal_id: { expr: 'block_internal_id', kind: 'uint' },
      },
    },
  },
};

const blockInclusionOrder: OrderSpec = {
  fields: {
    block_internal_id: { expr: 'block_internal_id', kind: 'uint' },
    transaction_index: { expr: 'transaction_index', kind: 'uint' },
  },
  nested: {
    block: {
      fields: {
        height: { expr: 'height', kind: 'uint' },
        internal_id: { expr: 'block_internal_id', kind: 'uint' },
      },
    },
  },
};

const blockTransactionOrder: OrderSpec = {
  fields: {
    transaction_index: { expr: 'transaction_index', kind: 'uint' },
    transaction_internal_id: { expr: 'transaction_internal_id', kind: 'uint' },
  },
  nested: { transaction: throughTransaction },
};

const timelineOrder: OrderSpec = {
  fields: {
    internal_id: { expr: 'internal_id', kind: 'uint' },
    replaced_at: {
      expr: "ifNull(replaced_at, toDateTime64(0, 3, 'UTC'))",
      kind: 'time',
    },
    validated_at: {
      expr: "ifNull(validated_at, toDateTime64(0, 3, 'UTC'))",
      kind: 'time',
    },
  },
};

const migrationOrder: OrderSpec = {
  fields: { migration_index: { expr: 'migration_index', kind: 'uint' } },
};

/** The spike's `order: ASC | DESC` on nested outputs/inputs, or `order_by`. */
const indexOrder =
  (spec: OrderSpec, index: string, tiebreak: readonly OrderTerm[]) =>
  (args: { [name: string]: unknown }) => {
    const explicit = orderTerms(spec, args.order_by);
    if (explicit !== undefined) return withTiebreak(explicit, tiebreak);
    return args.order === 'DESC' ? [term(index, 'DESC')] : undefined;
  };

const listOrder =
  (spec: OrderSpec, tiebreak: readonly OrderTerm[]) =>
  (args: { [name: string]: unknown }) => {
    const explicit = orderTerms(spec, args.order_by);
    return explicit === undefined
      ? undefined
      : withTiebreak(explicit, tiebreak);
  };

/* ------------------------------------------------------------------ */
/* entity columns                                                       */
/* ------------------------------------------------------------------ */

const transactionColumns = [
  'hash',
  'internal_id',
  'version',
  'locktime',
  'size_bytes',
  'is_coinbase',
  'input_count',
  'output_count',
  'output_value_satoshis',
] as const;

const outputColumns = [
  'transaction_hash',
  'output_index',
  'transaction_internal_id',
  'value_satoshis',
  'locking_bytecode',
  'token_category',
  'fungible_token_amount',
  'nonfungible_token_capability',
  'nonfungible_token_commitment',
  'locking_bytecode_prefix',
  'nonfungible_token_commitment_key',
] as const;

const inputColumns = [
  'transaction_hash',
  'input_index',
  'transaction_internal_id',
  'outpoint_transaction_hash',
  'outpoint_index',
  'sequence_number',
  'unlocking_bytecode',
] as const;

const blockColumns = [
  'hash',
  'internal_id',
  'height',
  'version',
  'timestamp',
  'previous_block_hash',
  'merkle_root',
  'bits',
  'nonce',
  'size_bytes',
  'transaction_count',
  'output_value_satoshis',
] as const;

/* ------------------------------------------------------------------ */
/* sources                                                              */
/* ------------------------------------------------------------------ */

const outputsOf = (c: Ctx, condition: string) =>
  `SELECT * FROM ${c.sql.view('output_at')} WHERE ${condition}`;

const transactionsByHash = (c: Ctx, link: string) =>
  `SELECT * FROM ${c.sql.view('transaction_at')} WHERE hash IN (${link})`;

const blocksById = (c: Ctx, link: string) =>
  `SELECT * FROM ${c.sql.view('block_at')} WHERE internal_id IN (${link})`;

const nodesById = (link: string) =>
  `SELECT internal_id, name, protocol_version, user_agent, first_connected_at, latest_connection_began_at FROM node_v WHERE internal_id IN (${link})`;

/** Spent output values of the given transactions' inputs (output join, page only). */
const inputValuesSql = (
  c: Ctx,
  transactions: string
) => `SELECT i.transaction_hash AS transaction_hash, sum(o.value_satoshis) AS input_value
  FROM (SELECT transaction_hash, outpoint_transaction_hash, outpoint_index FROM ${c.sql.view(
    'input_at'
  )} WHERE transaction_hash IN (${transactions})) AS i
  INNER JOIN (SELECT transaction_hash, output_index, value_satoshis FROM ${c.sql.view(
    'output_at'
  )} WHERE (transaction_hash, output_index) IN (SELECT outpoint_transaction_hash, outpoint_index FROM ${c.sql.view(
  'input_at'
)} WHERE transaction_hash IN (${transactions}))) AS o
    ON o.transaction_hash = i.outpoint_transaction_hash AND o.output_index = i.outpoint_index
  GROUP BY i.transaction_hash`;

/** Transactions of the given blocks. */
const blockTransactionHashes = (c: Ctx, blocks: string) =>
  `SELECT transaction_hash FROM ${c.sql.view(
    'block_transaction_at'
  )} WHERE block_internal_id IN (${blocks})`;

/** v1's maximum authchain depth (`collect_authchains`). */
export const maxAuthchainDepth = 100_000;

/**
 * Per node: from each base transaction, follow the spends of output 0 by
 * transactions the node accepts until output 0 is unspent (v1
 * `collect_authchains`, at the node's view: at most one spender per output,
 * so one chain per base). Columns: base, head, length, path (hashes, base
 * first), unspent (false only when the depth limit cut the chain).
 *
 * A walk over the authchain index's segments (ddl/080_authchain.sql), one
 * recursive CTE alternating two steps per base:
 * - phase 0, along a segment: the segment members after the current
 *   transaction that the node accepts, in position order while contiguous
 *   (one keyed read of the segment; conflicting spends leave one branch);
 * - phase 1, across a segment end: the spender of the last transaction's
 *   output 0 that the node accepts, from the input table (any commit, so
 *   transactions above the spend watermark are followed link by link);
 *   none: the chain ends (phase 2).
 * Segments come from heavy-path placement, so a chain crosses few ends
 * (chipnet: at most 6 over every chain; X6's 776-long chain crosses 2, the
 * longest, 56,904 links, none). The stage-1 walk followed one link per
 * recursion step (~9 ms each).
 */
export const authchainSql = (c: Ctx, link: string) => {
  const zero = `toFixedString(unhex(repeat('00', 32)), 32)`;
  const links = c.sql.view('authchain_link_at');
  const max = String(maxAuthchainDepth);
  /*
   * walk(base, cur, len, path, phase). Steps read `walk` (the previous
   * round's rows) and name their own columns differently, so no alias
   * shadows a column it is computed from.
   */
  /* phase 0: the current transaction's segment and position (no row: it starts its own) */
  const here = `SELECT w.base AS h_base, w.cur AS h_cur, w.len AS h_len, w.path AS h_path,
      if(l.transaction_hash = ${zero}, t.t_id, l.l_segment) AS h_seg,
      if(l.transaction_hash = ${zero}, toUInt32(0), l.l_position) AS h_pos
    FROM walk AS w
    LEFT JOIN (SELECT transaction_hash, min(segment) AS l_segment, argMin(position, segment) AS l_position
      FROM ${links} WHERE transaction_hash IN (SELECT cur FROM walk WHERE phase = 0)
      GROUP BY transaction_hash) AS l ON l.transaction_hash = w.cur
    LEFT JOIN (SELECT hash, min(internal_id) AS t_id FROM ${c.sql.view(
      'transaction_at'
    )} WHERE hash IN (SELECT cur FROM walk WHERE phase = 0) GROUP BY hash) AS t ON t.hash = w.cur
    WHERE w.phase = 0`;
  const segments = `SELECT h_seg FROM (${here})`;
  const members = `SELECT segment AS m_seg, position AS m_pos, transaction_hash AS m_hash FROM ${links}
    WHERE segment IN (${segments}) AND transaction_hash IN (${c.sql.accepted(
    c.node,
    `SELECT transaction_hash FROM ${links} WHERE segment IN (${segments})`
  )})`;
  /* the accepted members after the current position, while their positions are contiguous */
  const contiguous = `arrayFirstIndex((x, i) -> x.1 != g_pos + i, g_sorted, arrayEnumerate(g_sorted))`;
  const along = `SELECT g_base, if(empty(g_next), g_cur, g_next[-1]), g_len + length(g_next),
      arrayConcat(g_path, g_next), toUInt8(if(g_len + length(g_next) >= ${max}, 2, 1))
    FROM (SELECT g_base, g_cur, g_len, g_path,
        arraySlice(arrayMap(x -> x.2, arraySlice(g_sorted, 1,
          toInt64(if(${contiguous} = 0, length(g_sorted), ${contiguous} - 1)))), 1,
          toInt64(${max}) - toInt64(g_len)) AS g_next
      FROM (SELECT h_base AS g_base, any(h_cur) AS g_cur, any(h_len) AS g_len, any(h_path) AS g_path,
          any(h_pos) AS g_pos,
          arraySort(x -> x.1, groupUniqArrayIf((m_pos, m_hash), m_pos > h_pos)) AS g_sorted
        FROM (${here}) AS h LEFT JOIN (${members}) AS m ON m.m_seg = h.h_seg
        GROUP BY h_base))`;
  /* phase 1: the accepted spender of the current transaction's output 0 */
  const spent = `SELECT cur, toUInt32(0) FROM walk WHERE phase = 1`;
  const spenders = `SELECT outpoint_transaction_hash AS s_spent, min(transaction_hash) AS s_spender
    FROM ${c.sql.view(
      'input_at'
    )} WHERE (outpoint_transaction_hash, outpoint_index) IN (${spent})
      AND transaction_hash IN (${c.sql.accepted(
        c.node,
        `SELECT transaction_hash FROM ${c.sql.view(
          'input_at'
        )} WHERE (outpoint_transaction_hash, outpoint_index) IN (${spent})`
      )})
    GROUP BY outpoint_transaction_hash`;
  const across = `SELECT w.base, if(s.s_spender = ${zero}, w.cur, s.s_spender),
      w.len + (s.s_spender != ${zero}),
      if(s.s_spender = ${zero}, w.path, arrayPushBack(w.path, s.s_spender)),
      toUInt8(if(s.s_spender = ${zero} OR w.len + 1 >= ${max}, 2, 0))
    FROM walk AS w LEFT JOIN (${spenders}) AS s ON s.s_spent = w.cur
    WHERE w.phase = 1`;
  /* a statement-level CTE: the recursion is never nested in a subquery that is referenced twice */
  return `SELECT * FROM ${c.sql.cte(
    'authchains',
    `WITH RECURSIVE walk AS (
      SELECT hash AS base, hash AS cur, toUInt64(1) AS len, [hash] AS path, toUInt8(0) AS phase
      FROM (SELECT DISTINCT hash FROM (${link}))
      WHERE hash IN (${c.sql.accepted(c.node, `SELECT hash FROM (${link})`)})
      UNION ALL ${along}
      UNION ALL ${across}
    )
    SELECT base, argMax(cur, len) AS head, max(len) AS length, argMax(path, len) AS path,
      max(len) < ${max} AS unspent
    FROM walk WHERE phase = 2
    GROUP BY base`
  )}`;
};

/* ------------------------------------------------------------------ */
/* entities                                                             */
/* ------------------------------------------------------------------ */

/* eslint-disable @typescript-eslint/no-use-before-define */

const outputs = (
  condition: (c: Ctx, link: string) => string,
  many = true
): RelationField => ({
  childKeys: ['transaction_hash'],
  defaultOrder: [term('output_index')],
  kind: 'relation',
  many,
  order: indexOrder(outputOrder, 'output_index', [term('output_index')]),
  parentKeys: ['hash'],
  source: (c, link) => outputsOf(c, condition(c, link)),
  target: () => OutputEntity,
  where: (c, where) =>
    outputPredicate(c.sql, where as OutputFilter, outputRowColumns, c.node),
});

const transactionByHash = (parentKey: string): RelationField => ({
  childKeys: ['hash'],
  defaultOrder: [],
  kind: 'relation',
  many: false,
  parentKeys: [parentKey],
  source: transactionsByHash,
  target: () => TransactionEntity,
});

const nodeById: RelationField = {
  childKeys: ['internal_id'],
  defaultOrder: [],
  kind: 'relation',
  many: false,
  parentKeys: ['node_internal_id'],
  source: (_c, link) => nodesById(link),
  target: () => NodeEntity,
};

const blockById = (parentKey: string): RelationField => ({
  childKeys: ['internal_id'],
  defaultOrder: [],
  kind: 'relation',
  many: false,
  parentKeys: [parentKey],
  source: blocksById,
  target: () => BlockEntity,
});

export const TransactionEntity: Entity = {
  columns: transactionColumns,
  fields: {
    __enc_inputs: {
      childKeys: ['transaction_hash'],
      defaultOrder: [term('input_index')],
      kind: 'relation',
      many: true,
      parentKeys: ['hash'],
      source: (c, link) =>
        `SELECT * FROM ${c.sql.view(
          'input_at'
        )} WHERE transaction_hash IN (${link})`,
      target: () => InputEntity,
    },
    __enc_outputs: outputs((_c, link) => `transaction_hash IN (${link})`),
    __input_value: {
      childKeys: ['transaction_hash'],
      defaultOrder: [],
      kind: 'relation',
      many: false,
      parentKeys: ['hash'],
      source: inputValuesSql,
      target: () => InputValueEntity,
    },
    authchains: {
      childKeys: ['base'],
      defaultOrder: [],
      kind: 'relation',
      many: true,
      parentKeys: ['hash'],
      perNode: true,
      recursive: true,
      source: authchainSql,
      target: () => AuthchainEntity,
    },
    block_inclusions: {
      childKeys: ['transaction_hash'],
      defaultOrder: [term('height'), term('block_internal_id')],
      kind: 'relation',
      many: true,
      order: listOrder(blockInclusionOrder, [term('block_internal_id')]),
      parentKeys: ['hash'],
      perNode: true,
      /* transaction_index comes from block_transaction: joined only when read (the join costs ~5 ms) */
      source: (c, link, _args, needs) => {
        const acceptance = `SELECT transaction_hash, block_internal_id, height, accepted_at FROM ${c.sql.nodeView(
          'tx_acceptance_at',
          c.node
        )} WHERE block_internal_id != 0 AND transaction_hash IN (${link})`;
        return needs.has('transaction_index')
          ? `SELECT a.transaction_hash AS transaction_hash, a.block_internal_id AS block_internal_id,
          a.height AS height, a.accepted_at AS accepted_at, bt.transaction_index AS transaction_index
        FROM (${acceptance}) AS a
        LEFT JOIN (SELECT block_internal_id, transaction_hash, transaction_index FROM ${c.sql.view(
          'block_transaction_at'
        )} WHERE transaction_hash IN (${link})) AS bt
          ON bt.block_internal_id = a.block_internal_id AND bt.transaction_hash = a.transaction_hash`
          : acceptance;
      },
      target: () => BlockInclusionEntity,
      where: (c, where) =>
        blockInclusionPredicate(c.sql, where as BlockInclusionFilter, c.node),
    },
    data_carrier_outputs: outputs(
      (_c, link) =>
        `transaction_hash IN (${link}) AND (value_satoshis = 0 OR substring(locking_bytecode, 1, 1) = unhex('6a'))`
    ),
    encoded_hex: {
      kind: 'computed',
      needs: () => [
        sel('version'),
        sel('locktime'),
        sel(
          '__enc_inputs',
          [
            'outpoint_transaction_hash',
            'outpoint_index',
            'unlocking_bytecode',
            'sequence_number',
          ].map((name) => sel(name))
        ),
        sel(
          '__enc_outputs',
          [
            'value_satoshis',
            'locking_bytecode',
            'token_category',
            'fungible_token_amount',
            'nonfungible_token_capability',
            'nonfungible_token_commitment',
          ].map((name) => sel(name))
        ),
      ],
    },
    fee_satoshis: {
      kind: 'computed',
      needs: () => [
        sel('is_coinbase'),
        sel('output_value_satoshis'),
        sel('__input_value', [sel('value')]),
      ],
    },
    hash: scalar(hex('hash')),
    identity_output: outputs(
      (_c, link) => `transaction_hash IN (${link}) AND output_index = 0`,
      false
    ),
    input_count: scalar('input_count'),
    input_value_satoshis: {
      kind: 'computed',
      needs: () => [sel('__input_value', [sel('value')])],
    },
    inputs: {
      childKeys: ['transaction_hash'],
      defaultOrder: [term('input_index')],
      kind: 'relation',
      many: true,
      order: indexOrder(inputOrder, 'input_index', [term('input_index')]),
      parentKeys: ['hash'],
      source: (c, link) =>
        `SELECT * FROM ${c.sql.view(
          'input_at'
        )} WHERE transaction_hash IN (${link})`,
      target: () => InputEntity,
      where: (c, where, source) =>
        inputPredicate(
          c.sql,
          where as InputFilter,
          c.node,
          `SELECT outpoint_transaction_hash, outpoint_index FROM (${source})`
        ),
    },
    internal_id: scalar(str('internal_id')),
    is_coinbase: scalar('is_coinbase'),
    locktime: scalar(str('locktime')),
    node_validation_timeline: {
      childKeys: ['transaction_internal_id'],
      defaultOrder: [term('internal_id')],
      kind: 'relation',
      many: true,
      order: listOrder(timelineOrder, [term('internal_id')]),
      parentKeys: ['internal_id'],
      perNode: true,
      source: (c, link) =>
        `SELECT * FROM ${c.sql.nodeView(
          'node_transaction_history_at',
          c.node
        )} WHERE transaction_internal_id IN (${link})`,
      target: () => TimelineEntity,
    },
    node_validations: {
      childKeys: ['transaction_hash'],
      defaultOrder: [term('transaction_internal_id')],
      kind: 'relation',
      many: true,
      parentKeys: ['hash'],
      perNode: true,
      source: (c, link) =>
        `SELECT * FROM ${c.sql.nodeView(
          'node_transaction_at',
          c.node
        )} WHERE transaction_hash IN (${link})`,
      target: () => NodeTransactionEntity,
      where: (c, where) =>
        nodeTransactionPredicate(c.sql, where as NodeTransactionFilter, c.node),
    },
    output_count: scalar('output_count'),
    output_value_satoshis: scalar(str('output_value_satoshis')),
    outputs: outputs((_c, link) => `transaction_hash IN (${link})`),
    signing_output: outputs(
      (_c, link) => `transaction_hash IN (${link}) AND output_index = 1`,
      false
    ),
    size_bytes: scalar('size_bytes'),
    version: scalar('version'),
  },
  name: 'Transaction',
};

export const InputValueEntity: Entity = {
  columns: ['transaction_hash', 'input_value'],
  fields: { value: scalar(str('input_value')) },
  name: 'InputValue',
};

export const OutputEntity: Entity = {
  columns: outputColumns,
  fields: {
    fungible_token_amount: scalar(str('fungible_token_amount')),
    locking_bytecode: scalar(hex('locking_bytecode')),
    locking_bytecode_pattern: {
      kind: 'computed',
      needs: () => [sel('locking_bytecode')],
    },
    nonfungible_token_capability: scalar(str('nonfungible_token_capability')),
    nonfungible_token_commitment: scalar(hex('nonfungible_token_commitment')),
    output_index: scalar('output_index'),
    spent_by: {
      childKeys: ['outpoint_transaction_hash', 'outpoint_index'],
      defaultOrder: [term('transaction_internal_id'), term('input_index')],
      kind: 'relation',
      many: true,
      order: listOrder(inputOrder, [
        term('transaction_internal_id'),
        term('input_index'),
      ]),
      parentKeys: ['transaction_hash', 'output_index'],
      perNode: true,
      source: (c, link) =>
        c.sql.acceptedRows(
          `SELECT * FROM ${c.sql.view(
            'input_at'
          )} WHERE (outpoint_transaction_hash, outpoint_index) IN (${link})`,
          '*',
          c.node
        ),
      target: () => InputEntity,
      where: (c, where, source) =>
        inputPredicate(
          c.sql,
          where as InputFilter,
          c.node,
          `SELECT outpoint_transaction_hash, outpoint_index FROM (${source})`
        ),
    },
    token_category: scalar(
      `if(token_category = ${zeroCategory}, NULL, ${hex('token_category')})`
    ),
    transaction: transactionByHash('transaction_hash'),
    transaction_hash: scalar(hex('transaction_hash')),
    transaction_internal_id: scalar(str('transaction_internal_id')),
    value_satoshis: scalar(str('value_satoshis')),
  },
  name: 'Output',
};

export const InputEntity: Entity = {
  columns: inputColumns,
  fields: {
    __outpoint_value: {
      childKeys: ['transaction_hash', 'output_index'],
      defaultOrder: [],
      kind: 'relation',
      many: false,
      parentKeys: ['outpoint_transaction_hash', 'outpoint_index'],
      source: (c, link) =>
        outputsOf(c, `(transaction_hash, output_index) IN (${link})`),
      target: () => OutputEntity,
    },
    input_index: scalar('input_index'),
    outpoint: {
      childKeys: ['transaction_hash', 'output_index'],
      defaultOrder: [],
      kind: 'relation',
      many: false,
      parentKeys: ['outpoint_transaction_hash', 'outpoint_index'],
      source: (c, link) =>
        outputsOf(c, `(transaction_hash, output_index) IN (${link})`),
      target: () => OutputEntity,
    },
    outpoint_index: scalar(str('outpoint_index')),
    outpoint_transaction_hash: scalar(hex('outpoint_transaction_hash')),
    redeem_bytecode_pattern: {
      kind: 'computed',
      needs: () => [sel('unlocking_bytecode')],
    },
    sequence_number: scalar(str('sequence_number')),
    transaction: transactionByHash('transaction_hash'),
    transaction_hash: scalar(hex('transaction_hash')),
    transaction_internal_id: scalar(str('transaction_internal_id')),
    unlocking_bytecode: scalar(hex('unlocking_bytecode')),
    unlocking_bytecode_pattern: {
      kind: 'computed',
      needs: () => [sel('unlocking_bytecode')],
    },
    value_satoshis: {
      kind: 'computed',
      needs: () => [sel('__outpoint_value', [sel('value_satoshis')])],
    },
  },
  name: 'Input',
};

const headerNeeds = () =>
  [
    'version',
    'previous_block_hash',
    'merkle_root',
    'timestamp',
    'bits',
    'nonce',
  ].map((name) => sel(name));

export const BlockEntity: Entity = {
  columns: blockColumns,
  fields: {
    __block_counts: {
      childKeys: ['block_internal_id'],
      defaultOrder: [],
      kind: 'relation',
      many: false,
      parentKeys: ['internal_id'],
      source: (c, link) => `SELECT bt.block_internal_id AS block_internal_id,
          sum(t.input_count) AS input_count, sum(t.output_count) AS output_count
        FROM (SELECT block_internal_id, transaction_hash FROM ${c.sql.view(
          'block_transaction_at'
        )} WHERE block_internal_id IN (${link})) AS bt
        INNER JOIN (SELECT hash, input_count, output_count FROM ${c.sql.view(
          'transaction_at'
        )} WHERE hash IN (${blockTransactionHashes(c, link)})) AS t
          ON t.hash = bt.transaction_hash
        GROUP BY bt.block_internal_id`,
      target: () => BlockCountsEntity,
    },
    __block_values: {
      childKeys: ['block_internal_id'],
      defaultOrder: [],
      kind: 'relation',
      many: false,
      parentKeys: ['internal_id'],
      source: (c, link) => `SELECT bt.block_internal_id AS block_internal_id,
          sum(ifNull(v.input_value, 0)) AS input_value,
          sumIf(t.output_value_satoshis, NOT t.is_coinbase) AS noncoinbase_output_value
        FROM (SELECT block_internal_id, transaction_hash FROM ${c.sql.view(
          'block_transaction_at'
        )} WHERE block_internal_id IN (${link})) AS bt
        INNER JOIN (SELECT hash, is_coinbase, output_value_satoshis FROM ${c.sql.view(
          'transaction_at'
        )} WHERE hash IN (${blockTransactionHashes(c, link)})) AS t
          ON t.hash = bt.transaction_hash
        LEFT JOIN (${inputValuesSql(c, blockTransactionHashes(c, link))}) AS v
          ON v.transaction_hash = bt.transaction_hash
        GROUP BY bt.block_internal_id`,
      target: () => BlockValuesEntity,
    },
    __enc_transactions: {
      childKeys: ['block_internal_id'],
      defaultOrder: [term('transaction_index')],
      kind: 'relation',
      many: true,
      parentKeys: ['internal_id'],
      source: (c, link) =>
        `SELECT * FROM ${c.sql.view(
          'block_transaction_at'
        )} WHERE block_internal_id IN (${link})`,
      target: () => BlockTransactionEntity,
    },
    accepted_by: {
      childKeys: ['block_internal_id'],
      defaultOrder: [term('node_internal_id')],
      kind: 'relation',
      many: true,
      parentKeys: ['internal_id'],
      perNode: true,
      source: (c, link) =>
        `SELECT * FROM ${c.sql.nodeView(
          'node_block_at',
          c.node
        )} WHERE block_internal_id IN (${link})`,
      target: () => NodeBlockEntity,
      where: (c, where) =>
        nodeBlockPredicate(c.sql, where as NodeBlockFilter, c.node),
    },
    bits: scalar(str('bits')),
    encoded_hex: {
      kind: 'computed',
      needs: () => [
        ...headerNeeds(),
        sel('__enc_transactions', [sel('transaction', [sel('encoded_hex')])]),
      ],
    },
    fee_satoshis: {
      kind: 'computed',
      needs: () => [
        sel('__block_values', [
          sel('input_value'),
          sel('noncoinbase_output_value'),
        ]),
      ],
    },
    generated_value_satoshis: {
      kind: 'computed',
      needs: () => [
        sel('output_value_satoshis'),
        sel('__block_values', [
          sel('input_value'),
          sel('noncoinbase_output_value'),
        ]),
      ],
    },
    hash: scalar(hex('hash')),
    header: { kind: 'computed', needs: headerNeeds },
    height: scalar('height'),
    input_count: {
      kind: 'computed',
      needs: () => [
        sel('__block_counts', [sel('input_count'), sel('output_count')]),
      ],
    },
    input_value_satoshis: {
      kind: 'computed',
      needs: () => [
        sel('__block_values', [
          sel('input_value'),
          sel('noncoinbase_output_value'),
        ]),
      ],
    },
    internal_id: scalar(str('internal_id')),
    merkle_root: scalar(hex('merkle_root')),
    nonce: scalar(str('nonce')),
    output_count: {
      kind: 'computed',
      needs: () => [
        sel('__block_counts', [sel('input_count'), sel('output_count')]),
      ],
    },
    output_value_satoshis: scalar(str('output_value_satoshis')),
    previous_block_hash: scalar(hex('previous_block_hash')),
    size_bytes: scalar('size_bytes'),
    timestamp: scalar(str('timestamp')),
    transaction_count: scalar('transaction_count'),
    transactions: {
      childKeys: ['block_internal_id'],
      defaultOrder: [term('transaction_index')],
      kind: 'relation',
      many: true,
      order: listOrder(blockTransactionOrder, [term('transaction_index')]),
      parentKeys: ['internal_id'],
      source: (c, link) =>
        `SELECT * FROM ${c.sql.view(
          'block_transaction_at'
        )} WHERE block_internal_id IN (${link})`,
      target: () => BlockTransactionEntity,
      where: (c, where) =>
        blockTransactionPredicate(
          c.sql,
          where as BlockTransactionFilter,
          c.node
        ),
    },
    version: scalar('version'),
  },
  name: 'Block',
};

export const BlockCountsEntity: Entity = {
  columns: ['block_internal_id', 'input_count', 'output_count'],
  fields: {
    input_count: scalar(str('input_count')),
    output_count: scalar(str('output_count')),
  },
  name: 'BlockCounts',
};

export const BlockValuesEntity: Entity = {
  columns: ['block_internal_id', 'input_value', 'noncoinbase_output_value'],
  fields: {
    input_value: scalar(str('input_value')),
    noncoinbase_output_value: scalar(str('noncoinbase_output_value')),
  },
  name: 'BlockValues',
};

export const BlockInclusionEntity: Entity = {
  columns: [
    'transaction_hash',
    'block_internal_id',
    'height',
    'accepted_at',
    'transaction_index',
  ],
  fields: {
    accepted_at: scalar(time('accepted_at')),
    block: blockById('block_internal_id'),
    block_internal_id: scalar(str('block_internal_id')),
    transaction_index: scalar('transaction_index'),
  },
  name: 'BlockInclusion',
};

export const BlockTransactionEntity: Entity = {
  columns: [
    'block_internal_id',
    'transaction_index',
    'transaction_internal_id',
    'transaction_hash',
  ],
  fields: {
    block: blockById('block_internal_id'),
    transaction: transactionByHash('transaction_hash'),
    transaction_hash: scalar(hex('transaction_hash')),
    transaction_index: scalar('transaction_index'),
    transaction_internal_id: scalar(str('transaction_internal_id')),
  },
  name: 'BlockTransaction',
};

export const NodeTransactionEntity: Entity = {
  columns: [
    'node_internal_id',
    'transaction_internal_id',
    'transaction_hash',
    'validated_at',
  ],
  fields: {
    node: nodeById,
    node_internal_id: scalar('node_internal_id'),
    transaction: transactionByHash('transaction_hash'),
    transaction_hash: scalar(hex('transaction_hash')),
    transaction_internal_id: scalar(str('transaction_internal_id')),
    validated_at: scalar(time('validated_at')),
  },
  name: 'NodeTransaction',
};

export const TimelineEntity: Entity = {
  columns: [
    'node_internal_id',
    'transaction_internal_id',
    'internal_id',
    'validated_at',
    'replaced_at',
  ],
  fields: {
    node: nodeById,
    replaced_at: scalar(time('replaced_at')),
    transaction_internal_id: scalar(str('transaction_internal_id')),
    validated_at: scalar(time('validated_at')),
  },
  name: 'NodeTransactionHistory',
};

export const NodeBlockEntity: Entity = {
  columns: [
    'node_internal_id',
    'block_internal_id',
    'block_hash',
    'height',
    'accepted_at',
  ],
  fields: {
    accepted_at: scalar(time('accepted_at')),
    block: blockById('block_internal_id'),
    block_hash: scalar(hex('block_hash')),
    block_internal_id: scalar(str('block_internal_id')),
    height: scalar('height'),
    node: nodeById,
    node_internal_id: scalar('node_internal_id'),
  },
  name: 'NodeBlock',
};

export const NodeEntity: Entity = {
  columns: [
    'internal_id',
    'name',
    'protocol_version',
    'user_agent',
    'first_connected_at',
    'latest_connection_began_at',
  ],
  fields: {
    __mempool_count: {
      childKeys: ['node_internal_id'],
      defaultOrder: [],
      kind: 'relation',
      many: false,
      parentKeys: ['internal_id'],
      perNode: true,
      source: (c, link) =>
        `SELECT node_internal_id, count() AS count FROM ${c.sql.nodeView(
          'node_transaction_at',
          c.node
        )} WHERE node_internal_id IN (${link}) GROUP BY node_internal_id`,
      target: () => CountEntity,
    },
    accepted_blocks: {
      childKeys: ['node_internal_id'],
      defaultOrder: [term('block_internal_id')],
      kind: 'relation',
      many: true,
      order: listOrder(nodeBlockOrder, [term('block_internal_id')]),
      parentKeys: ['internal_id'],
      perNode: true,
      source: (c, link) =>
        `SELECT * FROM ${c.sql.nodeView(
          'node_block_at',
          c.node
        )} WHERE node_internal_id IN (${link})`,
      target: () => NodeBlockEntity,
      where: (c, where) =>
        nodeBlockPredicate(c.sql, where as NodeBlockFilter, c.node),
    },
    first_connected_at: scalar(time('first_connected_at')),
    latest_connection_began_at: scalar(time('latest_connection_began_at')),
    name: scalar('name'),
    protocol_version: scalar('protocol_version'),
    unconfirmed_transaction_count: {
      kind: 'computed',
      needs: () => [sel('__mempool_count', [sel('count')])],
    },
    unconfirmed_transactions: {
      childKeys: ['node_internal_id'],
      defaultOrder: [term('transaction_internal_id')],
      kind: 'relation',
      many: true,
      order: listOrder(nodeTransactionOrder, [term('transaction_internal_id')]),
      parentKeys: ['internal_id'],
      perNode: true,
      source: (c, link) =>
        `SELECT * FROM ${c.sql.nodeView(
          'node_transaction_at',
          c.node
        )} WHERE node_internal_id IN (${link})`,
      target: () => NodeTransactionEntity,
      where: (c, where) =>
        nodeTransactionPredicate(c.sql, where as NodeTransactionFilter, c.node),
    },
    user_agent: scalar('user_agent'),
  },
  name: 'Node',
};

export const CountEntity: Entity = {
  columns: ['node_internal_id', 'count'],
  fields: { count: scalar(str('count')) },
  name: 'Count',
};

/* ------------------------------------------------------------------ */
/* authchains                                                           */
/* ------------------------------------------------------------------ */

export const AuthchainEntity: Entity = {
  columns: ['base', 'head', 'length', 'path', 'unspent'],
  fields: {
    authchain_length: scalar('length'),
    authhead: transactionByHash('head'),
    authhead_transaction_hash: scalar(hex('head')),
    migrations: {
      childKeys: ['base'],
      defaultOrder: [term('migration_index')],
      kind: 'relation',
      linkColumns: ['base', 'path'],
      many: true,
      order: listOrder(migrationOrder, [term('migration_index')]),
      parentKeys: ['base'],
      source: (_c, link) =>
        `SELECT base, toUInt32(index - 1) AS migration_index, migration AS transaction_hash
          FROM (${link}) ARRAY JOIN path AS migration, arrayEnumerate(path) AS index`,
      target: () => MigrationEntity,
    },
    unspent_authhead: scalar('unspent'),
  },
  name: 'Authchain',
};

export const MigrationEntity: Entity = {
  columns: ['base', 'migration_index', 'transaction_hash'],
  fields: {
    migration_index: scalar('migration_index'),
    transaction: transactionByHash('transaction_hash'),
    transaction_hash: scalar(hex('transaction_hash')),
  },
  name: 'AuthchainMigration',
};

/* eslint-enable @typescript-eslint/no-use-before-define */

/** Block filter → predicate (re-exported for roots). */
export const blockWhere = (c: Ctx, where: BlockFilter | null | undefined) =>
  blockPredicate(c.sql, where, c.node);
