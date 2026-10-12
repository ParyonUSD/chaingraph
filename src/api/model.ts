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
 *     (structural; opt-in `accepted_by`)       10. Node.accepted_blocks (per node)
 *  4. Output.transaction, Input.transaction    11. Block.transactions → BlockTransaction.transaction
 *  5. Transaction.block_inclusions → block     12. Transaction.authchains → authhead / migrations
 *     (structural; opt-in `accepted_by`)           (per node); authchain_branches (structural)
 *  6. Block.accepted_by → node (per node)      13. Transaction.identity_output, signing_output,
 *                                                  data_carrier_outputs
 *
 * Structural facts (owner decision 2026-10-11) are node-agnostic and
 * complete: every stored block containing a transaction (stale and fork
 * blocks included), every stored input spending an output (conflicting
 * spends included), every branch of an identity-output tree. Acceptance is
 * opt-in and per node: `accepted_by: node` on those relationships,
 * `accepted(node:)` on transactions, inputs and inclusions. No field answers
 * "accepted by any node".
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
  authchainTooWideCode,
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

/** Inclusions by height, then block hash (owner decision 2026-10-12, item 3). */
const blockInclusionDefaultOrder = [term('height'), term('block_hash')];

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
  /*
   * phase 0: the current transaction's segment and position (no row: it
   * starts its own). The path stays out of the join with the segment's
   * members (one copy per member: a 5,870-link path into a 23,027-member
   * segment is ~4 GB on chipnet); it is joined back by base (one walk row
   * per base).
   */
  const here = `SELECT w.base AS h_base, w.cur AS h_cur, w.len AS h_len,
      if(l.transaction_hash = ${zero}, t.t_id, l.l_segment) AS h_seg,
      if(l.transaction_hash = ${zero}, toUInt32(0), l.l_position) AS h_pos
    FROM (SELECT base, cur, len, phase FROM walk) AS w
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
      arrayConcat(p.p_path, g_next), toUInt8(if(g_len + length(g_next) >= ${max}, 2, 1))
    FROM (SELECT g_base, g_cur, g_len,
        arraySlice(arrayMap(x -> x.2, arraySlice(g_sorted, 1,
          toInt64(if(${contiguous} = 0, length(g_sorted), ${contiguous} - 1)))), 1,
          toInt64(${max}) - toInt64(g_len)) AS g_next
      FROM (SELECT h_base AS g_base, any(h_cur) AS g_cur, any(h_len) AS g_len,
          any(h_pos) AS g_pos,
          arraySort(x -> x.1, groupUniqArrayIf((m_pos, m_hash), m_pos > h_pos)) AS g_sorted
        FROM (${here}) AS h LEFT JOIN (${members}) AS m ON m.m_seg = h.h_seg
        GROUP BY h_base)) AS g
    INNER JOIN (SELECT base AS p_base, path AS p_path FROM walk WHERE phase = 0) AS p ON p.p_base = g.g_base`;
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
      max(len) < ${max} AS unspent, toUInt64(0) AS branch
    FROM walk WHERE phase = 2
    GROUP BY base`
  )}`;
};

/**
 * Node-agnostic: every branch of the identity-output tree from each base
 * transaction, over all stored spends of output 0 (conflicting spends and
 * transactions of stale blocks included; no acceptance). One row per path
 * from the base to an output 0 no stored transaction spends (or to the depth
 * limit); a transaction spending the outputs 0 of two members of the tree
 * (a merge) is on two paths. Columns as `authchainSql`; `branch` tells the
 * paths of one base apart.
 *
 * A walk over the authchain index, one recursive CTE alternating two steps:
 * - phase 0, along a segment: if the current transaction is the only member
 *   at its position (or it starts the segment), the run of positions after
 *   it holding exactly one member each (one keyed read of the segment). A
 *   member at position p + 1 was placed from its heavy parent at position p,
 *   so the run is a chain of output-0 spends; conflicting spends placed on
 *   the segment (two members at one position) end the run;
 * - phase 1, verify and branch: every spender of output 0 of the members just
 *   added (the input table, any commit: also above the spend watermark).
 *   Spenders other than the next member start new branches (light children,
 *   conflicting spends placed elsewhere, transactions not indexed yet); the
 *   path is cut where the next member is not a spender (a stale placement
 *   costs a step, never a wrong edge); an output 0 without spenders ends a
 *   branch (phase 2).
 */
export const authchainBranchesSql = (c: Ctx, link: string) => {
  const zero = `toFixedString(unhex(repeat('00', 32)), 32)`;
  const links = c.sql.view('authchain_link_at');
  const max = String(maxAuthchainDepth);
  /*
   * A walk row is identified by (base, sipHash64(path)): the steps join and
   * group by that, never by the path itself, and take the path back once
   * per row (`paths`). A path carried into a join with every member of a
   * segment is copied once per member: ClickHouse 26.8 may evaluate an
   * expression over `walk`'s columns after a later join, so the path is
   * dropped in a subquery of `walk` before any join (a 5,870-link path into
   * a 23,027-member segment: 7.2 GB and 2.3 s; dropped: 0.3 GB, 0.25 s).
   */
  const paths = (phase: number) =>
    `SELECT base AS p_base, sipHash64(path) AS p_pid, path AS p_path FROM walk WHERE phase = ${phase}`;
  /* phase 0: the current transaction's segment and position (no row: it starts its own) */
  const here = `SELECT w.base AS h_base, w.cur AS h_cur, w.len AS h_len, w.pid AS h_pid,
      if(l.transaction_hash = ${zero}, t.t_id, l.l_segment) AS h_seg,
      if(l.transaction_hash = ${zero}, toUInt32(0), l.l_position) AS h_pos
    FROM (SELECT base, cur, len, phase, sipHash64(path) AS pid FROM walk) AS w
    LEFT JOIN (SELECT transaction_hash, min(segment) AS l_segment, argMin(position, segment) AS l_position
      FROM ${links} WHERE transaction_hash IN (SELECT cur FROM walk WHERE phase = 0)
      GROUP BY transaction_hash) AS l ON l.transaction_hash = w.cur
    LEFT JOIN (SELECT hash, min(internal_id) AS t_id FROM ${c.sql.view(
      'transaction_at'
    )} WHERE hash IN (SELECT cur FROM walk WHERE phase = 0) GROUP BY hash) AS t ON t.hash = w.cur
    WHERE w.phase = 0`;
  const members = `SELECT segment AS m_seg, position AS m_pos, transaction_hash AS m_hash FROM ${links}
    WHERE segment IN (SELECT h_seg FROM (${here}))`;
  /* per walk row: members at or after its position, sorted; a position holding two members is a conflict */
  const runs = `SELECT g_base, g_cur, g_len, g_pid, g_pos, g_sorted,
      arrayMap(x -> x.1, g_sorted) AS g_ps,
      /* adjacent positions as shifted arrays: a lambda capturing g_ps would copy it per element */
      arrayMap((p, before, after) -> p = before OR p = after, g_ps,
        arrayPushFront(arrayPopBack(g_ps), toUInt32(0)),
        arrayPushBack(arrayPopFront(g_ps), toUInt32(0))) AS g_dup,
      arrayFilter(x -> tupleElement(x.1, 1) > g_pos, arrayZip(g_sorted, g_dup)) AS g_after,
      arrayFirstIndex((x, i) -> tupleElement(x.1, 1) != g_pos + i OR x.2, g_after, arrayEnumerate(g_after)) AS g_fail,
      g_pos = 0 OR countEqual(g_ps, g_pos) = 1 AS g_unique
    FROM (SELECT h_base AS g_base, any(h_cur) AS g_cur, any(h_len) AS g_len, h_pid AS g_pid,
        any(h_pos) AS g_pos,
        arraySort(groupUniqArrayIf((m_pos, m_hash), m_pos >= h_pos)) AS g_sorted
      FROM (${here}) AS h LEFT JOIN (${members}) AS m ON m.m_seg = h.h_seg
      GROUP BY h_base, h_pid)`;
  const along = `SELECT g_base, if(empty(g_next), g_cur, g_next[-1]), g_len + length(g_next),
      arrayConcat(p.p_path, g_next), toUInt8(1), g_len
    FROM (SELECT g_base, g_cur, g_len, g_pid,
        if(g_unique, arraySlice(arrayMap(x -> tupleElement(x.1, 2), arraySlice(g_after, 1,
          toInt64(if(g_fail = 0, length(g_after), g_fail - 1)))), 1,
          toInt64(${max}) - toInt64(g_len)), []) AS g_next
      FROM (${runs})) AS g
    INNER JOIN (${paths(0)}) AS p ON p.p_base = g.g_base AND p.p_pid = g.g_pid`;
  /* phase 1: every spender of output 0 of the members added since `start` */
  const spenders = `SELECT outpoint_transaction_hash AS s_parent, groupUniqArray(transaction_hash) AS s_kids
    FROM ${c.sql.view('input_at')}
    WHERE (outpoint_transaction_hash, outpoint_index) IN (
      SELECT member, toUInt32(0) FROM (SELECT arraySlice(path, start) AS added FROM walk WHERE phase = 1)
        ARRAY JOIN added AS member)
    GROUP BY outpoint_transaction_hash`;
  /*
   * (j, member j, member j + 1) of each row's added members, zipped from
   * slices (a lambda reading `path` would copy it per element); the path
   * itself stays behind
   */
  const items = `SELECT x.x_base AS x_base, x.x_pid AS x_pid, x.x_j AS x_j, x.x_next AS x_next,
      s.s_kids AS x_kids
    FROM (SELECT x_base, x_pid, e.1 AS x_j, e.2 AS x_m, e.3 AS x_next
      FROM (SELECT base AS x_base, sipHash64(path) AS x_pid,
          arrayZip(range(start, len + 1), arraySlice(path, start),
            arrayPushBack(arraySlice(path, start + 1), ${zero})) AS x_list
        FROM walk WHERE phase = 1)
      ARRAY JOIN x_list AS e) AS x
    LEFT JOIN (${spenders}) AS s ON s.s_parent = x.x_m`;
  /* the path holds while each next member spends the previous one's output 0 (y_c: the last item kept) */
  const ends = `SELECT y_base, y_pid, y_items, if(y_fail = 0, length(y_items), y_fail) AS y_c
    FROM (SELECT y_base, y_pid, y_items,
        arrayFirstIndex(x -> x.2 != ${zero} AND NOT has(x.3, x.2), y_items) AS y_fail
      FROM (SELECT x_base AS y_base, x_pid AS y_pid,
          arraySort(x -> x.1, groupArray((x_j, x_next, x_kids))) AS y_items
        FROM (${items}) GROUP BY x_base, x_pid))`;
  /*
   * New rows: a branch for every spender of a kept item other than the next
   * member (kind 0), the end of the path where the last kept item has no
   * spender (kind 2). Expanded by ARRAY JOIN without the path, which is
   * joined back once per new row (array expressions over the items that
   * build these rows cost ~7 GB on a 5,870-item run on ClickHouse 26.8).
   */
  const branches = `SELECT b_base, b_pid, b_j, b_kid, toUInt8(0) AS b_kind
    FROM (SELECT y_base AS b_base, y_pid AS b_pid, item.1 AS b_j, item.2 AS b_next, item.3 AS b_kids
      FROM (SELECT y_base, y_pid, arraySlice(y_items, 1, y_c) AS kept FROM (${ends}))
      ARRAY JOIN kept AS item)
    ARRAY JOIN b_kids AS b_kid
    WHERE b_kid != b_next`;
  const terminals = `SELECT y_base, y_pid, y_items[y_c].1, ${zero}, toUInt8(2)
    FROM (${ends}) WHERE empty(y_items[y_c].3)`;
  /*
   * The branch cap (owner decision 2026-10-12): a step's new rows of one
   * base are distinct paths that each end in at least one final path, so
   * more of them than the cap already means a tree over it; the walk stops
   * there instead of building the rest (the final check below counts the
   * paths ended in earlier steps).
   */
  const cap = c.sql.authchainMaxBranches;
  if (!Number.isInteger(cap) || cap < 1) {
    throw new Error('authchainMaxBranches must be a positive integer.');
  }
  const tooWide = (count: string) =>
    `throwIf(${count} > ${cap}, '${authchainTooWideCode}: an identity-output tree has more than ${cap} branches (authchain_branches cap per base); ask for authchains(node:) or a narrower base') = 0`;
  const across = `SELECT a_base, a_cur, a_len, a_path, a_phase, a_start
    FROM (SELECT e.b_base AS a_base, if(e.b_kind = 2, p.p_path[e.b_j], e.b_kid) AS a_cur,
      if(e.b_kind = 2, e.b_j, e.b_j + 1) AS a_len,
      if(e.b_kind = 2, arraySlice(p.p_path, 1, e.b_j), arrayPushBack(arraySlice(p.p_path, 1, e.b_j), e.b_kid)) AS a_path,
      toUInt8(if(e.b_kind = 2 OR e.b_j + 1 >= ${max}, 2, 0)) AS a_phase, toUInt64(1) AS a_start,
      count() OVER (PARTITION BY e.b_base) AS a_new
    FROM (${branches} UNION ALL ${terminals}) AS e
    INNER JOIN (${paths(1)}) AS p ON p.p_base = e.b_base AND p.p_pid = e.b_pid)
    WHERE ${tooWide('a_new')}`;
  return `SELECT * FROM ${c.sql.cte(
    'authchain_branches',
    `WITH RECURSIVE walk AS (
      SELECT hash AS base, hash AS cur, toUInt64(1) AS len, [hash] AS path, toUInt8(0) AS phase,
        toUInt64(1) AS start
      FROM (SELECT DISTINCT hash FROM (${link}))
      UNION ALL ${along}
      UNION ALL ${across}
    )
    SELECT base, head, length, path, unspent, branch
    FROM (SELECT *, count() OVER (PARTITION BY base) AS paths_of_base
      FROM (SELECT DISTINCT base, cur AS head, len AS length, path, len < ${max} AS unspent,
          sipHash64(path) AS branch
        FROM walk WHERE phase = 2))
    WHERE ${tooWide('paths_of_base')}`
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

/** The node an `accepted_by:` argument names (undefined: node-agnostic). */
const acceptedByNode = (c: Ctx, args: { [name: string]: unknown }) =>
  typeof args.accepted_by === 'string' ? c.nodeId(args.accepted_by) : undefined;

/**
 * `accepted(node:)`: whether the named node accepts the parent row (a
 * transaction: a block it accepts or its mempool; a block: it accepts the
 * block). One row per accepted parent key; the resolver answers "any row".
 * The node is required: no field answers "accepted by any node".
 */
const acceptedTransaction = (parentKey: string): RelationField => ({
  childKeys: ['transaction_hash'],
  defaultOrder: [],
  kind: 'relation',
  many: true,
  parentKeys: [parentKey],
  perNode: true,
  source: (c, link) =>
    `SELECT DISTINCT transaction_hash FROM ${c.sql.nodeView(
      'tx_acceptance_at',
      c.node,
      `transaction_hash IN (${link})`
    )}`,
  target: () => AcceptanceMarkEntity,
});

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
    accepted: acceptedTransaction('hash'),
    authchain_branches: {
      childKeys: ['base'],
      defaultOrder: [
        term('length', 'DESC'),
        term('head', 'ASC', 'hex'),
        term('branch'),
      ],
      kind: 'relation',
      many: true,
      parentKeys: ['hash'],
      recursive: true,
      source: authchainBranchesSql,
      target: () => AuthchainEntity,
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
      /*
       * By height, then block hash (owner decision 2026-10-12, item 3): a
       * stable order that does not depend on the store's internal ids. It
       * joins the inclusions to their blocks (once, a statement-level CTE;
       * ~10-20 ms on the library documents), for a list that is almost
       * always one block. `order_by: { block_internal_id }` (storage order)
       * reads no block row unless a block field is selected.
       */
      defaultOrder: blockInclusionDefaultOrder,
      kind: 'relation',
      many: true,
      order: listOrder(blockInclusionOrder, blockInclusionDefaultOrder),
      parentKeys: ['hash'],
      /*
       * Node-agnostic and complete: every stored block containing the
       * transaction (blocks of stale forks included). `accepted_by: node`
       * keeps the blocks that node accepts (read from its acceptance rows;
       * transaction_index from block_transaction only when read: ~5 ms).
       */
      source: (c, link, args, needs) => {
        const node = acceptedByNode(c, args);
        const inclusions =
          node === undefined
            ? `SELECT block_internal_id, transaction_hash, transaction_index FROM ${c.sql.view(
                'block_transaction_at'
              )} WHERE transaction_hash IN (${link})`
            : needs.has('transaction_index')
            ? `SELECT a.transaction_hash AS transaction_hash, a.block_internal_id AS block_internal_id,
          bt.transaction_index AS transaction_index
        FROM (SELECT transaction_hash, block_internal_id FROM ${c.sql.nodeView(
          'tx_acceptance_at',
          node
        )} WHERE block_internal_id != 0 AND transaction_hash IN (${link})) AS a
        LEFT JOIN (SELECT block_internal_id, transaction_hash, transaction_index FROM ${c.sql.view(
          'block_transaction_at'
        )} WHERE transaction_hash IN (${link})) AS bt
          ON bt.block_internal_id = a.block_internal_id AND bt.transaction_hash = a.transaction_hash`
            : `SELECT transaction_hash, block_internal_id, toUInt32(0) AS transaction_index FROM ${c.sql.nodeView(
                'tx_acceptance_at',
                node
              )} WHERE block_internal_id != 0 AND transaction_hash IN (${link})`;
        if (!needs.has('height') && !needs.has('block_hash')) {
          return `SELECT transaction_hash, block_internal_id, toUInt32(0) AS height,
          toFixedString('', 32) AS block_hash, transaction_index FROM (${inclusions})`;
        }
        /* the inclusions once (a statement-level CTE), then their blocks' heights and hashes */
        const rows = c.sql.cte('incl', inclusions);
        return `SELECT bt.transaction_hash AS transaction_hash, bt.block_internal_id AS block_internal_id,
          b.height AS height, b.hash AS block_hash, bt.transaction_index AS transaction_index
        FROM ${rows} AS bt
        INNER JOIN (SELECT internal_id, height, hash FROM ${c.sql.view(
          'block_at'
        )} WHERE internal_id IN (SELECT block_internal_id FROM ${rows})) AS b
          ON b.internal_id = bt.block_internal_id`;
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
      /*
       * Node-agnostic and complete: every stored input spending the output
       * (conflicting spends included). `accepted_by: node` keeps the inputs
       * of transactions that node accepts.
       */
      source: (c, link, args) => {
        const rows = `SELECT * FROM ${c.sql.view(
          'input_at'
        )} WHERE (outpoint_transaction_hash, outpoint_index) IN (${link})`;
        const node = acceptedByNode(c, args);
        return node === undefined ? rows : c.sql.acceptedRows(rows, '*', node);
      },
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
    accepted: acceptedTransaction('transaction_hash'),
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
    'block_hash',
    'transaction_index',
  ],
  fields: {
    accepted: {
      childKeys: ['block_internal_id'],
      defaultOrder: [],
      kind: 'relation',
      many: true,
      parentKeys: ['block_internal_id'],
      perNode: true,
      source: (c, link) =>
        `SELECT DISTINCT block_internal_id FROM ${c.sql.nodeView(
          'node_block_at',
          c.node,
          `block_internal_id IN (${link})`
        )}`,
      target: () => AcceptanceMarkEntity,
    },
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

/** A row of an `accepted(node:)` relation: only its key. */
export const AcceptanceMarkEntity: Entity = {
  columns: ['transaction_hash', 'block_internal_id'],
  fields: {},
  name: 'AcceptanceMark',
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
  columns: ['base', 'head', 'length', 'path', 'unspent', 'branch'],
  fields: {
    authchain_length: scalar('length'),
    authhead: transactionByHash('head'),
    authhead_transaction_hash: scalar(hex('head')),
    migrations: {
      childKeys: ['base', 'branch'],
      defaultOrder: [term('migration_index')],
      kind: 'relation',
      linkColumns: ['base', 'branch', 'path'],
      many: true,
      order: listOrder(migrationOrder, [term('migration_index')]),
      parentKeys: ['base', 'branch'],
      source: (_c, link) =>
        `SELECT base, branch, toUInt32(index - 1) AS migration_index, migration AS transaction_hash
          FROM (${link}) ARRAY JOIN path AS migration, arrayEnumerate(path) AS index`,
      target: () => MigrationEntity,
    },
    unspent_authhead: scalar('unspent'),
  },
  name: 'Authchain',
};

export const MigrationEntity: Entity = {
  columns: ['base', 'branch', 'migration_index', 'transaction_hash'],
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
