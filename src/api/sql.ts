/* eslint-disable camelcase, @typescript-eslint/naming-convention, complexity, functional/no-throw-statement, @typescript-eslint/no-magic-numbers, @typescript-eslint/parameter-properties, max-params, no-underscore-dangle, max-lines, @typescript-eslint/no-use-before-define, functional/no-let, functional/no-loop-statement, @typescript-eslint/member-ordering */
// cspell:ignore clickhouse unhex seqs nfts
/**
 * SQL building blocks of the v2 API (Phase 2): a parameter binder and the
 * filter compilers for the typed `where` inputs.
 *
 * Rules (docs/clickhouse-port/phase2-api.md §2):
 * - Every client value is bound as a `{pN:Type}` query parameter; nothing a
 *   client sends is ever interpolated into SQL text. Identifiers come from
 *   fixed tables in this file, never from a request.
 * - The snapshot parameters (`node`, `visible`, `visible0`, `tail`, `fence`,
 *   `void`, `spend`, and `node_<id>` / `visible_<id>` for further pinned
 *   nodes) are set only from `readSnapshot(Multi)` (`Sql` constructor).
 *   Client values are always bound under `p0`, `p1`, …, so they cannot
 *   collide with or override a snapshot parameter.
 * - Every read goes through the rows of a pinned `*_at` view (`Sql.view`,
 *   `Sql.nodeView`), with the view's gate predicate inlined over its base
 *   table (`inlinePinnedView`: same rows, same snapshot parameters; the
 *   views themselves stay for ad-hoc readers). Phase 2 stage 2 contract
 *   change: the API no longer names the views in its statements.
 * - Per-node predicates (acceptance, mempool, blocks, spends) name their node
 *   explicitly; a filter without a node is node-agnostic.
 */
import { GraphQLError } from 'graphql';

import type { VisibilitySnapshot } from '../store/clickhouse/visibility.js';
import {
  inlinePinnedView,
  nodeAgnosticId,
  snapshotParams,
} from '../store/clickhouse/visibility.js';

/** Names of the snapshot parameters; a client can never set them. */
export const snapshotParameterNames = [
  'fence',
  'node',
  'spend',
  'tail',
  'visible',
  'visible0',
  'void',
] as const;

const clientParamPattern = /^p\d+$/u;
const extraNodeParamPattern = /^(?:node|visible)_\d+$/u;

/** True for a parameter name the binder hands out for client values. */
export const isClientParamName = (name: string) =>
  clientParamPattern.test(name);

/** True for a snapshot parameter of a further pinned node (`node_<id>`, `visible_<id>`). */
export const isExtraNodeParamName = (name: string) =>
  extraNodeParamPattern.test(name);

export const invalid = (message: string) =>
  new GraphQLError(message, { extensions: { code: 'BAD_USER_INPUT' } });

/** Maximum list elements bound in one query parameter (ClickHouse's 128 KiB field limit). */
export const maxListChunk = 1_000;

const chunks = <T>(values: readonly T[], size = maxListChunk): T[][] => {
  const out: T[][] = [];
  for (let start = 0; start < values.length; start += size) {
    out.push(values.slice(start, start + size));
  }
  return out.length === 0 ? [[]] : out;
};

export class Sql {
  readonly params: { [name: string]: unknown };

  private next = 0;

  /**
   * `snapshot`: the request snapshot at the statement's primary node (its
   * `node`/`visible` parameters; node 0 for a node-agnostic statement).
   * `others`: visible(n) of every further node the request pinned (one
   * multi-node snapshot: they share visible0, tail, fence and void).
   */
  constructor(
    readonly snapshot: VisibilitySnapshot,
    readonly others: ReadonlyMap<number, bigint> = new Map()
  ) {
    this.params = { ...snapshotParams(snapshot) };
  }

  /** The statement's primary node (undefined for a node-agnostic statement). */
  get primaryNode(): number | undefined {
    return this.snapshot.nodeId === nodeAgnosticId
      ? undefined
      : this.snapshot.nodeId;
  }

  private names = 0;

  /** A name for a statement-local CTE (unique within the statement). */
  uniqueName(prefix: string) {
    this.names += 1;
    return `${prefix}_${this.names}`;
  }

  /**
   * Items of the statement's top-level `WITH` clause, in definition order:
   * `MATERIALIZED` CTEs (`cte`) and, for a root with relationships, the
   * level subqueries of compile.ts. A statement builder emits them with
   * `withClause()`. CTEs are never nested inside a subquery: a scalar
   * subquery containing a `MATERIALIZED` CTE is evaluated again at every
   * reference on ClickHouse 26.8 (chipnet L31: 9 evaluations, 0.7 s).
   */
  readonly withItems: string[] = [];

  /** A statement-level `MATERIALIZED` CTE of `body`; returns its name. */
  cte(prefix: string, body: string) {
    const name = this.uniqueName(prefix);
    this.withItems.push(`${name} AS MATERIALIZED (${body})`);
    return name;
  }

  /** `WITH …` of the statement's items (empty when there are none). */
  withClause() {
    return this.withItems.length === 0
      ? ''
      : `WITH ${this.withItems.join(',\n')}\n`;
  }

  /**
   * Rows of `rows` (a subquery with a `transaction_hash` column) whose
   * transaction the node accepts, with `columns` selected: the rows are
   * computed once (a statement-level MATERIALIZED CTE) and the acceptance
   * check reads only their transactions' acceptance rows.
   */
  acceptedRows(rows: string, columns: string, node?: number) {
    const name = this.cte('acc', rows);
    return `SELECT ${columns} FROM ${name} WHERE transaction_hash IN (${this.accepted(
      node,
      `SELECT transaction_hash FROM ${name}`
    )})`;
  }

  /** Bind a value; returns its `{pN:Type}` placeholder. */
  bind(value: unknown, type: string) {
    const name = `p${this.next}`;
    this.next += 1;
    this.params[name] = value;
    return `{${name}:${type}}`;
  }

  /**
   * The rows of a node-agnostic pinned view (or the primary node's) at the
   * snapshot, as a FROM item (the gate inlined: `inlinePinnedView`).
   */
  view(name: string, keyFilter?: string) {
    return inlinePinnedView(name, this.snapshot, undefined, keyFilter);
  }

  /**
   * A node-scoped pinned view of `node` (default: the primary node). A node
   * other than the primary one must be pinned by the request snapshot; its
   * `node_<id>` / `visible_<id>` parameters come from that snapshot.
   */
  nodeView(name: string, node?: number, keyFilter?: string) {
    const id = node ?? this.primaryNode;
    if (id === undefined || id === nodeAgnosticId) {
      throw invalid(
        'This field reads per-node data: name the node (`node:` argument).'
      );
    }
    if (id === this.snapshot.nodeId) return this.view(name, keyFilter);
    const visible = this.others.get(id);
    if (visible === undefined) {
      throw new GraphQLError(`Node ${id} is not pinned by this request.`);
    }
    this.params[`node_${id}`] = id;
    this.params[`visible_${id}`] = visible;
    return inlinePinnedView(
      name,
      this.snapshot,
      { node: `node_${id}`, visible: `visible_${id}` },
      keyFilter
    );
  }

  /**
   * ACC: `SELECT transaction_hash` of transactions the node accepts (a block
   * it accepts or its mempool). With `among` (a subquery of transaction
   * hashes), only those are checked: the acceptance rows are then read by
   * the primary key (the filter applies to the base rows, below the
   * collapsing GROUP BY), not in full.
   */
  accepted(node?: number, among?: string) {
    return `SELECT transaction_hash FROM ${this.nodeView(
      'tx_acceptance_at',
      node,
      among === undefined ? undefined : `transaction_hash IN (${among})`
    )}`;
  }
}

/* ------------------------------------------------------------------ */
/* comparison input shapes (as coerced by the schema)                   */
/* ------------------------------------------------------------------ */

export interface IntComparison {
  _eq?: number | null;
  _neq?: number | null;
  _in?: readonly number[] | null;
  _nin?: readonly number[] | null;
  _gt?: number | null;
  _gte?: number | null;
  _lt?: number | null;
  _lte?: number | null;
  _is_null?: boolean | null;
}

export interface BigIntComparison {
  _eq?: bigint | null;
  _neq?: bigint | null;
  _in?: readonly bigint[] | null;
  _nin?: readonly bigint[] | null;
  _gt?: bigint | null;
  _gte?: bigint | null;
  _lt?: bigint | null;
  _lte?: bigint | null;
  _is_null?: boolean | null;
}

export interface HexComparison {
  _eq?: string | null;
  _neq?: string | null;
  _in?: readonly string[] | null;
  _nin?: readonly string[] | null;
  _gt?: string | null;
  _gte?: string | null;
  _lt?: string | null;
  _lte?: string | null;
  _is_null?: boolean | null;
  _prefix?: string | null;
  _prefix_in?: readonly string[] | null;
}

export type Capability = 'minting' | 'mutable' | 'none';

export interface CapabilityComparison {
  _eq?: Capability | null;
  _neq?: Capability | null;
  _in?: readonly Capability[] | null;
  _nin?: readonly Capability[] | null;
  _is_null?: boolean | null;
}

export interface BooleanComparison {
  _eq?: boolean | null;
  _neq?: boolean | null;
}

/** ISO-8601 timestamps (UTC unless the string has an offset). */
export interface TimestampComparison {
  _eq?: string | null;
  _neq?: string | null;
  _gt?: string | null;
  _gte?: string | null;
  _lt?: string | null;
  _lte?: string | null;
  _is_null?: boolean | null;
}

/* ------------------------------------------------------------------ */
/* filter input shapes                                                  */
/* ------------------------------------------------------------------ */

/** Filters on output attributes; also used for a spent output (`outpoint`). */
export interface OutputFilter {
  _and?: readonly OutputFilter[] | null;
  _or?: readonly OutputFilter[] | null;
  _not?: OutputFilter | null;
  transaction_hash?: HexComparison | null;
  transaction_internal_id?: BigIntComparison | null;
  output_index?: IntComparison | null;
  value_satoshis?: BigIntComparison | null;
  locking_bytecode?: HexComparison | null;
  token_category?: HexComparison | null;
  fungible_token_amount?: BigIntComparison | null;
  nonfungible_token_capability?: CapabilityComparison | null;
  nonfungible_token_commitment?: HexComparison | null;
  /** The creating transaction matches (same output row). */
  transaction?: TransactionFilter | null;
  /** EXISTS an input spending the output, in a transaction the node accepts, matching. */
  spent_by?: InputFilter | null;
}

export interface InputFilter {
  _and?: readonly InputFilter[] | null;
  _or?: readonly InputFilter[] | null;
  _not?: InputFilter | null;
  transaction_hash?: HexComparison | null;
  transaction_internal_id?: BigIntComparison | null;
  input_index?: IntComparison | null;
  outpoint_transaction_hash?: HexComparison | null;
  outpoint_index?: BigIntComparison | null;
  sequence_number?: BigIntComparison | null;
  /**
   * The spent output: the input row's outpoint is the key of an output
   * matching it (still a predicate on the same input row, so same-row
   * conjunction holds).
   */
  outpoint?: OutputFilter | null;
  /** The spending transaction matches. */
  transaction?: TransactionFilter | null;
}

export interface BlockWindow {
  /** Inclusive lower height bound. */
  from_height?: number | null;
  /** Exclusive upper height bound. */
  to_height?: number | null;
  /** Also match transactions in the node's mempool (default true). */
  include_mempool?: boolean | null;
}

export interface TransactionFilter {
  _and?: readonly TransactionFilter[] | null;
  _or?: readonly TransactionFilter[] | null;
  _not?: TransactionFilter | null;
  internal_id?: BigIntComparison | null;
  hash?: HexComparison | null;
  version?: IntComparison | null;
  locktime?: BigIntComparison | null;
  size_bytes?: IntComparison | null;
  is_coinbase?: BooleanComparison | null;
  input_count?: IntComparison | null;
  output_count?: IntComparison | null;
  output_value_satoshis?: BigIntComparison | null;
  /** EXISTS an output of this transaction matching all of the filter (one row). */
  has_output?: OutputFilter | null;
  /** EXISTS an input of this transaction matching all of the filter (one row). */
  has_input?: InputFilter | null;
  /** WIN, per node: in a block the node accepts with height in the window, or in its mempool. */
  block_window?: BlockWindow | null;
  /** Per node: in a block the node accepts that matches. */
  in_block?: BlockFilter | null;
  /** Per node: in (true) or not in (false) the node's mempool. */
  in_mempool?: boolean | null;
}

export interface BlockFilter {
  _and?: readonly BlockFilter[] | null;
  _or?: readonly BlockFilter[] | null;
  _not?: BlockFilter | null;
  hash?: HexComparison | null;
  internal_id?: BigIntComparison | null;
  height?: IntComparison | null;
  version?: IntComparison | null;
  timestamp?: BigIntComparison | null;
  previous_block_hash?: HexComparison | null;
  merkle_root?: HexComparison | null;
  bits?: BigIntComparison | null;
  nonce?: BigIntComparison | null;
  size_bytes?: IntComparison | null;
  transaction_count?: IntComparison | null;
  /** EXISTS a transaction in the block matching. */
  has_transaction?: TransactionFilter | null;
}

/** A mempool entry (node_transaction): the node's validation of a transaction. */
export interface NodeTransactionFilter {
  _and?: readonly NodeTransactionFilter[] | null;
  _or?: readonly NodeTransactionFilter[] | null;
  _not?: NodeTransactionFilter | null;
  transaction_internal_id?: BigIntComparison | null;
  transaction_hash?: HexComparison | null;
  validated_at?: TimestampComparison | null;
  transaction?: TransactionFilter | null;
}

/** An accepted block (node_block). */
export interface NodeBlockFilter {
  _and?: readonly NodeBlockFilter[] | null;
  _or?: readonly NodeBlockFilter[] | null;
  _not?: NodeBlockFilter | null;
  block_internal_id?: BigIntComparison | null;
  height?: IntComparison | null;
  accepted_at?: TimestampComparison | null;
  block?: BlockFilter | null;
}

/** A block inclusion of a transaction (per node). */
export interface BlockInclusionFilter {
  _and?: readonly BlockInclusionFilter[] | null;
  _or?: readonly BlockInclusionFilter[] | null;
  _not?: BlockInclusionFilter | null;
  transaction_index?: IntComparison | null;
  block?: BlockFilter | null;
}

/** A transaction of a block. */
export interface BlockTransactionFilter {
  _and?: readonly BlockTransactionFilter[] | null;
  _or?: readonly BlockTransactionFilter[] | null;
  _not?: BlockTransactionFilter | null;
  transaction_index?: IntComparison | null;
  transaction?: TransactionFilter | null;
}

/* ------------------------------------------------------------------ */
/* comparisons                                                          */
/* ------------------------------------------------------------------ */

type HexColumnKind = 'bytes' | 'hash' | 'nullableBytes' | 'tokenCategory';

const hashBytes = 32;
export const zeroCategory = `toFixedString(unhex('${'00'.repeat(
  hashBytes
)}'), 32)`;

const assertHashLength = (hex: string, column: string) => {
  if (hex.length !== hashBytes * 2) {
    throw invalid(`${column} values are 32 bytes (64 hex characters).`);
  }
};

export const all = (parts: readonly string[]) =>
  parts.length === 0 ? '1' : parts.map((part) => `(${part})`).join(' AND ');

export const any = (parts: readonly string[]) =>
  parts.length === 0 ? '0' : parts.map((part) => `(${part})`).join(' OR ');

const given = <T>(value: T | null | undefined): value is T =>
  value !== undefined && value !== null;

/**
 * SQL's three-valued logic for a condition on a nullable column: NULL when
 * the column is NULL (as Postgres, and so Chaingraph v1, evaluates `IN`,
 * comparisons and their negations). ClickHouse's `NULL IN (…)` is 0, not
 * NULL, which would make `_not: { … _in … }` match NULL rows.
 */
export const nullSafe = (column: string, condition: string) =>
  `if(isNull(${column}), NULL, ${condition})`;

/** ClickHouse integer column types and their ranges. */
const integerRanges: { [type: string]: readonly [bigint, bigint] } = {
  Int32: [-(2n ** 31n), 2n ** 31n - 1n],
  Int64: [-(2n ** 63n), 2n ** 63n - 1n],
  UInt32: [0n, 2n ** 32n - 1n],
  UInt64: [0n, 2n ** 64n - 1n],
  UInt8: [0n, 255n],
};

/**
 * Numeric comparisons on a non-null integer column of ClickHouse type
 * `type`. A value outside the column's range is resolved here (e.g.
 * `internal_id > -1` is always true), so binding never fails and the key
 * condition stays on the column's own type.
 */
export const numericComparison = (
  sql: Sql,
  column: string,
  type: string,
  comparison: BigIntComparison | IntComparison,
  nullable = false
) => {
  const [min, max] = integerRanges[type] ?? [-(2n ** 127n), 2n ** 127n - 1n];
  const parts: string[] = [];
  const nullableWrap = (condition: string) =>
    nullable ? nullSafe(column, condition) : condition;
  const value = (raw: bigint | number) => BigInt(raw);
  const bindValue = (raw: bigint) => sql.bind(raw, type);
  if (given(comparison._eq)) {
    const v = value(comparison._eq);
    parts.push(v < min || v > max ? '0' : `${column} = ${bindValue(v)}`);
  }
  if (given(comparison._neq)) {
    const v = value(comparison._neq);
    parts.push(v < min || v > max ? '1' : `${column} != ${bindValue(v)}`);
  }
  const range = (
    key: '_gt' | '_gte' | '_lt' | '_lte',
    operator: string,
    belowMin: '0' | '1',
    aboveMax: '0' | '1'
  ) => {
    const raw = comparison[key];
    if (!given(raw)) return;
    const v = value(raw);
    if (v < min) parts.push(belowMin);
    else if (v > max) parts.push(aboveMax);
    else parts.push(`${column} ${operator} ${bindValue(v)}`);
  };
  range('_gt', '>', '1', '0');
  range('_gte', '>=', '1', '0');
  range('_lt', '<', '0', '1');
  range('_lte', '<=', '0', '1');
  const inRange = (values: readonly (bigint | number)[]) =>
    values.map(value).filter((v) => v >= min && v <= max);
  if (given(comparison._in)) {
    const values = inRange(comparison._in);
    parts.push(
      values.length === 0
        ? '0'
        : nullableWrap(
            any(
              chunks(values).map(
                (chunk) => `${column} IN ${sql.bind(chunk, `Array(${type})`)}`
              )
            )
          )
    );
  }
  if (given(comparison._nin)) {
    const values = inRange(comparison._nin);
    if (values.length > 0) {
      parts.push(
        nullableWrap(
          all(
            chunks(values).map(
              (chunk) => `${column} NOT IN ${sql.bind(chunk, `Array(${type})`)}`
            )
          )
        )
      );
    }
  }
  if (given(comparison._is_null)) {
    parts.push(
      nullable
        ? comparison._is_null
          ? `isNull(${column})`
          : `isNotNull(${column})`
        : comparison._is_null
        ? '0'
        : '1'
    );
  }
  return parts;
};

const isFixed = (kind: HexColumnKind) =>
  kind === 'hash' || kind === 'tokenCategory';

const hexValue = (
  sql: Sql,
  kind: HexColumnKind,
  hex: string,
  label: string
) => {
  if (isFixed(kind)) {
    assertHashLength(hex, label);
    return `toFixedString(unhex(${sql.bind(hex, 'String')}), 32)`;
  }
  return `unhex(${sql.bind(hex, 'String')})`;
};

/** A hex list bound in chunks of at most `maxListChunk` values, as bytes. */
const hexList = (
  sql: Sql,
  kind: HexColumnKind,
  values: readonly string[],
  label: string
) => {
  const fixed = isFixed(kind);
  if (fixed)
    values.forEach((hex) => {
      assertHashLength(hex, label);
    });
  const bound = chunks(values).map((chunk) => sql.bind(chunk, 'Array(String)'));
  /*
   * A subquery set, not `IN arrayMap(…, {param})`: ClickHouse turns the
   * latter into has(), which no primary key or projection can use (chipnet:
   * a full scan of `spend`, 6,491 granules, against 8 for the subquery set).
   */
  return `(SELECT ${
    fixed ? 'toFixedString(unhex(x), 32)' : 'unhex(x)'
  } FROM (SELECT arrayJoin(${
    bound.length === 1 ? bound[0]! : `arrayConcat(${bound.join(', ')})`
  }) AS x))`;
};

/** Byte prefix: `startsWith` on a String column (`column` may be nullable). */
const prefixCondition = (
  sql: Sql,
  kind: HexColumnKind,
  column: string,
  prefix: string
) =>
  `startsWith(${
    kind === 'bytes'
      ? column
      : kind === 'nullableBytes'
      ? `toString(assumeNotNull(${column}))`
      : `toString(${column})`
  }, unhex(${sql.bind(prefix, 'String')}))`;

/**
 * Byte comparisons. v1 semantics (Postgres): a column without a value is
 * NULL (an NFT commitment of an output without an NFT; token_category of an
 * output without tokens, stored here as the zero category), and every value
 * comparison on it is NULL. Under an even number of `_not`s NULL and false
 * both exclude the row, so only conditions that could be true for an absent
 * value get a guard and `_eq`/`_in` stay plain key conditions; under an odd
 * number (`negated`) the exact NULL is produced.
 */
export const hexComparison = (
  sql: Sql,
  column: string,
  kind: HexColumnKind,
  comparison: HexComparison,
  label: string,
  negated = false
) => {
  const parts: string[] = [];
  const absent =
    kind === 'tokenCategory'
      ? `${column} = ${zeroCategory}`
      : kind === 'nullableBytes'
      ? `isNull(${column})`
      : undefined;
  const zeroHex = '00'.repeat(hashBytes);
  const value = (condition: string, plainWhenPositive: boolean) =>
    absent === undefined
      ? condition
      : negated
      ? `if(${absent}, NULL, ${condition})`
      : plainWhenPositive
      ? condition
      : `NOT (${absent}) AND (${condition})`;
  const binary = (
    key: keyof HexComparison,
    operator: string,
    plainWhenPositive: boolean
  ) => {
    const operand = comparison[key];
    if (typeof operand === 'string') {
      parts.push(
        value(
          `${column} ${operator} ${hexValue(sql, kind, operand, label)}`,
          plainWhenPositive && operand.toLowerCase() !== zeroHex
        )
      );
    }
  };
  binary('_eq', '=', true);
  binary('_neq', '!=', false);
  binary('_gt', '>', false);
  binary('_gte', '>=', false);
  binary('_lt', '<', false);
  binary('_lte', '<=', false);
  if (given(comparison._in)) {
    parts.push(
      comparison._in.length === 0
        ? '0'
        : value(
            `${column} IN ${hexList(sql, kind, comparison._in, label)}`,
            !comparison._in.some((hex) => hex.toLowerCase() === zeroHex)
          )
    );
  }
  if (given(comparison._nin) && comparison._nin.length > 0) {
    parts.push(
      value(
        `${column} NOT IN ${hexList(sql, kind, comparison._nin, label)}`,
        false
      )
    );
  }
  if (given(comparison._is_null)) {
    if (absent === undefined) {
      parts.push(comparison._is_null ? '0' : '1');
    } else {
      parts.push(comparison._is_null ? absent : `NOT (${absent})`);
    }
  }
  if (given(comparison._prefix)) {
    parts.push(
      value(prefixCondition(sql, kind, column, comparison._prefix), false)
    );
  }
  if (given(comparison._prefix_in)) {
    parts.push(
      value(
        any(
          comparison._prefix_in.map((prefix) =>
            prefixCondition(sql, kind, column, prefix)
          )
        ),
        false
      )
    );
  }
  return parts;
};

export const capabilityComparison = (
  sql: Sql,
  column: string,
  comparison: CapabilityComparison
) => {
  const parts: string[] = [];
  if (given(comparison._eq)) {
    parts.push(`${column} = ${sql.bind(comparison._eq, 'String')}`);
  }
  if (given(comparison._neq)) {
    parts.push(`${column} != ${sql.bind(comparison._neq, 'String')}`);
  }
  if (given(comparison._in)) {
    parts.push(
      nullSafe(
        column,
        `toString(${column}) IN ${sql.bind(comparison._in, 'Array(String)')}`
      )
    );
  }
  if (given(comparison._nin)) {
    parts.push(
      nullSafe(
        column,
        `toString(${column}) NOT IN ${sql.bind(
          comparison._nin,
          'Array(String)'
        )}`
      )
    );
  }
  if (given(comparison._is_null)) {
    parts.push(
      comparison._is_null ? `isNull(${column})` : `isNotNull(${column})`
    );
  }
  return parts;
};

export const booleanComparison = (
  sql: Sql,
  column: string,
  comparison: BooleanComparison
) => {
  const parts: string[] = [];
  if (given(comparison._eq)) {
    parts.push(`${column} = ${sql.bind(comparison._eq ? 1 : 0, 'UInt8')}`);
  }
  if (given(comparison._neq)) {
    parts.push(`${column} != ${sql.bind(comparison._neq ? 1 : 0, 'UInt8')}`);
  }
  return parts;
};

const parseTimestamp = (value: string, label: string) => {
  const ms = Date.parse(value);
  if (Number.isNaN(ms)) throw invalid(`${label}: not an ISO-8601 timestamp.`);
  return BigInt(ms);
};

export const timestampComparison = (
  sql: Sql,
  column: string,
  comparison: TimestampComparison,
  label: string
) => {
  const parts: string[] = [];
  const operators: [keyof TimestampComparison, string][] = [
    ['_eq', '='],
    ['_neq', '!='],
    ['_gt', '>'],
    ['_gte', '>='],
    ['_lt', '<'],
    ['_lte', '<='],
  ];
  operators.forEach(([key, operator]) => {
    const value = comparison[key];
    if (typeof value === 'string') {
      parts.push(
        `${column} ${operator} fromUnixTimestamp64Milli(${sql.bind(
          parseTimestamp(value, label),
          'Int64'
        )}, 'UTC')`
      );
    }
  });
  if (given(comparison._is_null)) {
    parts.push(
      comparison._is_null ? `isNull(${column})` : `isNotNull(${column})`
    );
  }
  return parts;
};

/* ------------------------------------------------------------------ */
/* spend relation (spend-index.md, read rule)                           */
/* ------------------------------------------------------------------ */

/**
 * Columns of an output-like row: an `output_at` row, or a spend-relation row
 * (whose spent output's key is the outpoint; its own `transaction_hash` is
 * the spending transaction's).
 */
export interface OutputColumns {
  transaction_hash: string;
  output_index: string;
  /** Undefined: the row has no spent-output transaction id (spend relation). */
  transaction_internal_id: string | undefined;
}

export const outputRowColumns: OutputColumns = {
  output_index: 'output_index',
  transaction_hash: 'transaction_hash',
  transaction_internal_id: 'transaction_internal_id',
};

/** A spend-relation row: the spent output's key is the outpoint. */
export const spentOutputColumns: OutputColumns = {
  output_index: 'outpoint_index',
  transaction_hash: 'outpoint_transaction_hash',
  transaction_internal_id: undefined,
};

const zeroHashSql = `toFixedString(unhex(repeat('00', 32)), 32)`;

const spendRelationColumns =
  'transaction_hash, input_index, transaction_internal_id, outpoint_transaction_hash, outpoint_index, value_satoshis, ' +
  'locking_bytecode, locking_bytecode_prefix, token_category, fungible_token_amount, nonfungible_token_capability, ' +
  'nonfungible_token_commitment, nonfungible_token_commitment_key';

const spentOutputSelect =
  'transaction_hash, output_index, value_satoshis, locking_bytecode, locking_bytecode_prefix, token_category, ' +
  'fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment, nonfungible_token_commitment_key';

const joinedSpendColumns = `i.transaction_hash AS transaction_hash, i.input_index AS input_index,
      i.transaction_internal_id AS transaction_internal_id,
      i.outpoint_transaction_hash AS outpoint_transaction_hash, i.outpoint_index AS outpoint_index,
      o.value_satoshis AS value_satoshis, o.locking_bytecode AS locking_bytecode,
      o.locking_bytecode_prefix AS locking_bytecode_prefix, o.token_category AS token_category,
      o.fungible_token_amount AS fungible_token_amount,
      o.nonfungible_token_capability AS nonfungible_token_capability,
      o.nonfungible_token_commitment AS nonfungible_token_commitment,
      o.nonfungible_token_commitment_key AS nonfungible_token_commitment_key`;

/**
 * The spend relation at the request's snapshot: one row per visible input
 * whose spent output is visible, with the input key, the outpoint and the
 * spent output's attributes (columns `spendRelationColumns`), filtered by
 * `predicate` (over those columns). The read rule
 * (docs/clickhouse-port/spend-index.md): with W the snapshot's spend
 * watermark (`{spend}`), a pair (input seq i, output seq o) is
 * - in the spend index (`spend_at`) iff both seqs are at most W;
 * - else an input above W joined to its output (any seq), or
 * - an input at or below W spending an output above W.
 * The three parts are disjoint and cover every visible pair, so the result is
 * exact at the snapshot whatever the index's lag; only the two tail parts
 * (recent commits) use the join.
 */
export const spendRelationSql = (sql: Sql, predicate: string) => {
  const input = sql.view('input_at');
  const output = sql.view('output_at');
  const index = `SELECT ${spendRelationColumns} FROM ${sql.view(
    'spend_at'
  )} WHERE ${predicate}`;
  /*
   * Caught up (spend watermark = visible(0), no committed tail): no visible
   * input or output has a seq above W, so both tail parts are empty and are
   * left out (they cost ~15 ms of planning on chipnet).
   */
  if (
    sql.snapshot.spend >= sql.snapshot.visible0 &&
    sql.snapshot.committedTail.length === 0
  ) {
    return `(${index})`;
  }
  return `(${index}
    UNION ALL
    SELECT ${spendRelationColumns} FROM (
      SELECT ${joinedSpendColumns}
      FROM (SELECT transaction_hash, input_index, transaction_internal_id, outpoint_transaction_hash, outpoint_index FROM ${input}
            WHERE commit_seq > {spend:UInt64} AND outpoint_transaction_hash != ${zeroHashSql}) AS i
      INNER JOIN (SELECT ${spentOutputSelect} FROM ${output}
                  WHERE (transaction_hash, output_index) IN (
                    SELECT outpoint_transaction_hash, outpoint_index FROM ${input}
                    WHERE commit_seq > {spend:UInt64})) AS o
        ON o.transaction_hash = i.outpoint_transaction_hash AND o.output_index = i.outpoint_index)
    WHERE ${predicate}
    UNION ALL
    SELECT ${spendRelationColumns} FROM (
      SELECT ${joinedSpendColumns}
      FROM (SELECT transaction_hash, input_index, transaction_internal_id, outpoint_transaction_hash, outpoint_index FROM ${input}
            WHERE commit_seq <= {spend:UInt64}
              AND (outpoint_transaction_hash, outpoint_index) IN (
                SELECT transaction_hash, output_index FROM ${output} WHERE commit_seq > {spend:UInt64})) AS i
      INNER JOIN (SELECT ${spentOutputSelect} FROM ${output} WHERE commit_seq > {spend:UInt64}) AS o
        ON o.transaction_hash = i.outpoint_transaction_hash AND o.output_index = i.outpoint_index)
    WHERE ${predicate})`;
};

/* ------------------------------------------------------------------ */
/* row filters                                                          */
/* ------------------------------------------------------------------ */

/**
 * Redundant key conditions next to a locking bytecode filter: the same
 * filter on the 25-byte prefix the sort keys use (`output`'s p_script,
 * `spend`'s primary key), so a range of the key is read, not a scan.
 */
const lockingBytecodePrefixKey = (sql: Sql, comparison: HexComparison) => {
  const parts: string[] = [];
  if (given(comparison._eq)) {
    parts.push(
      `locking_bytecode_prefix = substring(unhex(${sql.bind(
        comparison._eq,
        'String'
      )}), 1, 25)`
    );
  }
  if (given(comparison._in) && comparison._in.length > 0) {
    const bound = chunks(comparison._in).map((chunk) =>
      sql.bind(chunk, 'Array(String)')
    );
    parts.push(
      `locking_bytecode_prefix IN (SELECT substring(unhex(x), 1, 25) FROM (SELECT arrayJoin(${
        bound.length === 1 ? bound[0]! : `arrayConcat(${bound.join(', ')})`
      }) AS x))`
    );
  }
  const prefixKey = (prefix: string) =>
    `startsWith(locking_bytecode_prefix, substring(unhex(${sql.bind(
      prefix,
      'String'
    )}), 1, 25))`;
  if (given(comparison._prefix)) parts.push(prefixKey(comparison._prefix));
  if (given(comparison._prefix_in) && comparison._prefix_in.length > 0) {
    parts.push(any(comparison._prefix_in.map(prefixKey)));
  }
  return parts;
};

/** The same for an NFT commitment prefix: `nonfungible_token_commitment_key` (p_category). */
const commitmentKeyCondition = (sql: Sql, comparison: HexComparison) => {
  const key = (prefix: string) =>
    `startsWith(nonfungible_token_commitment_key, unhex(${sql.bind(
      prefix,
      'String'
    )}))`;
  return [
    ...(given(comparison._prefix) ? [key(comparison._prefix)] : []),
    ...(given(comparison._prefix_in) && comparison._prefix_in.length > 0
      ? [any(comparison._prefix_in.map(key))]
      : []),
  ];
};

/** Whether an output filter can be evaluated on a spend-relation row. */
export const spendRowEvaluable = (
  filter: OutputFilter | null | undefined
): boolean => {
  if (!given(filter)) return true;
  if (
    given(filter.transaction_internal_id) ||
    given(filter.transaction) ||
    given(filter.spent_by)
  ) {
    return false;
  }
  return (
    (filter._and ?? []).every(spendRowEvaluable) &&
    (filter._or ?? []).every(spendRowEvaluable) &&
    spendRowEvaluable(filter._not)
  );
};

/**
 * An `OutputFilter` as a predicate on one output-like row (`output_at`, or a
 * spend-relation row with `spentOutputColumns`). `node`: the node of per-node
 * sub-filters (`spent_by`, the transaction's `in_block` …).
 */
export const outputPredicate = (
  sql: Sql,
  filter: OutputFilter | null | undefined,
  columns: OutputColumns = outputRowColumns,
  node?: number,
  negated = false
): string => {
  if (!given(filter)) return '1';
  const parts: string[] = [];
  if (given(filter.transaction_hash)) {
    parts.push(
      ...hexComparison(
        sql,
        columns.transaction_hash,
        'hash',
        filter.transaction_hash,
        'transaction_hash'
      )
    );
  }
  if (given(filter.transaction_internal_id)) {
    if (columns.transaction_internal_id === undefined) {
      throw new Error('transaction_internal_id is not on this row.');
    }
    parts.push(
      ...numericComparison(
        sql,
        columns.transaction_internal_id,
        'UInt64',
        filter.transaction_internal_id
      )
    );
  }
  if (given(filter.output_index)) {
    parts.push(
      ...numericComparison(
        sql,
        columns.output_index,
        'UInt32',
        filter.output_index
      )
    );
  }
  if (given(filter.value_satoshis)) {
    parts.push(
      ...numericComparison(
        sql,
        'value_satoshis',
        'Int64',
        filter.value_satoshis
      )
    );
  }
  if (given(filter.fungible_token_amount)) {
    parts.push(
      ...numericComparison(
        sql,
        'fungible_token_amount',
        'Int64',
        filter.fungible_token_amount,
        true
      )
    );
  }
  if (given(filter.locking_bytecode)) {
    parts.push(
      ...hexComparison(
        sql,
        'locking_bytecode',
        'bytes',
        filter.locking_bytecode,
        'locking_bytecode'
      ),
      ...lockingBytecodePrefixKey(sql, filter.locking_bytecode)
    );
  }
  if (given(filter.token_category)) {
    parts.push(
      ...hexComparison(
        sql,
        'token_category',
        'tokenCategory',
        filter.token_category,
        'token_category',
        negated
      )
    );
  }
  if (given(filter.nonfungible_token_capability)) {
    parts.push(
      ...capabilityComparison(
        sql,
        'nonfungible_token_capability',
        filter.nonfungible_token_capability
      )
    );
  }
  if (given(filter.nonfungible_token_commitment)) {
    parts.push(
      ...hexComparison(
        sql,
        'nonfungible_token_commitment',
        'nullableBytes',
        filter.nonfungible_token_commitment,
        'nonfungible_token_commitment',
        negated
      ),
      /* a key condition, equivalent to the comparison only where NULL and false both exclude */
      ...(negated
        ? []
        : commitmentKeyCondition(sql, filter.nonfungible_token_commitment))
    );
  }
  if (given(filter.transaction)) {
    parts.push(
      `${columns.transaction_hash} IN (SELECT hash FROM ${sql.view(
        'transaction_at'
      )} WHERE ${transactionPredicate(sql, filter.transaction, node)})`
    );
  }
  if (given(filter.spent_by)) {
    parts.push(
      `(${columns.transaction_hash}, ${
        columns.output_index
      }) IN (${sql.acceptedRows(
        inputRowsSql(sql, filter.spent_by, node),
        'outpoint_transaction_hash, outpoint_index',
        node
      )})`
    );
  }
  if (given(filter._and)) {
    parts.push(
      ...filter._and.map((f) => outputPredicate(sql, f, columns, node, negated))
    );
  }
  if (given(filter._or)) {
    parts.push(
      any(
        filter._or.map((f) => outputPredicate(sql, f, columns, node, negated))
      )
    );
  }
  if (given(filter._not)) {
    parts.push(
      `NOT (${outputPredicate(sql, filter._not, columns, node, !negated)})`
    );
  }
  return all(parts);
};

/** Whether an input filter can be evaluated on a spend-relation row. */
const inputSpendEvaluable = (filter: InputFilter | null | undefined): boolean =>
  !given(filter) ||
  (!given(filter.sequence_number) &&
    spendRowEvaluable(filter.outpoint) &&
    (filter._and ?? []).every(inputSpendEvaluable) &&
    (filter._or ?? []).every(inputSpendEvaluable) &&
    inputSpendEvaluable(filter._not));

/**
 * Whether every input matching `filter` has a stored spent output (the filter
 * requires an `outpoint` match: positively, in a conjunction, or in every
 * branch of a disjunction) and the whole filter can be evaluated on a
 * spend-relation row. Such a filter is answered from the spend relation alone
 * (index + tail); any other one that mentions `outpoint` (under `_not`, in
 * some `_or` branches, or with sub-filters the spend row lacks) is evaluated
 * on input rows with the outpoint semi-join.
 */
export const requiresOutpoint = (filter: InputFilter): boolean => {
  const requires = (f: InputFilter): boolean =>
    given(f.outpoint) ||
    (f._and ?? []).some(requires) ||
    (given(f._or) && f._or.length > 0 && f._or.every(requires));
  return requires(filter) && inputSpendEvaluable(filter);
};

/** Predicates on an input's own columns (common to input rows and spend rows). */
const inputOwnColumns = (sql: Sql, filter: InputFilter, node?: number) => {
  const parts: string[] = [];
  if (given(filter.transaction_hash)) {
    parts.push(
      ...hexComparison(
        sql,
        'transaction_hash',
        'hash',
        filter.transaction_hash,
        'transaction_hash'
      )
    );
  }
  if (given(filter.transaction_internal_id)) {
    parts.push(
      ...numericComparison(
        sql,
        'transaction_internal_id',
        'UInt64',
        filter.transaction_internal_id
      )
    );
  }
  if (given(filter.input_index)) {
    parts.push(
      ...numericComparison(sql, 'input_index', 'UInt32', filter.input_index)
    );
  }
  if (given(filter.outpoint_transaction_hash)) {
    parts.push(
      ...hexComparison(
        sql,
        'outpoint_transaction_hash',
        'hash',
        filter.outpoint_transaction_hash,
        'outpoint_transaction_hash'
      )
    );
  }
  if (given(filter.outpoint_index)) {
    parts.push(
      ...numericComparison(
        sql,
        'outpoint_index',
        'UInt32',
        filter.outpoint_index
      )
    );
  }
  if (given(filter.transaction)) {
    parts.push(
      `transaction_hash IN (SELECT hash FROM ${sql.view(
        'transaction_at'
      )} WHERE ${transactionPredicate(sql, filter.transaction, node)})`
    );
  }
  return parts;
};

/**
 * An `InputFilter` as a predicate on one spend-relation row (the input's own
 * fields and the spent output's attributes on the same row).
 */
export const spendRowPredicate = (
  sql: Sql,
  filter: InputFilter | null | undefined,
  node?: number,
  negated = false
): string => {
  if (!given(filter)) return '1';
  const parts = inputOwnColumns(sql, filter, node);
  if (given(filter.outpoint)) {
    /*
     * v1: EXISTS a spent output matching, so true or false, never NULL. Under
     * an even number of `_not`s a NULL here excludes the row as false would,
     * and the plain predicate keeps the spend index key conditions; under an
     * odd number it must be false.
     */
    const predicate = outputPredicate(
      sql,
      filter.outpoint,
      spentOutputColumns,
      node
    );
    parts.push(negated ? `ifNull((${predicate}), 0)` : predicate);
  }
  if (given(filter._and)) {
    parts.push(
      ...filter._and.map((f) => spendRowPredicate(sql, f, node, negated))
    );
  }
  if (given(filter._or)) {
    parts.push(
      any(filter._or.map((f) => spendRowPredicate(sql, f, node, negated)))
    );
  }
  if (given(filter._not)) {
    parts.push(`NOT (${spendRowPredicate(sql, filter._not, node, !negated)})`);
  }
  return all(parts);
};

/**
 * An `InputFilter` as a predicate on one input_at row.
 * - A filter that requires a spent output (`requiresOutpoint`): the input's
 *   key is in the spend relation filtered by it (`spendRelationSql`: the
 *   spend index up to the snapshot's spend watermark, the join above it).
 * - Otherwise `outpoint` sub-filters are a semi-join on the row's outpoint:
 *   the outpoint is the key of a matching output (a coinbase input, or one
 *   whose spent output is not stored, matches no `outpoint` filter, as in
 *   Hasura v1).
 */
export const inputPredicate = (
  sql: Sql,
  filter: InputFilter | null | undefined,
  node?: number,
  among?: string
): string => {
  if (!given(filter)) return '1';
  if (among === undefined && requiresOutpoint(filter)) {
    return `(transaction_hash, input_index) IN (SELECT transaction_hash, input_index FROM ${spendRelationSql(
      sql,
      spendRowPredicate(sql, filter, node)
    )})`;
  }
  return inputRowPredicate(sql, filter, node, among);
};

/**
 * `among`: a subquery of the outpoints of the rows being filtered (a nested
 * relationship's own input rows). The `outpoint` semi-join then reads only
 * those outputs by primary key instead of every output matching the filter.
 */
const inputRowPredicate = (
  sql: Sql,
  filter: InputFilter,
  node?: number,
  among?: string
): string => {
  const parts = inputOwnColumns(sql, filter, node);
  if (given(filter.sequence_number)) {
    parts.push(
      ...numericComparison(
        sql,
        'sequence_number',
        'UInt32',
        filter.sequence_number
      )
    );
  }
  if (given(filter.outpoint)) {
    parts.push(
      `(outpoint_transaction_hash, outpoint_index) IN (SELECT transaction_hash, output_index FROM ${sql.view(
        'output_at'
      )} WHERE ${
        among === undefined
          ? ''
          : `(transaction_hash, output_index) IN (${among}) AND `
      }${outputPredicate(sql, filter.outpoint, outputRowColumns, node)})`
    );
  }
  if (given(filter._and)) {
    parts.push(...filter._and.map((f) => inputPredicate(sql, f, node, among)));
  }
  if (given(filter._or)) {
    parts.push(any(filter._or.map((f) => inputPredicate(sql, f, node, among))));
  }
  if (given(filter._not)) {
    parts.push(`NOT (${inputPredicate(sql, filter._not, node, among)})`);
  }
  return all(parts);
};

/**
 * Input rows matching `filter` (columns transaction_hash, input_index,
 * outpoint_transaction_hash, outpoint_index): from the spend relation when
 * the filter requires a spent output, else from `input_at`.
 */
export const inputRowsSql = (
  sql: Sql,
  filter: InputFilter | null | undefined,
  node?: number
) =>
  given(filter) && requiresOutpoint(filter)
    ? `SELECT transaction_hash, input_index, outpoint_transaction_hash, outpoint_index FROM ${spendRelationSql(
        sql,
        spendRowPredicate(sql, filter, node)
      )}`
    : `SELECT transaction_hash, input_index, outpoint_transaction_hash, outpoint_index FROM ${sql.view(
        'input_at'
      )} WHERE ${inputPredicate(sql, filter, node)}`;

/** Block columns of a `BlockFilter` on a block_at row. */
export const blockPredicate = (
  sql: Sql,
  filter: BlockFilter | null | undefined,
  node?: number
): string => {
  if (!given(filter)) return '1';
  const parts: string[] = [];
  if (given(filter.hash)) {
    parts.push(...hexComparison(sql, 'hash', 'hash', filter.hash, 'hash'));
  }
  if (given(filter.previous_block_hash)) {
    parts.push(
      ...hexComparison(
        sql,
        'previous_block_hash',
        'hash',
        filter.previous_block_hash,
        'previous_block_hash'
      )
    );
  }
  if (given(filter.merkle_root)) {
    parts.push(
      ...hexComparison(
        sql,
        'merkle_root',
        'hash',
        filter.merkle_root,
        'merkle_root'
      )
    );
  }
  const numeric: [keyof BlockFilter, string][] = [
    ['internal_id', 'UInt64'],
    ['height', 'UInt32'],
    ['version', 'Int32'],
    ['timestamp', 'UInt32'],
    ['bits', 'UInt32'],
    ['nonce', 'UInt32'],
    ['size_bytes', 'UInt32'],
    ['transaction_count', 'UInt32'],
  ];
  numeric.forEach(([key, type]) => {
    const comparison = filter[key] as IntComparison | null | undefined;
    if (given(comparison)) {
      parts.push(...numericComparison(sql, key, type, comparison));
    }
  });
  if (given(filter.has_transaction)) {
    parts.push(
      `internal_id IN (SELECT block_internal_id FROM ${sql.view(
        'block_transaction_at'
      )} WHERE transaction_hash IN (SELECT hash FROM ${sql.view(
        'transaction_at'
      )} WHERE ${transactionPredicate(sql, filter.has_transaction, node)}))`
    );
  }
  if (given(filter._and)) {
    parts.push(...filter._and.map((f) => blockPredicate(sql, f, node)));
  }
  if (given(filter._or)) {
    parts.push(any(filter._or.map((f) => blockPredicate(sql, f, node))));
  }
  if (given(filter._not)) {
    parts.push(`NOT (${blockPredicate(sql, filter._not, node)})`);
  }
  return all(parts);
};

/** True when a block filter only constrains `height` (served by tx_acceptance's height column). */
const heightOnly = (filter: BlockFilter): boolean =>
  Object.entries(filter).every(
    ([key, value]) =>
      !given(value) ||
      key === 'height' ||
      (key === '_and' && (value as BlockFilter[]).every(heightOnly)) ||
      (key === '_or' && (value as BlockFilter[]).every(heightOnly)) ||
      (key === '_not' && heightOnly(value as BlockFilter))
  );

/** A height-only block filter as a predicate on tx_acceptance / node_block rows (`height`). */
const heightPredicate = (sql: Sql, filter: BlockFilter): string =>
  all([
    ...(given(filter.height)
      ? numericComparison(sql, 'height', 'UInt32', filter.height)
      : []),
    ...(filter._and ?? []).map((f) => heightPredicate(sql, f)),
    ...(given(filter._or)
      ? [any(filter._or.map((f) => heightPredicate(sql, f)))]
      : []),
    ...(given(filter._not)
      ? [`NOT (${heightPredicate(sql, filter._not)})`]
      : []),
  ]);

/**
 * `block_internal_id` of blocks the node accepts that match `filter`, as a
 * predicate on a row with `block_internal_id` and `height` (tx_acceptance or
 * node_block): a height-only filter reads the row's own height.
 */
export const acceptedBlockCondition = (
  sql: Sql,
  filter: BlockFilter,
  node?: number
) =>
  heightOnly(filter)
    ? heightPredicate(sql, filter)
    : `block_internal_id IN (SELECT internal_id FROM ${sql.view(
        'block_at'
      )} WHERE ${blockPredicate(sql, filter, node)})`;

/**
 * A `TransactionFilter` as a predicate on a transaction_at row (`hash`,
 * `internal_id`, …). Relationship filters are semi-joins on the pinned views
 * (EXISTS = `hash IN (…)`, NOT EXISTS = `NOT (hash IN (…))`). `node`: the
 * node of the per-node parts (`block_window`, `in_block`, `in_mempool`, and
 * acceptance inside `spent_by`); default the statement's primary node.
 */
export const transactionPredicate = (
  sql: Sql,
  filter: TransactionFilter | null | undefined,
  node?: number,
  among?: string
): string => {
  /*
   * `among`: a subquery of transaction hashes every result row is among (the
   * root's candidates; the root's WHERE has `hash IN (among)` too). Each
   * semi-join below then reads only those transactions' rows by key: safe
   * under any negation, since a row outside `among` is excluded anyway.
   */
  const restrict = (column: string) =>
    among === undefined ? '' : `${column} IN (${among}) AND `;
  if (!given(filter)) return '1';
  const parts: string[] = [];
  if (given(filter.internal_id)) {
    parts.push(
      ...numericComparison(sql, 'internal_id', 'UInt64', filter.internal_id)
    );
  }
  if (given(filter.hash)) {
    parts.push(...hexComparison(sql, 'hash', 'hash', filter.hash, 'hash'));
  }
  const numeric: [keyof TransactionFilter, string][] = [
    ['version', 'Int32'],
    ['locktime', 'UInt32'],
    ['size_bytes', 'UInt32'],
    ['input_count', 'UInt32'],
    ['output_count', 'UInt32'],
    ['output_value_satoshis', 'Int64'],
  ];
  numeric.forEach(([key, type]) => {
    const comparison = filter[key] as IntComparison | null | undefined;
    if (given(comparison)) {
      parts.push(...numericComparison(sql, key, type, comparison));
    }
  });
  if (given(filter.is_coinbase)) {
    parts.push(...booleanComparison(sql, 'is_coinbase', filter.is_coinbase));
  }
  if (given(filter.has_output)) {
    parts.push(
      `hash IN (SELECT transaction_hash FROM ${sql.view(
        'output_at'
      )} WHERE ${restrict('transaction_hash')}${outputPredicate(
        sql,
        filter.has_output,
        outputRowColumns,
        node
      )})`
    );
  }
  if (given(filter.has_input)) {
    /*
     * A filter requiring a spent output reads the spend relation alone (no
     * `input` rows): an address's or a category's spending transactions.
     */
    parts.push(
      requiresOutpoint(filter.has_input)
        ? `hash IN (SELECT transaction_hash FROM ${spendRelationSql(
            sql,
            `${restrict('transaction_hash')}(${spendRowPredicate(
              sql,
              filter.has_input,
              node
            )})`
          )})`
        : `hash IN (SELECT transaction_hash FROM ${sql.view(
            'input_at'
          )} WHERE ${restrict('transaction_hash')}${inputPredicate(
            sql,
            filter.has_input,
            node
          )})`
    );
  }
  if (given(filter.block_window)) {
    const window = filter.block_window;
    const heights: string[] = ['block_internal_id != 0'];
    if (given(window.from_height)) {
      heights.push(
        ...numericComparison(sql, 'height', 'UInt32', {
          _gte: window.from_height,
        })
      );
    }
    if (given(window.to_height)) {
      heights.push(
        ...numericComparison(sql, 'height', 'UInt32', {
          _lt: window.to_height,
        })
      );
    }
    const mempool = window.include_mempool !== false;
    parts.push(
      `hash IN (SELECT transaction_hash FROM ${sql.nodeView(
        'tx_acceptance_at',
        node
      )} WHERE ${restrict('transaction_hash')}(${
        mempool ? any([all(heights), 'block_internal_id = 0']) : all(heights)
      }))`
    );
  }
  if (given(filter.in_block)) {
    parts.push(
      `hash IN (SELECT transaction_hash FROM ${sql.nodeView(
        'tx_acceptance_at',
        node
      )} WHERE ${restrict(
        'transaction_hash'
      )}block_internal_id != 0 AND (${acceptedBlockCondition(
        sql,
        filter.in_block,
        node
      )}))`
    );
  }
  if (given(filter.in_mempool)) {
    const inMempool = `hash IN (SELECT transaction_hash FROM ${sql.nodeView(
      'node_transaction_at',
      node
    )}${among === undefined ? '' : ` WHERE transaction_hash IN (${among})`})`;
    parts.push(filter.in_mempool ? inMempool : `NOT (${inMempool})`);
  }
  if (given(filter._and)) {
    parts.push(
      ...filter._and.map((f) => transactionPredicate(sql, f, node, among))
    );
  }
  if (given(filter._or)) {
    parts.push(
      any(filter._or.map((f) => transactionPredicate(sql, f, node, among)))
    );
  }
  if (given(filter._not)) {
    parts.push(`NOT (${transactionPredicate(sql, filter._not, node, among)})`);
  }
  return all(parts);
};

/** A `NodeTransactionFilter` on a node_transaction_at row. */
export const nodeTransactionPredicate = (
  sql: Sql,
  filter: NodeTransactionFilter | null | undefined,
  node?: number
): string => {
  if (!given(filter)) return '1';
  const parts: string[] = [];
  if (given(filter.transaction_internal_id)) {
    parts.push(
      ...numericComparison(
        sql,
        'transaction_internal_id',
        'UInt64',
        filter.transaction_internal_id
      )
    );
  }
  if (given(filter.transaction_hash)) {
    parts.push(
      ...hexComparison(
        sql,
        'transaction_hash',
        'hash',
        filter.transaction_hash,
        'transaction_hash'
      )
    );
  }
  if (given(filter.validated_at)) {
    parts.push(
      ...timestampComparison(
        sql,
        'validated_at',
        filter.validated_at,
        'validated_at'
      )
    );
  }
  if (given(filter.transaction)) {
    parts.push(
      `transaction_hash IN (SELECT hash FROM ${sql.view(
        'transaction_at'
      )} WHERE hash IN (SELECT transaction_hash FROM ${sql.nodeView(
        'node_transaction_at',
        node
      )}) AND ${transactionPredicate(sql, filter.transaction, node)})`
    );
  }
  if (given(filter._and)) {
    parts.push(
      ...filter._and.map((f) => nodeTransactionPredicate(sql, f, node))
    );
  }
  if (given(filter._or)) {
    parts.push(
      any(filter._or.map((f) => nodeTransactionPredicate(sql, f, node)))
    );
  }
  if (given(filter._not)) {
    parts.push(`NOT (${nodeTransactionPredicate(sql, filter._not, node)})`);
  }
  return all(parts);
};

/** A `NodeBlockFilter` on a node_block_at row. */
export const nodeBlockPredicate = (
  sql: Sql,
  filter: NodeBlockFilter | null | undefined,
  node?: number
): string => {
  if (!given(filter)) return '1';
  const parts: string[] = [];
  if (given(filter.block_internal_id)) {
    parts.push(
      ...numericComparison(
        sql,
        'block_internal_id',
        'UInt64',
        filter.block_internal_id
      )
    );
  }
  if (given(filter.height)) {
    parts.push(...numericComparison(sql, 'height', 'UInt32', filter.height));
  }
  if (given(filter.accepted_at)) {
    parts.push(
      ...timestampComparison(
        sql,
        'accepted_at',
        filter.accepted_at,
        'accepted_at'
      )
    );
  }
  if (given(filter.block)) {
    parts.push(acceptedBlockCondition(sql, filter.block, node));
  }
  if (given(filter._and)) {
    parts.push(...filter._and.map((f) => nodeBlockPredicate(sql, f, node)));
  }
  if (given(filter._or)) {
    parts.push(any(filter._or.map((f) => nodeBlockPredicate(sql, f, node))));
  }
  if (given(filter._not)) {
    parts.push(`NOT (${nodeBlockPredicate(sql, filter._not, node)})`);
  }
  return all(parts);
};

/** A `BlockInclusionFilter` on a block-inclusion row (block_internal_id, height, transaction_index). */
export const blockInclusionPredicate = (
  sql: Sql,
  filter: BlockInclusionFilter | null | undefined,
  node?: number
): string => {
  if (!given(filter)) return '1';
  const parts: string[] = [];
  if (given(filter.transaction_index)) {
    parts.push(
      ...numericComparison(
        sql,
        'transaction_index',
        'UInt32',
        filter.transaction_index
      )
    );
  }
  if (given(filter.block)) {
    parts.push(acceptedBlockCondition(sql, filter.block, node));
  }
  if (given(filter._and)) {
    parts.push(
      ...filter._and.map((f) => blockInclusionPredicate(sql, f, node))
    );
  }
  if (given(filter._or)) {
    parts.push(
      any(filter._or.map((f) => blockInclusionPredicate(sql, f, node)))
    );
  }
  if (given(filter._not)) {
    parts.push(`NOT (${blockInclusionPredicate(sql, filter._not, node)})`);
  }
  return all(parts);
};

/** A `BlockTransactionFilter` on a block_transaction_at row. */
export const blockTransactionPredicate = (
  sql: Sql,
  filter: BlockTransactionFilter | null | undefined,
  node?: number
): string => {
  if (!given(filter)) return '1';
  const parts: string[] = [];
  if (given(filter.transaction_index)) {
    parts.push(
      ...numericComparison(
        sql,
        'transaction_index',
        'UInt32',
        filter.transaction_index
      )
    );
  }
  if (given(filter.transaction)) {
    parts.push(
      `transaction_hash IN (SELECT hash FROM ${sql.view(
        'transaction_at'
      )} WHERE ${transactionPredicate(sql, filter.transaction, node)})`
    );
  }
  if (given(filter._and)) {
    parts.push(
      ...filter._and.map((f) => blockTransactionPredicate(sql, f, node))
    );
  }
  if (given(filter._or)) {
    parts.push(
      any(filter._or.map((f) => blockTransactionPredicate(sql, f, node)))
    );
  }
  if (given(filter._not)) {
    parts.push(`NOT (${blockTransactionPredicate(sql, filter._not, node)})`);
  }
  return all(parts);
};

/* ------------------------------------------------------------------ */
/* candidates: restricting acceptance checks                            */
/* ------------------------------------------------------------------ */

const hasSelectiveHex = (comparison: HexComparison | null | undefined) =>
  given(comparison) &&
  (given(comparison._eq) ||
    given(comparison._in) ||
    given(comparison._prefix) ||
    given(comparison._prefix_in));

/** Whether an output filter narrows the outputs by an indexed column (script, category, transaction). */
export const outputFilterIsSelective = (
  filter: OutputFilter | null | undefined
): boolean =>
  given(filter) &&
  (hasSelectiveHex(filter.locking_bytecode) ||
    hasSelectiveHex(filter.token_category) ||
    hasSelectiveHex(filter.transaction_hash) ||
    (filter._and ?? []).some(outputFilterIsSelective) ||
    (given(filter._or) &&
      filter._or.length > 0 &&
      filter._or.every(outputFilterIsSelective)));

/**
 * A subquery of candidate transaction hashes that every transaction matching
 * `filter` is among (a positive, selective conjunct), or undefined when the
 * filter has none. Used to restrict the acceptance check: reading the node's
 * acceptance rows of a few thousand candidates by key costs a fraction of
 * reading all of them (chipnet: 95 ms vs 420 ms for a busy category).
 */
/** Positive output-side candidate sets of a filter and its `_and` parts (index reads by script, category, hash). */
const outputSideCandidates = (
  sql: Sql,
  filter: TransactionFilter,
  node?: number
): string[] => [
  ...(hasSelectiveHex(filter.hash) && !given(filter.hash?._prefix)
    ? [
        `SELECT hash FROM ${sql.view('transaction_at')} WHERE ${all(
          hexComparison(sql, 'hash', 'hash', filter.hash!, 'hash')
        )}`,
      ]
    : []),
  ...(outputFilterIsSelective(filter.has_output)
    ? [
        `SELECT transaction_hash FROM ${sql.view(
          'output_at'
        )} WHERE ${outputPredicate(
          sql,
          filter.has_output,
          outputRowColumns,
          node
        )}`,
      ]
    : []),
  ...(filter._and ?? []).flatMap((part) =>
    given(part) ? outputSideCandidates(sql, part, node) : []
  ),
];

export const transactionCandidates = (
  sql: Sql,
  filter: TransactionFilter | null | undefined,
  node?: number
): string | undefined => {
  if (!given(filter)) return undefined;
  /*
   * Hash lists and output conjuncts (`has_output` by script, category,
   * transaction) are cheap index reads: all of them are intersected (chipnet
   * L1: 1,441 candidates of one conjunct, 7 of both, so the acceptance reads
   * shrink accordingly). Otherwise the first positive selective conjunct.
   * Spend-relation conjuncts are not intersected: one can be a large
   * category (144k spends) that the semi-join reads restricted anyway.
   */
  const sets = outputSideCandidates(sql, filter, node);
  if (sets.length === 1) return sets[0];
  if (sets.length > 1) {
    return `SELECT * FROM (${sets
      .map((set) => `(${set})`)
      .join(' INTERSECT ')})`;
  }
  if (given(filter.has_input) && requiresOutpoint(filter.has_input)) {
    return `SELECT transaction_hash FROM ${spendRelationSql(
      sql,
      spendRowPredicate(sql, filter.has_input, node)
    )}`;
  }
  for (const part of filter._and ?? []) {
    const candidates = transactionCandidates(sql, part, node);
    if (candidates !== undefined) return candidates;
  }
  if (given(filter._or) && filter._or.length > 0) {
    const branches = filter._or.map((part) =>
      transactionCandidates(sql, part, node)
    );
    if (branches.every((branch) => branch !== undefined)) {
      return branches.join(' UNION ALL ');
    }
  }
  return undefined;
};

/**
 * True when every transaction matching `filter` is accepted by the filter's
 * node anyway (a positive `block_window`, `in_block` or `in_mempool`): the
 * root's own acceptance check is then implied and left out (one read of the
 * candidates' acceptance rows instead of two).
 */
export const impliesAcceptance = (
  filter: TransactionFilter | null | undefined
): boolean =>
  given(filter) &&
  (given(filter.block_window) ||
    given(filter.in_block) ||
    filter.in_mempool === true ||
    (filter._and ?? []).some(impliesAcceptance) ||
    (given(filter._or) &&
      filter._or.length > 0 &&
      filter._or.every(impliesAcceptance)));

/**
 * A subquery of candidate block internal ids that every block matching
 * `filter` is among (a positive conjunct on `hash` or `height`, both
 * index-backed: the primary key and p_height), or undefined. The node's
 * acceptance check then reads only these blocks' `node_block` rows (by key)
 * instead of every block the node accepts (chipnet: 327k rows, 10-15 ms).
 */
export const blockCandidates = (
  sql: Sql,
  filter: BlockFilter | null | undefined
): string | undefined => {
  if (!given(filter)) return undefined;
  if (hasSelectiveHex(filter.hash) && !given(filter.hash?._prefix)) {
    return `SELECT internal_id FROM ${sql.view('block_at')} WHERE ${all(
      hexComparison(sql, 'hash', 'hash', filter.hash!, 'hash')
    )}`;
  }
  const { height } = filter;
  if (
    given(height) &&
    (given(height._eq) ||
      given(height._in) ||
      given(height._gt) ||
      given(height._gte) ||
      given(height._lt) ||
      given(height._lte))
  ) {
    return `SELECT internal_id FROM ${sql.view('block_at')} WHERE ${all(
      numericComparison(sql, 'height', 'UInt32', {
        _eq: height._eq,
        _gt: height._gt,
        _gte: height._gte,
        _in: height._in,
        _lt: height._lt,
        _lte: height._lte,
      })
    )}`;
  }
  for (const part of filter._and ?? []) {
    const candidates = blockCandidates(sql, part);
    if (candidates !== undefined) return candidates;
  }
  if (given(filter._or) && filter._or.length > 0) {
    const branches = filter._or.map((part) => blockCandidates(sql, part));
    if (branches.every((branch) => branch !== undefined)) {
      return branches.join(' UNION ALL ');
    }
  }
  return undefined;
};

/**
 * Per-node unspent outputs at query time (UTXO tables off, Chaingraph v1's
 * definition): outputs of transactions the node accepts, minus outpoints
 * spent by an input of a transaction the node accepts. With a filter, the
 * spends of matching outputs come from the spend relation (spend index up to
 * the snapshot's spend watermark, the join above it), and the acceptance
 * checks read only the candidates' acceptance rows.
 */
export const unspentOutputPredicate = (
  sql: Sql,
  filter: OutputFilter | null | undefined,
  node?: number
) => {
  const matching = outputPredicate(sql, filter, outputRowColumns, node);
  if (matching === '1') {
    const spent = `SELECT outpoint_transaction_hash, outpoint_index FROM ${sql.view(
      'input_at'
    )} WHERE transaction_hash IN (${sql.accepted(node)})`;
    return `transaction_hash IN (${sql.accepted(node)})
  AND (transaction_hash, output_index) NOT IN (${spent})`;
  }
  const selective = outputFilterIsSelective(filter);
  /*
   * The spends of matching outputs: from the spend relation when the filter
   * can be evaluated on spend rows (index + tail, by script or category),
   * else the inputs spending a matching output.
   */
  const spends = spendRowEvaluable(filter)
    ? `SELECT transaction_hash, outpoint_transaction_hash, outpoint_index FROM ${spendRelationSql(
        sql,
        outputPredicate(sql, filter, spentOutputColumns, node)
      )}`
    : `SELECT transaction_hash, outpoint_transaction_hash, outpoint_index FROM ${sql.view(
        'input_at'
      )} WHERE (outpoint_transaction_hash, outpoint_index) IN (SELECT transaction_hash, output_index FROM ${sql.view(
        'output_at'
      )} WHERE ${outputPredicate(sql, filter, outputRowColumns, node)})`;
  if (selective) {
    /*
     * One acceptance read for the transactions that created a matching
     * output and those that spent one (a statement-level CTE): the two sets
     * overlap and each costs a read of most of tx_acceptance's granules when
     * large (hash order), so reading them together halves that (chipnet L12
     * AllHolders: 6.2M -> 3.3M rows).
     */
    const spending = sql.cte('acc', spends);
    const accepted = sql.cte(
      'accepted',
      sql.accepted(
        node,
        `SELECT transaction_hash FROM ${sql.view(
          'output_at'
        )} WHERE ${outputPredicate(
          sql,
          filter,
          outputRowColumns,
          node
        )} UNION ALL SELECT transaction_hash FROM ${spending}`
      )
    );
    return `transaction_hash IN (SELECT transaction_hash FROM ${accepted})
  AND (transaction_hash, output_index) NOT IN (SELECT outpoint_transaction_hash, outpoint_index FROM ${spending}
    WHERE transaction_hash IN (SELECT transaction_hash FROM ${accepted}))
  AND (${matching})`;
  }
  return `transaction_hash IN (${sql.accepted(node)})
  AND (transaction_hash, output_index) NOT IN (SELECT outpoint_transaction_hash, outpoint_index FROM (${spends}) WHERE transaction_hash IN (${sql.accepted(
    node
  )}))
  AND (${matching})`;
};
