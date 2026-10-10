/* eslint-disable camelcase, @typescript-eslint/naming-convention, complexity, functional/no-throw-statement, @typescript-eslint/no-magic-numbers, @typescript-eslint/parameter-properties, max-params, no-underscore-dangle */
// cspell:ignore clickhouse unhex
/**
 * SQL building blocks of the v2 API spike (Phase 2): a parameter binder and
 * the filter compilers for the typed `where` inputs.
 *
 * Rules (docs/clickhouse-port/phase2-spike.md §2):
 * - Every client value is bound as a `{pN:Type}` query parameter; nothing a
 *   client sends is ever interpolated into SQL text. Identifiers come from
 *   fixed tables in this file, never from a request.
 * - The snapshot parameters (`node`, `visible`, `visible0`, `tail`, `fence`,
 *   `void`) are set only from `readSnapshot` (`Sql` constructor). Client
 *   values are always bound under `p0`, `p1`, …, so they cannot collide with
 *   or override a snapshot parameter.
 * - Every read goes through a pinned `*_at` view (`Sql.view`).
 */
import { GraphQLError } from 'graphql';

import type { VisibilitySnapshot } from '../store/clickhouse/visibility.js';
import { pinnedView, snapshotParams } from '../store/clickhouse/visibility.js';

/** Names of the snapshot parameters; a client can never set them. */
export const snapshotParameterNames = [
  'fence',
  'node',
  'tail',
  'visible',
  'visible0',
  'void',
] as const;

const clientParamPattern = /^p\d+$/u;

/** True for a parameter name the binder hands out for client values. */
export const isClientParamName = (name: string) =>
  clientParamPattern.test(name);

export class Sql {
  readonly params: { [name: string]: unknown };

  private next = 0;

  constructor(readonly snapshot: VisibilitySnapshot) {
    this.params = { ...snapshotParams(snapshot) };
  }

  /** Bind a value; returns its `{pN:Type}` placeholder. */
  bind(value: unknown, type: string) {
    const name = `p${this.next}`;
    this.next += 1;
    this.params[name] = value;
    return `{${name}:${type}}`;
  }

  /** A pinned view call bound to this request's snapshot. */
  // eslint-disable-next-line class-methods-use-this
  view(name: string) {
    return pinnedView(name);
  }

  /** `SELECT transaction_hash FROM tx_acceptance_at(...)`: ACC for the snapshot's node. */
  accepted() {
    return `SELECT transaction_hash FROM ${this.view('tx_acceptance_at')}`;
  }
}

/* ------------------------------------------------------------------ */
/* filter input shapes (as coerced by the schema)                       */
/* ------------------------------------------------------------------ */

export interface IntComparison {
  _eq?: number | null;
  _neq?: number | null;
  _in?: readonly number[] | null;
  _gt?: number | null;
  _gte?: number | null;
  _lt?: number | null;
  _lte?: number | null;
}

export interface BigIntComparison {
  _eq?: bigint | null;
  _neq?: bigint | null;
  _in?: readonly bigint[] | null;
  _gt?: bigint | null;
  _gte?: bigint | null;
  _lt?: bigint | null;
  _lte?: bigint | null;
}

export interface HexComparison {
  _eq?: string | null;
  _neq?: string | null;
  _in?: readonly string[] | null;
  _is_null?: boolean | null;
  _prefix?: string | null;
}

export type Capability = 'minting' | 'mutable' | 'none';

export interface CapabilityComparison {
  _eq?: Capability | null;
  _neq?: Capability | null;
  _in?: readonly Capability[] | null;
  _is_null?: boolean | null;
}

/** Filters on output attributes; also used for a spent output (input row). */
export interface OutputFilter {
  _and?: readonly OutputFilter[] | null;
  _or?: readonly OutputFilter[] | null;
  _not?: OutputFilter | null;
  output_index?: IntComparison | null;
  value_satoshis?: BigIntComparison | null;
  locking_bytecode?: HexComparison | null;
  token_category?: HexComparison | null;
  fungible_token_amount?: BigIntComparison | null;
  nonfungible_token_capability?: CapabilityComparison | null;
  nonfungible_token_commitment?: HexComparison | null;
}

export interface InputFilter {
  _and?: readonly InputFilter[] | null;
  _or?: readonly InputFilter[] | null;
  _not?: InputFilter | null;
  input_index?: IntComparison | null;
  outpoint_transaction_hash?: HexComparison | null;
  /** The spent output, matched on the same input row (same-row conjunction). */
  outpoint?: OutputFilter | null;
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
  /** EXISTS an output of this transaction matching all of the filter (one row). */
  has_output?: OutputFilter | null;
  /** EXISTS an input of this transaction matching all of the filter (one row). */
  has_input?: InputFilter | null;
  /** WIN, per node: in a block the node accepts with height in the window, or in its mempool. */
  block_window?: BlockWindow | null;
}

/* ------------------------------------------------------------------ */
/* comparisons                                                          */
/* ------------------------------------------------------------------ */

type HexColumnKind = 'bytes' | 'hash' | 'nullableBytes' | 'tokenCategory';

const hashBytes = 32;
const zeroCategory = `toFixedString(unhex('${'00'.repeat(hashBytes)}'), 32)`;

const invalid = (message: string) =>
  new GraphQLError(message, { extensions: { code: 'BAD_USER_INPUT' } });

const assertHashLength = (hex: string, column: string) => {
  if (hex.length !== hashBytes * 2) {
    throw invalid(`${column} values are 32 bytes (64 hex characters).`);
  }
};

const all = (parts: readonly string[]) =>
  parts.length === 0 ? '1' : parts.map((part) => `(${part})`).join(' AND ');

const any = (parts: readonly string[]) =>
  parts.length === 0 ? '0' : parts.map((part) => `(${part})`).join(' OR ');

const numericComparison = (
  sql: Sql,
  column: string,
  type: string,
  comparison: BigIntComparison | IntComparison
) => {
  const parts: string[] = [];
  const operators: [keyof IntComparison, string][] = [
    ['_eq', '='],
    ['_neq', '!='],
    ['_gt', '>'],
    ['_gte', '>='],
    ['_lt', '<'],
    ['_lte', '<='],
  ];
  operators.forEach(([key, operator]) => {
    const value = comparison[key];
    if (value !== undefined && value !== null) {
      parts.push(`${column} ${operator} ${sql.bind(value, type)}`);
    }
  });
  if (comparison._in !== undefined && comparison._in !== null) {
    parts.push(`${column} IN ${sql.bind(comparison._in, `Array(${type})`)}`);
  }
  return parts;
};

const hexValue = (
  sql: Sql,
  kind: HexColumnKind,
  hex: string,
  label: string
) => {
  if (kind === 'hash' || kind === 'tokenCategory') {
    assertHashLength(hex, label);
    return `toFixedString(unhex(${sql.bind(hex, 'String')}), 32)`;
  }
  return `unhex(${sql.bind(hex, 'String')})`;
};

const hexComparison = (
  sql: Sql,
  column: string,
  kind: HexColumnKind,
  comparison: HexComparison,
  label: string
) => {
  const parts: string[] = [];
  if (comparison._eq !== undefined && comparison._eq !== null) {
    parts.push(`${column} = ${hexValue(sql, kind, comparison._eq, label)}`);
  }
  if (comparison._neq !== undefined && comparison._neq !== null) {
    parts.push(`${column} != ${hexValue(sql, kind, comparison._neq, label)}`);
  }
  if (comparison._in !== undefined && comparison._in !== null) {
    const fixed = kind === 'hash' || kind === 'tokenCategory';
    if (fixed)
      comparison._in.forEach((hex) => {
        assertHashLength(hex, label);
      });
    parts.push(
      `${column} IN arrayMap(x -> ${
        fixed ? 'toFixedString(unhex(x), 32)' : 'unhex(x)'
      }, ${sql.bind(comparison._in, 'Array(String)')})`
    );
  }
  if (comparison._is_null !== undefined && comparison._is_null !== null) {
    if (kind === 'tokenCategory') {
      parts.push(
        `${column} ${comparison._is_null ? '=' : '!='} ${zeroCategory}`
      );
    } else if (kind === 'nullableBytes') {
      parts.push(
        comparison._is_null ? `isNull(${column})` : `isNotNull(${column})`
      );
    } else {
      parts.push(comparison._is_null ? '0' : '1');
    }
  }
  if (comparison._prefix !== undefined && comparison._prefix !== null) {
    parts.push(
      `startsWith(${
        kind === 'bytes' ? column : `toString(assumeNotNull(${column}))`
      }, unhex(${sql.bind(comparison._prefix, 'String')}))`
    );
  }
  return parts;
};

const capabilityComparison = (
  sql: Sql,
  column: string,
  comparison: CapabilityComparison
) => {
  const parts: string[] = [];
  if (comparison._eq !== undefined && comparison._eq !== null) {
    parts.push(`${column} = ${sql.bind(comparison._eq, 'String')}`);
  }
  if (comparison._neq !== undefined && comparison._neq !== null) {
    parts.push(`${column} != ${sql.bind(comparison._neq, 'String')}`);
  }
  if (comparison._in !== undefined && comparison._in !== null) {
    parts.push(
      `toString(${column}) IN ${sql.bind(comparison._in, 'Array(String)')}`
    );
  }
  if (comparison._is_null !== undefined && comparison._is_null !== null) {
    parts.push(
      comparison._is_null ? `isNull(${column})` : `isNotNull(${column})`
    );
  }
  return parts;
};

/* ------------------------------------------------------------------ */
/* row filters                                                          */
/* ------------------------------------------------------------------ */

/**
 * Columns of an output-like row: `output_at` itself, or an `input_at` row,
 * which carries the spent output's attributes (the outpoint's index is
 * `outpoint_index` there).
 */
export interface OutputColumns {
  output_index: string;
}

export const outputRowColumns: OutputColumns = { output_index: 'output_index' };
export const spentOutputColumns: OutputColumns = {
  output_index: 'outpoint_index',
};

/** An `OutputFilter` as a predicate on one output_at (or input_at) row. */
export const outputPredicate = (
  sql: Sql,
  filter: OutputFilter | null | undefined,
  columns: OutputColumns = outputRowColumns
): string => {
  if (filter === undefined || filter === null) return '1';
  const parts: string[] = [];
  if (filter.output_index) {
    parts.push(
      ...numericComparison(
        sql,
        columns.output_index,
        'UInt32',
        filter.output_index
      )
    );
  }
  if (filter.value_satoshis) {
    parts.push(
      ...numericComparison(
        sql,
        'value_satoshis',
        'Int64',
        filter.value_satoshis
      )
    );
  }
  if (filter.fungible_token_amount) {
    parts.push(
      ...numericComparison(
        sql,
        'fungible_token_amount',
        'Int64',
        filter.fungible_token_amount
      )
    );
  }
  if (filter.locking_bytecode) {
    parts.push(
      ...hexComparison(
        sql,
        'locking_bytecode',
        'bytes',
        filter.locking_bytecode,
        'locking_bytecode'
      )
    );
  }
  if (filter.token_category) {
    parts.push(
      ...hexComparison(
        sql,
        'token_category',
        'tokenCategory',
        filter.token_category,
        'token_category'
      )
    );
  }
  if (filter.nonfungible_token_capability) {
    parts.push(
      ...capabilityComparison(
        sql,
        'nonfungible_token_capability',
        filter.nonfungible_token_capability
      )
    );
  }
  if (filter.nonfungible_token_commitment) {
    parts.push(
      ...hexComparison(
        sql,
        'nonfungible_token_commitment',
        'nullableBytes',
        filter.nonfungible_token_commitment,
        'nonfungible_token_commitment'
      )
    );
  }
  if (filter._and) {
    parts.push(...filter._and.map((f) => outputPredicate(sql, f, columns)));
  }
  if (filter._or) {
    parts.push(any(filter._or.map((f) => outputPredicate(sql, f, columns))));
  }
  if (filter._not) {
    parts.push(`NOT (${outputPredicate(sql, filter._not, columns)})`);
  }
  return all(parts);
};

/** An `InputFilter` as a predicate on one input_at row (outpoint fields on the same row). */
export const inputPredicate = (
  sql: Sql,
  filter: InputFilter | null | undefined
): string => {
  if (filter === undefined || filter === null) return '1';
  const parts: string[] = [];
  if (filter.input_index) {
    parts.push(
      ...numericComparison(sql, 'input_index', 'UInt32', filter.input_index)
    );
  }
  if (filter.outpoint_transaction_hash) {
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
  if (filter.outpoint) {
    parts.push(outputPredicate(sql, filter.outpoint, spentOutputColumns));
  }
  if (filter._and) {
    parts.push(...filter._and.map((f) => inputPredicate(sql, f)));
  }
  if (filter._or) {
    parts.push(any(filter._or.map((f) => inputPredicate(sql, f))));
  }
  if (filter._not) {
    parts.push(`NOT (${inputPredicate(sql, filter._not)})`);
  }
  return all(parts);
};

/**
 * A `TransactionFilter` as a predicate on a transaction_at row (`hash`,
 * `internal_id`). Relationship filters are semi-joins on the pinned views
 * (EXISTS = `hash IN (…)`, NOT EXISTS = `NOT (hash IN (…))`).
 */
export const transactionPredicate = (
  sql: Sql,
  filter: TransactionFilter | null | undefined
): string => {
  if (filter === undefined || filter === null) return '1';
  const parts: string[] = [];
  if (filter.internal_id) {
    parts.push(
      ...numericComparison(sql, 'internal_id', 'UInt64', filter.internal_id)
    );
  }
  if (filter.hash) {
    parts.push(...hexComparison(sql, 'hash', 'hash', filter.hash, 'hash'));
  }
  if (filter.has_output) {
    parts.push(
      `hash IN (SELECT transaction_hash FROM ${sql.view(
        'output_at'
      )} WHERE ${outputPredicate(sql, filter.has_output)})`
    );
  }
  if (filter.has_input) {
    parts.push(
      `hash IN (SELECT transaction_hash FROM ${sql.view(
        'input_at'
      )} WHERE ${inputPredicate(sql, filter.has_input)})`
    );
  }
  if (filter.block_window) {
    const window = filter.block_window;
    const heights: string[] = ['block_internal_id != 0'];
    if (window.from_height !== undefined && window.from_height !== null) {
      heights.push(`height >= ${sql.bind(window.from_height, 'UInt32')}`);
    }
    if (window.to_height !== undefined && window.to_height !== null) {
      heights.push(`height < ${sql.bind(window.to_height, 'UInt32')}`);
    }
    const mempool = window.include_mempool !== false;
    parts.push(
      `hash IN (SELECT transaction_hash FROM ${sql.view(
        'tx_acceptance_at'
      )} WHERE ${
        mempool ? any([all(heights), 'block_internal_id = 0']) : all(heights)
      })`
    );
  }
  if (filter._and) {
    parts.push(...filter._and.map((f) => transactionPredicate(sql, f)));
  }
  if (filter._or) {
    parts.push(any(filter._or.map((f) => transactionPredicate(sql, f))));
  }
  if (filter._not) {
    parts.push(`NOT (${transactionPredicate(sql, filter._not)})`);
  }
  return all(parts);
};

/**
 * Per-node unspent outputs at query time (UTXO tables off, Chaingraph v1's
 * definition): outputs of transactions the node accepts, minus outpoints
 * spent by an input of a transaction the node accepts. The anti-join is
 * narrowed with the same filter applied to the spending input's copy of the
 * spent output's attributes (input carries them), so it reads only inputs
 * that could spend a matching output (the `p_spent_*` projections).
 */
export const unspentOutputPredicate = (
  sql: Sql,
  filter: OutputFilter | null | undefined
) => {
  const spentScope = outputPredicate(sql, filter, spentOutputColumns);
  return `transaction_hash IN (${sql.accepted()})
  AND (transaction_hash, output_index) NOT IN (
    SELECT outpoint_transaction_hash, outpoint_index FROM ${sql.view(
      'input_at'
    )}
    WHERE (${spentScope}) AND transaction_hash IN (${sql.accepted()}))
  AND (${outputPredicate(sql, filter)})`;
};
