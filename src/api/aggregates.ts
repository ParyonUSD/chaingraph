/* eslint-disable camelcase, @typescript-eslint/naming-convention, functional/no-throw-statement, @typescript-eslint/no-magic-numbers, max-params, complexity, functional/no-mixed-type, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-argument */
// cspell:ignore clickhouse
/**
 * Aggregates (R24) and group-by (R25) over the per-node row sets of the
 * roots (roots.ts `rowSets`): count, sum, min, max and avg of each entity's
 * numeric fields, over the whole set or per group of key columns. One
 * statement per root field, at the request's snapshot.
 *
 * Empty sets follow SQL (and so Chaingraph v1): count 0, sum/min/max/avg null.
 */
import { GraphQLError } from 'graphql';

import type { Sel } from './compile.js';
import type { RootEnv, RowSet } from './roots.js';
import { checkLimit, maxLimits } from './roots.js';
import { invalid, zeroCategory } from './sql.js';

export type KeyKind = 'bool' | 'hex' | 'int' | 'string';

export interface AggregateSpec {
  /** GraphQL type prefix, e.g. `Output`. */
  name: string;
  /** Numeric fields: field → SQL over the row set's columns. */
  numeric: { [field: string]: string };
  /** Group keys: enum value → field name and SQL (output as text). */
  keys: { [key: string]: { field: string; sql: string; kind: KeyKind } };
  /** Extra columns the keys need (joined onto the row set), by key. */
  joins?: (env: RootEnv, keys: readonly string[]) => string | undefined;
}

const hex = (column: string) => `lower(hex(${column}))`;

export const aggregateSpecs = {
  Block: {
    keys: {
      DAY: {
        field: 'day',
        kind: 'string',
        sql: "formatDateTime(toDateTime(timestamp, 'UTC'), '%Y-%m-%d')",
      },
      MONTH: {
        field: 'month',
        kind: 'string',
        sql: "formatDateTime(toDateTime(timestamp, 'UTC'), '%Y-%m')",
      },
      VERSION: { field: 'version', kind: 'int', sql: 'version' },
      YEAR: {
        field: 'year',
        kind: 'string',
        sql: "formatDateTime(toDateTime(timestamp, 'UTC'), '%Y')",
      },
    },
    name: 'Block',
    numeric: {
      height: 'height',
      output_value_satoshis: 'output_value_satoshis',
      size_bytes: 'size_bytes',
      timestamp: 'timestamp',
      transaction_count: 'transaction_count',
    },
  },
  Mempool: {
    keys: {},
    name: 'NodeTransaction',
    numeric: { transaction_internal_id: 'transaction_internal_id' },
  },
  Output: {
    keys: {
      LOCKING_BYTECODE: {
        field: 'locking_bytecode',
        kind: 'hex',
        sql: hex('locking_bytecode'),
      },
      NONFUNGIBLE_TOKEN_CAPABILITY: {
        field: 'nonfungible_token_capability',
        kind: 'string',
        sql: 'toString(nonfungible_token_capability)',
      },
      NONFUNGIBLE_TOKEN_COMMITMENT: {
        field: 'nonfungible_token_commitment',
        kind: 'hex',
        sql: hex('nonfungible_token_commitment'),
      },
      TOKEN_CATEGORY: {
        field: 'token_category',
        kind: 'hex',
        sql: `if(token_category = ${zeroCategory}, NULL, ${hex(
          'token_category'
        )})`,
      },
      TRANSACTION_HASH: {
        field: 'transaction_hash',
        kind: 'hex',
        sql: hex('transaction_hash'),
      },
    },
    name: 'Output',
    numeric: {
      fungible_token_amount: 'fungible_token_amount',
      output_index: 'output_index',
      value_satoshis: 'value_satoshis',
    },
  },
  Spend: {
    keys: {
      OUTPOINT_LOCKING_BYTECODE: {
        field: 'outpoint_locking_bytecode',
        kind: 'hex',
        sql: hex('locking_bytecode'),
      },
      OUTPOINT_NONFUNGIBLE_TOKEN_COMMITMENT: {
        field: 'outpoint_nonfungible_token_commitment',
        kind: 'hex',
        sql: hex('nonfungible_token_commitment'),
      },
      OUTPOINT_TOKEN_CATEGORY: {
        field: 'outpoint_token_category',
        kind: 'hex',
        sql: `if(token_category = ${zeroCategory}, NULL, ${hex(
          'token_category'
        )})`,
      },
      TRANSACTION_HASH: {
        field: 'transaction_hash',
        kind: 'hex',
        sql: hex('transaction_hash'),
      },
    },
    name: 'Spend',
    numeric: {
      outpoint_fungible_token_amount: 'fungible_token_amount',
      outpoint_value_satoshis: 'value_satoshis',
    },
  },
  Transaction: {
    joins: (env, keys) =>
      keys.some((key) => key === 'BLOCK_HEIGHT' || key === 'BLOCK_DAY')
        ? `LEFT JOIN (SELECT a.transaction_hash AS key_hash, min(a.height) AS key_height,
             min(b.timestamp) AS key_timestamp
           FROM (SELECT transaction_hash, block_internal_id, height FROM ${env.sql.nodeView(
             'tx_acceptance_at',
             env.ctx.node
           )} WHERE block_internal_id != 0) AS a
           INNER JOIN (SELECT internal_id, timestamp FROM ${env.sql.view(
             'block_at'
           )}) AS b ON b.internal_id = a.block_internal_id
           GROUP BY a.transaction_hash) AS keys ON keys.key_hash = hash`
        : undefined,
    keys: {
      BLOCK_DAY: {
        field: 'block_day',
        kind: 'string',
        sql: "if(isNull(keys.key_timestamp) OR keys.key_hash = '', NULL, formatDateTime(toDateTime(keys.key_timestamp, 'UTC'), '%Y-%m-%d'))",
      },
      BLOCK_HEIGHT: {
        field: 'block_height',
        kind: 'int',
        sql: "if(keys.key_hash = '', NULL, keys.key_height)",
      },
      IS_COINBASE: { field: 'is_coinbase', kind: 'bool', sql: 'is_coinbase' },
      VERSION: { field: 'version', kind: 'int', sql: 'version' },
    },
    name: 'Transaction',
    numeric: {
      input_count: 'input_count',
      locktime: 'locktime',
      output_count: 'output_count',
      output_value_satoshis: 'output_value_satoshis',
      size_bytes: 'size_bytes',
    },
  },
} satisfies { [name: string]: AggregateSpec };

/* ------------------------------------------------------------------ */
/* requested aggregates                                                 */
/* ------------------------------------------------------------------ */

export type AggregateFunction = 'avg' | 'max' | 'min' | 'sum';

export interface Requested {
  count: boolean;
  /** function → numeric fields */
  functions: { [fn in AggregateFunction]?: Set<string> };
}

/** The aggregate selections under `aggregate { … }` sels. */
export const requestedAggregates = (aggregateSels: readonly Sel[]) => {
  const requested: Requested = { count: false, functions: {} };
  aggregateSels.forEach((sel) => {
    if (sel.name === 'count') {
      requested.count = true;
      return;
    }
    const fn = sel.name as AggregateFunction;
    const fields = requested.functions[fn] ?? new Set<string>();
    (sel.sels ?? []).forEach((child) => fields.add(child.name));
    requested.functions[fn] = fields;
  });
  return requested;
};

const aggregateColumns = (spec: AggregateSpec, requested: Requested) => {
  const columns = ['toString(count()) AS `count`'];
  (
    Object.entries(requested.functions) as [AggregateFunction, Set<string>][]
  ).forEach(([fn, fields]) => {
    fields.forEach((field) => {
      const expr = spec.numeric[field];
      if (expr === undefined) {
        throw new GraphQLError(`${spec.name}: no numeric field ${field}.`);
      }
      const value =
        fn === 'sum'
          ? `toString(sum(toInt128(${expr})))`
          : fn === 'avg'
          ? `avg(${expr})`
          : `toString(${fn}(${expr}))`;
      columns.push(
        `if(count(${expr}) = 0, NULL, ${value}) AS \`${fn}.${field}\``
      );
    });
  });
  return columns;
};

/** A decoded aggregate row: count, and `fn.field` values (avg as a number). */
export interface AggregateRow {
  count: string;
  values: { [fnField: string]: number | string | null };
  key: { [field: string]: unknown };
}

const decodeRow = (
  row: { [column: string]: unknown },
  keyFields: readonly string[]
): AggregateRow => {
  const values: AggregateRow['values'] = {};
  const key: AggregateRow['key'] = {};
  Object.entries(row).forEach(([column, value]) => {
    if (column === 'count') return;
    if (column.startsWith('key.')) {
      key[column.slice(4)] = value;
      return;
    }
    values[column] =
      value === null || value === undefined
        ? null
        : column.startsWith('avg.')
        ? Number(value)
        : String(value);
  });
  keyFields.forEach((field) => {
    key[field] ??= null;
  });
  return { count: String(row.count), key, values };
};

const whereOf = (rows: RowSet) =>
  rows.where.map((part) => `(${part})`).join(' AND ');

/** count/sum/min/max/avg over a row set (one statement). */
export const runAggregate = async (
  env: RootEnv,
  label: string,
  spec: AggregateSpec,
  rows: RowSet,
  requested: Requested
): Promise<AggregateRow> => {
  const statement = `${env.sql.withClause()}SELECT ${aggregateColumns(
    spec,
    requested
  ).join(', ')} FROM (${rows.source}) WHERE ${whereOf(rows)}`;
  const [row] = await env.db.query<{ [column: string]: unknown }>(
    label,
    statement,
    env.sql.params,
    'aggregate'
  );
  return decodeRow(row ?? { count: '0' }, []);
};

export interface GroupOrder {
  by: string;
  direction?: 'ASC' | 'DESC' | null;
}

export interface GroupArgs {
  group_by: readonly string[];
  order_by?: GroupOrder | readonly GroupOrder[] | null;
  limit: number;
  offset?: number | null;
}

/**
 * Groups of a row set by key columns, each with its aggregates; ordered by
 * keys or aggregates (`COUNT`, `KEY`, `SUM_<FIELD>`, `MIN_…`, `MAX_…`,
 * `AVG_…`), limited (R33: `has_more`).
 */
export const runGroups = async (
  env: RootEnv,
  label: string,
  spec: AggregateSpec,
  rows: RowSet,
  args: GroupArgs,
  requested: Requested
): Promise<{ nodes: AggregateRow[]; has_more: boolean }> => {
  checkLimit(args.limit, maxLimits.groups);
  const keys = [...new Set(args.group_by)];
  if (keys.length === 0) {
    throw invalid('group_by needs at least one key.');
  }
  const keySql = keys.map((key) => {
    const definition = spec.keys[key];
    if (definition === undefined) {
      throw invalid(`${spec.name}: no group key ${key}.`);
    }
    return definition;
  });
  const orders = (
    args.order_by === undefined || args.order_by === null
      ? []
      : Array.isArray(args.order_by)
      ? args.order_by
      : [args.order_by as GroupOrder]
  ).map(({ by, direction }) => {
    const dir = direction ?? 'DESC';
    if (by === 'COUNT') return `count() ${dir}`;
    if (by === 'KEY') {
      return keySql.map((key) => `\`key.${key.field}\` ${dir}`).join(', ');
    }
    const match = /^(?<fn>SUM|MIN|MAX|AVG)_(?<field>.+)$/u.exec(by);
    const field = match?.groups?.field?.toLowerCase();
    const expr = field === undefined ? undefined : spec.numeric[field];
    if (expr === undefined) throw invalid(`Cannot order groups by ${by}.`);
    const fn = match!.groups!.fn!.toLowerCase();
    return `${
      fn === 'sum' ? `sum(toInt128(${expr}))` : `${fn}(${expr})`
    } ${dir}`;
  });
  const keyOrder = keySql.map((key) => `\`key.${key.field}\` ASC`);
  const join = spec.joins?.(env, keys);
  const statement = `${env.sql.withClause()}SELECT ${keySql
    .map((key) => `${key.sql} AS \`key.${key.field}\``)
    .join(', ')}, ${aggregateColumns(spec, requested).join(', ')}
  FROM (${rows.source}) AS rows ${join ?? ''}
  WHERE ${whereOf(rows)}
  GROUP BY ${keySql.map((key) => `\`key.${key.field}\``).join(', ')}
  ORDER BY ${[...orders, ...keyOrder].join(', ')}
  LIMIT ${env.sql.bind(args.limit + 1, 'UInt64')}${
    args.offset === undefined || args.offset === null || args.offset === 0
      ? ''
      : ` OFFSET ${env.sql.bind(args.offset, 'UInt64')}`
  }`;
  const result = await env.db.query<{ [column: string]: unknown }>(
    label,
    statement,
    env.sql.params,
    'aggregate'
  );
  const fields = keySql.map((key) => key.field);
  return {
    has_more: result.length > args.limit,
    nodes: result.slice(0, args.limit).map((row) => decodeRow(row, fields)),
  };
};
