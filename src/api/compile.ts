/* eslint-disable camelcase, @typescript-eslint/naming-convention, complexity, functional/no-throw-statement, @typescript-eslint/no-magic-numbers, functional/no-mixed-type, max-params, prefer-named-capture-group */
// cspell:ignore clickhouse unhex subquery
/**
 * Selection-driven SQL for the v2 API spike: one root field = ONE
 * ClickHouse statement. The root rows come from a pinned view; every
 * relationship the client selected (transaction → outputs / inputs /
 * block_inclusions, output/input → transaction) is a `LEFT JOIN` onto a
 * grouped subquery over a pinned view, restricted to the root page's keys.
 * Nested resolvers then only read prefetched data (no SQL), which is what
 * lets a live query's result be computed once and fanned out.
 *
 * Supported depth (spike): root output/input → transaction → outputs,
 * inputs, block_inclusions; root transaction → outputs, inputs,
 * block_inclusions.
 * Deeper paths (e.g. transaction.outputs.transaction) are rejected.
 */
import type {
  FieldNode,
  GraphQLObjectType,
  GraphQLResolveInfo,
  SelectionSetNode,
} from 'graphql';
import {
  getArgumentValues,
  getDirectiveValues,
  GraphQLError,
  GraphQLIncludeDirective,
  GraphQLSkipDirective,
  isObjectType,
} from 'graphql';

import type { ApiDb } from './db.js';
import type { InputFilter, OutputFilter, Sql } from './sql.js';
import { inputPredicate, outputPredicate } from './sql.js';

/* ------------------------------------------------------------------ */
/* selection collection                                                 */
/* ------------------------------------------------------------------ */

export interface FieldSelection {
  responseKey: string;
  name: string;
  args: { [name: string]: unknown };
  selectionSet: SelectionSetNode | undefined;
  type: GraphQLObjectType | undefined;
}

const namedObjectType = (type: unknown): GraphQLObjectType | undefined => {
  // unwrap NonNull / List
  // eslint-disable-next-line functional/no-let
  let current = type as { ofType?: unknown };
  // eslint-disable-next-line functional/no-loop-statement
  while (current.ofType !== undefined) {
    current = current.ofType as { ofType?: unknown };
  }
  return isObjectType(current) ? current : undefined;
};

const included = (
  node: Parameters<typeof getDirectiveValues>[1],
  variables: GraphQLResolveInfo['variableValues']
) => {
  const skip = getDirectiveValues(GraphQLSkipDirective, node, variables);
  if (skip?.if === true) return false;
  const include = getDirectiveValues(GraphQLIncludeDirective, node, variables);
  return include?.if !== false;
};

/** The fields selected on an object type (fragments flattened, skip/include directives applied). */
export const collectFields = (
  info: Pick<GraphQLResolveInfo, 'fragments' | 'variableValues'>,
  parentType: GraphQLObjectType,
  selectionSet: SelectionSetNode | undefined,
  into = new Map<string, FieldSelection>()
): Map<string, FieldSelection> => {
  selectionSet?.selections.forEach((selection) => {
    if (!included(selection, info.variableValues)) return;
    if (selection.kind === 'Field') {
      const responseKey = selection.alias?.value ?? selection.name.value;
      if (into.has(responseKey) || selection.name.value.startsWith('__')) {
        return;
      }
      const field = parentType.getFields()[selection.name.value];
      if (field === undefined) return;
      into.set(responseKey, {
        args: getArgumentValues(field, selection, info.variableValues),
        name: selection.name.value,
        responseKey,
        selectionSet: selection.selectionSet,
        type: namedObjectType(field.type),
      });
    } else if (selection.kind === 'InlineFragment') {
      collectFields(info, parentType, selection.selectionSet, into);
    } else {
      const fragment = info.fragments[selection.name.value];
      if (fragment !== undefined) {
        collectFields(info, parentType, fragment.selectionSet, into);
      }
    }
  });
  return into;
};

/** The root field's own sub-selection: `info.fieldNodes` merged. */
export const rootSelection = (
  info: Pick<
    GraphQLResolveInfo,
    'fieldNodes' | 'fragments' | 'returnType' | 'variableValues'
  >
) => {
  const type = namedObjectType(info.returnType);
  if (type === undefined) throw new Error('Root field is not an object.');
  const merged = new Map<string, FieldSelection>();
  info.fieldNodes.forEach((node: FieldNode) =>
    collectFields(info, type, node.selectionSet, merged)
  );
  return { fields: merged, type };
};

/* ------------------------------------------------------------------ */
/* columns                                                              */
/* ------------------------------------------------------------------ */

const hex = (column: string) => `lower(hex(${column}))`;
const zeroCategory = `toFixedString(unhex('${'00'.repeat(32)}'), 32)`;

type Column = readonly [name: string, sql: string];

const transactionColumns: readonly Column[] = [
  ['hash', hex('hash')],
  ['internal_id', 'toString(internal_id)'],
  ['version', 'version'],
  ['locktime', 'locktime'],
  ['size_bytes', 'size_bytes'],
  ['is_coinbase', 'is_coinbase'],
  ['input_count', 'input_count'],
  ['output_count', 'output_count'],
  ['output_value_satoshis', 'toString(output_value_satoshis)'],
];

/** The token/value attributes shared by output and input (spent output) rows. */
const valueColumns: readonly Column[] = [
  ['value_satoshis', 'toString(value_satoshis)'],
  ['locking_bytecode', hex('locking_bytecode')],
  [
    'token_category',
    `if(token_category = ${zeroCategory}, NULL, ${hex('token_category')})`,
  ],
  ['fungible_token_amount', 'toString(fungible_token_amount)'],
  ['nonfungible_token_capability', 'toString(nonfungible_token_capability)'],
  ['nonfungible_token_commitment', hex('nonfungible_token_commitment')],
];

const outputColumns: readonly Column[] = [
  ['transaction_hash', hex('transaction_hash')],
  ['output_index', 'output_index'],
  ['transaction_internal_id', 'toString(transaction_internal_id)'],
  ...valueColumns,
];

const inputColumns = (withUnlocking: boolean): readonly Column[] => [
  ['transaction_hash', hex('transaction_hash')],
  ['input_index', 'input_index'],
  ['transaction_internal_id', 'toString(transaction_internal_id)'],
  ['outpoint_transaction_hash', hex('outpoint_transaction_hash')],
  ['outpoint_index', 'outpoint_index'],
  ['sequence_number', 'sequence_number'],
  ['unlocking_bytecode', withUnlocking ? hex('unlocking_bytecode') : "''"],
  ...valueColumns.map(([name, sql]): Column => [`outpoint.${name}`, sql]),
];

const inclusionColumns: readonly Column[] = [
  ['block.hash', hex('b.hash')],
  ['block.height', 'b.height'],
  ['block.timestamp', 'b.timestamp'],
  ['block.internal_id', 'toString(b.internal_id)'],
  ['transaction_index', 'bt.transaction_index'],
];

const tupleOf = (columns: readonly Column[]) =>
  `tuple(${columns.map(([, sql]) => sql).join(', ')})`;

/** Positional tuple (array) or named tuple (object) from JSONEachRow. */
const tupleValues = (value: unknown): unknown[] =>
  Array.isArray(value)
    ? value
    : value !== null && typeof value === 'object'
    ? Object.values(value)
    : [];

/** Rows as plain records; `a.b` names build nested objects. */
const decodeTuple = (columns: readonly Column[], value: unknown) => {
  const values = tupleValues(value);
  const record: { [key: string]: unknown } = {};
  columns.forEach(([name], index) => {
    const path = name.split('.');
    // eslint-disable-next-line functional/no-let
    let target = record;
    path.slice(0, -1).forEach((part) => {
      target[part] ??= {};
      target = target[part] as { [key: string]: unknown };
    });
    target[path[path.length - 1]!] = values[index] ?? null;
  });
  return record;
};

/** Prefetched relationships of a decoded row, by response key. */
export const relations = Symbol('relations');

export interface Prefetched {
  [relations]?: { [responseKey: string]: unknown };
}

const withRelations = (
  record: { [key: string]: unknown },
  values: { [key: string]: unknown }
) => {
  Object.defineProperty(record, relations, {
    enumerable: false,
    value: values,
  });
  return record;
};

/** Read a prefetched relationship (nested resolvers). */
export const prefetched = (parent: unknown, responseKey: string) => {
  const values = (parent as Prefetched)[relations];
  if (values === undefined || !(responseKey in values)) {
    throw new GraphQLError(
      `Relationship ${responseKey} is not supported at this depth (spike).`
    );
  }
  return values[responseKey];
};

/* ------------------------------------------------------------------ */
/* plans                                                                */
/* ------------------------------------------------------------------ */

export type Order = 'ASC' | 'DESC';

interface Join {
  alias: string;
  sql: string;
  /** Root-row column the join key matches. */
  rootKey: string;
}

/** Joins plus a decoder that turns joined values into relationship values. */
interface RelationPlan {
  joins: Join[];
  decode: (row: { [column: string]: unknown }) => { [key: string]: unknown };
}

class JoinNames {
  private next = 0;

  alias() {
    const name = `j${this.next}`;
    this.next += 1;
    return name;
  }
}

const sortByIndex = (
  rows: { [key: string]: unknown }[],
  key: string,
  order: Order
) =>
  rows.sort((a, b) =>
    order === 'ASC'
      ? Number(a[key]) - Number(b[key])
      : Number(b[key]) - Number(a[key])
  );

const nestedLimit = (value: unknown) =>
  typeof value === 'number' && value >= 0 ? value : undefined;

const rejectDeeper = (field: FieldSelection, allowed: readonly string[]) => {
  if (field.type === undefined) return;
  if (!allowed.includes(field.name)) {
    throw new GraphQLError(
      `${field.name} is not supported at this depth in the spike.`
    );
  }
};

const assertNoRelations = (
  info: Pick<GraphQLResolveInfo, 'fragments' | 'variableValues'>,
  field: FieldSelection,
  allowed: readonly string[]
) => {
  if (field.type === undefined) return;
  collectFields(info, field.type, field.selectionSet).forEach((child) => {
    rejectDeeper(child, allowed);
  });
};

/**
 * Relationships of a Transaction selection, as joins keyed by a transaction
 * hash column of the root row. `keys` is a subquery returning the page's
 * transaction hashes.
 */
const transactionRelations = (
  sql: Sql,
  info: Pick<GraphQLResolveInfo, 'fragments' | 'variableValues'>,
  names: JoinNames,
  fields: Map<string, FieldSelection>,
  keys: string,
  rootKey: string
): RelationPlan => {
  const decoders: ((row: { [c: string]: unknown }) => [string, unknown])[] = [];
  const joins: Join[] = [];
  fields.forEach((field) => {
    if (field.name === 'outputs') {
      assertNoRelations(info, field, []);
      const alias = names.alias();
      const where = outputPredicate(sql, field.args.where as OutputFilter);
      joins.push({
        alias,
        rootKey,
        sql: `SELECT transaction_hash AS k, groupArray(${tupleOf(
          outputColumns
        )}) AS v FROM ${sql.view('output_at')}
          WHERE transaction_hash IN (${keys}) AND (${where}) GROUP BY transaction_hash`,
      });
      const order = (field.args.order as Order | undefined) ?? 'ASC';
      const limit = nestedLimit(field.args.limit);
      decoders.push((row) => {
        const items = sortByIndex(
          tupleValues(row[alias]).map((value) =>
            decodeTuple(outputColumns, value)
          ),
          'output_index',
          order
        );
        return [
          field.responseKey,
          limit === undefined ? items : items.slice(0, limit),
        ];
      });
    } else if (field.name === 'inputs') {
      assertNoRelations(info, field, ['outpoint']);
      const alias = names.alias();
      const where = inputPredicate(sql, field.args.where as InputFilter);
      const childFields =
        field.type === undefined
          ? new Map<string, FieldSelection>()
          : collectFields(info, field.type, field.selectionSet);
      const columns = inputColumns(
        [...childFields.values()].some(
          (child) => child.name === 'unlocking_bytecode'
        )
      );
      joins.push({
        alias,
        rootKey,
        sql: `SELECT transaction_hash AS k, groupArray(${tupleOf(
          columns
        )}) AS v FROM ${sql.view('input_at')}
          WHERE transaction_hash IN (${keys}) AND (${where}) GROUP BY transaction_hash`,
      });
      const order = (field.args.order as Order | undefined) ?? 'ASC';
      const limit = nestedLimit(field.args.limit);
      decoders.push((row) => {
        const items = sortByIndex(
          tupleValues(row[alias]).map((value) => decodeTuple(columns, value)),
          'input_index',
          order
        );
        return [
          field.responseKey,
          limit === undefined ? items : items.slice(0, limit),
        ];
      });
    } else if (field.name === 'block_inclusions') {
      assertNoRelations(info, field, ['block']);
      const alias = names.alias();
      /*
       * Per node (v2): the blocks the snapshot's node accepts that include
       * the transaction (tx_acceptance_at rows with a block), with the
       * block's header fields from block_at.
       */
      joins.push({
        alias,
        rootKey,
        sql: `SELECT a.transaction_hash AS k, groupArray(${tupleOf(
          inclusionColumns
        )}) AS v
          FROM (SELECT transaction_hash, block_internal_id FROM ${sql.view(
            'tx_acceptance_at'
          )}
                WHERE block_internal_id != 0 AND transaction_hash IN (${keys})) AS a
          INNER JOIN (SELECT internal_id, hash, height, timestamp FROM ${sql.view(
            'block_at'
          )}
                WHERE internal_id IN (SELECT block_internal_id FROM ${sql.view(
                  'tx_acceptance_at'
                )} WHERE transaction_hash IN (${keys}))) AS b
            ON b.internal_id = a.block_internal_id
          LEFT JOIN (SELECT block_internal_id, transaction_hash, transaction_index FROM ${sql.view(
            'block_transaction_at'
          )}
                WHERE transaction_hash IN (${keys})) AS bt
            ON bt.block_internal_id = a.block_internal_id AND bt.transaction_hash = a.transaction_hash
          GROUP BY a.transaction_hash`,
      });
      decoders.push((row) => [
        field.responseKey,
        sortByIndex(
          tupleValues(row[alias]).map((value) =>
            decodeTuple(inclusionColumns, value)
          ),
          'height',
          'ASC'
        ),
      ]);
    } else if (field.type !== undefined) {
      throw new GraphQLError(`Unsupported relationship ${field.name}.`);
    }
  });
  return {
    decode: (row) => Object.fromEntries(decoders.map((decode) => decode(row))),
    joins,
  };
};

/**
 * The `transaction` relationship(s) of an output or input root: the
 * creating/spending transaction (1:1) plus its own relationships.
 */
const parentTransactionRelations = (
  sql: Sql,
  info: Pick<GraphQLResolveInfo, 'fragments' | 'variableValues'>,
  names: JoinNames,
  fields: Map<string, FieldSelection>,
  keys: string,
  allowed: readonly string[]
): RelationPlan => {
  const joins: Join[] = [];
  const decoders: ((row: { [c: string]: unknown }) => [string, unknown])[] = [];
  fields.forEach((field) => {
    if (field.type === undefined) return;
    if (field.name !== 'transaction') {
      rejectDeeper(field, allowed);
      return;
    }
    const alias = names.alias();
    joins.push({
      alias,
      rootKey: 'k_tx',
      sql: `SELECT hash AS k, any(${tupleOf(transactionColumns)}) AS v
        FROM ${sql.view(
          'transaction_at'
        )} WHERE hash IN (${keys}) GROUP BY hash`,
    });
    const childFields = collectFields(info, field.type, field.selectionSet);
    const nested = transactionRelations(
      sql,
      info,
      names,
      childFields,
      keys,
      'k_tx'
    );
    joins.push(...nested.joins);
    decoders.push((row) => [
      field.responseKey,
      withRelations(
        decodeTuple(transactionColumns, row[alias]),
        nested.decode(row)
      ),
    ]);
  });
  return {
    decode: (row) => Object.fromEntries(decoders.map((decode) => decode(row))),
    joins,
  };
};

/* ------------------------------------------------------------------ */
/* root statements                                                      */
/* ------------------------------------------------------------------ */

export interface RootQuery {
  /** Source of root rows: a pinned view (or a subquery over one) with a `k_tx` column (raw transaction hash). */
  from: string;
  where: string;
  /** Raw key columns of the order, in order. */
  orderColumns: readonly string[];
  order: Order;
  /** Rows to fetch (page size + 1, for `has_more`). */
  limit: number;
}

export interface CompiledRoot {
  sql: string;
  decode: (rows: { [column: string]: unknown }[]) => {
    [key: string]: unknown;
  }[];
}

/** Output aliases are `c.<name>`, so they never shadow a raw column in WHERE. */
const outputAlias = (name: string) => `c.${name}`;

/**
 * Compile one root field: root rows from `root`, the selected relationships
 * joined on. One statement.
 */
const compileRoot = (
  sql: Sql,
  info: Pick<GraphQLResolveInfo, 'fragments' | 'variableValues'>,
  root: RootQuery,
  columns: readonly Column[],
  nodeFields: Map<string, FieldSelection>,
  kind: 'input' | 'output' | 'transaction'
): CompiledRoot => {
  const names = new JoinNames();
  const orderBy = (prefix: string) =>
    root.orderColumns
      .map((column) => `${prefix}${column} ${root.order}`)
      .join(', ');
  const rootSql = `SELECT ${columns
    .map(([name, expr]) => `${expr} AS \`${outputAlias(name)}\``)
    .join(', ')}, k_tx, ${root.orderColumns.join(', ')}
    FROM ${root.from} WHERE ${root.where} ORDER BY ${orderBy(
    ''
  )} LIMIT ${sql.bind(root.limit, 'UInt32')}`;
  /*
   * The page is a MATERIALIZED CTE: evaluated once and reused by every
   * relationship subquery (`keys`) and as the root rows. Without it,
   * ClickHouse expands the CTE in place and re-runs the root statement once per
   * relationship (S4: 160 ms instead of 65 ms; phase2-spike.md §4).
   */
  const keys = 'SELECT k_tx FROM page';
  const plan =
    kind === 'transaction'
      ? transactionRelations(sql, info, names, nodeFields, keys, 'k_tx')
      : parentTransactionRelations(
          sql,
          info,
          names,
          nodeFields,
          keys,
          kind === 'input' ? ['outpoint'] : []
        );
  const statement =
    plan.joins.length === 0
      ? `SELECT r.* FROM (${rootSql}) AS r ORDER BY ${orderBy('r.')}`
      : `WITH page AS MATERIALIZED (${rootSql})
SELECT r.*${plan.joins.map(({ alias }) => `, ${alias}.v AS ${alias}`).join('')}
FROM page AS r
${plan.joins
  .map(
    ({ alias, rootKey, sql: joinSql }) =>
      `LEFT JOIN (${joinSql}) AS ${alias} ON ${alias}.k = r.${rootKey}`
  )
  .join('\n')}
ORDER BY ${orderBy('r.')}`;
  return {
    decode: (rows) =>
      rows.map((row) =>
        withRelations(
          decodeTuple(
            columns,
            columns.map(([name]) => row[outputAlias(name)])
          ),
          plan.decode(row)
        )
      ),
    sql: statement,
  };
};

/* ------------------------------------------------------------------ */
/* root fields                                                          */
/* ------------------------------------------------------------------ */

export interface Page {
  nodes: { [key: string]: unknown }[];
  has_more: boolean;
  end_cursor: string | null;
}

/** Root selection of a page type: the fields under `nodes`. */
const nodeFieldsOfPage = (
  info: Pick<
    GraphQLResolveInfo,
    'fieldNodes' | 'fragments' | 'returnType' | 'variableValues'
  >
) => {
  const { fields } = rootSelection(info);
  const merged = new Map<string, FieldSelection>();
  fields.forEach((field) => {
    if (field.name === 'nodes' && field.type !== undefined) {
      collectFields(info, field.type, field.selectionSet, merged);
    }
  });
  return merged;
};

type RootInfo = Pick<
  GraphQLResolveInfo,
  'fieldNodes' | 'fragments' | 'returnType' | 'variableValues'
>;

const encodeCursor = (parts: readonly (number | string)[]) =>
  Buffer.from(parts.join(':')).toString('base64url');

/** Composite keyset cursor: `(transaction_internal_id, index)`. */
export const decodeCursor = (cursor: string | null | undefined) => {
  if (cursor === undefined || cursor === null) return undefined;
  const match = /^(\d{1,20}):(\d{1,10})$/u.exec(
    Buffer.from(cursor, 'base64url').toString()
  );
  if (match === null) {
    throw new GraphQLError('Invalid cursor.', {
      extensions: { code: 'BAD_USER_INPUT' },
    });
  }
  return { id: BigInt(match[1]!), index: Number(match[2]!) };
};

export interface TransactionsArgs {
  where?: unknown;
  order?: Order | null;
  after?: bigint | null;
  before?: bigint | null;
  limit: number;
}

export const runTransactions = async (
  db: ApiDb,
  sql: Sql,
  info: RootInfo,
  predicate: string,
  args: TransactionsArgs
): Promise<Page> => {
  const order = args.order ?? 'ASC';
  const bounds = [
    ...(args.after === undefined || args.after === null
      ? []
      : [`internal_id > ${sql.bind(args.after, 'UInt64')}`]),
    ...(args.before === undefined || args.before === null
      ? []
      : [`internal_id < ${sql.bind(args.before, 'UInt64')}`]),
  ];
  const compiled = compileRoot(
    sql,
    info,
    {
      from: `(SELECT *, hash AS k_tx FROM ${sql.view('transaction_at')})`,
      limit: args.limit + 1,
      order,
      orderColumns: ['internal_id'],
      where: [`hash IN (${sql.accepted()})`, ...bounds, `(${predicate})`].join(
        ' AND '
      ),
    },
    transactionColumns,
    nodeFieldsOfPage(info),
    'transaction'
  );
  const rows = compiled.decode(
    await db.query('transactions', compiled.sql, sql.params)
  );
  const nodes = rows.slice(0, args.limit);
  return {
    end_cursor:
      nodes.length === 0 ? null : String(nodes[nodes.length - 1]!.internal_id),
    has_more: rows.length > args.limit,
    nodes,
  };
};

export interface KeysetArgs {
  after?: string | null;
  limit: number;
}

export const runInputs = async (
  db: ApiDb,
  sql: Sql,
  info: RootInfo,
  filter: InputFilter | null | undefined,
  args: KeysetArgs
): Promise<Page> => {
  const after = decodeCursor(args.after);
  const compiled = compileRoot(
    sql,
    info,
    {
      from: `(SELECT *, transaction_hash AS k_tx FROM ${sql.view('input_at')})`,
      limit: args.limit + 1,
      order: 'ASC',
      orderColumns: ['transaction_internal_id', 'input_index'],
      where: [
        `transaction_hash IN (${sql.accepted()})`,
        ...(after === undefined
          ? []
          : [
              `(transaction_internal_id, input_index) > (${sql.bind(
                after.id,
                'UInt64'
              )}, ${sql.bind(after.index, 'UInt32')})`,
            ]),
        `(${inputPredicate(sql, filter)})`,
      ].join(' AND '),
    },
    inputColumns(true),
    nodeFieldsOfPage(info),
    'input'
  );
  const rows = compiled.decode(
    await db.query('inputs', compiled.sql, sql.params)
  );
  const nodes = rows.slice(0, args.limit);
  const last = nodes[nodes.length - 1];
  return {
    end_cursor:
      last === undefined
        ? null
        : encodeCursor([
            String(last.transaction_internal_id),
            Number(last.input_index),
          ]),
    has_more: rows.length > args.limit,
    nodes,
  };
};

export const runUnspentOutputs = async (
  db: ApiDb,
  sql: Sql,
  info: RootInfo,
  predicate: string,
  args: KeysetArgs
): Promise<Page> => {
  const after = decodeCursor(args.after);
  const compiled = compileRoot(
    sql,
    info,
    {
      from: `(SELECT *, transaction_hash AS k_tx FROM ${sql.view(
        'output_at'
      )})`,
      limit: args.limit + 1,
      order: 'ASC',
      orderColumns: ['transaction_internal_id', 'output_index'],
      where: [
        ...(after === undefined
          ? []
          : [
              `(transaction_internal_id, output_index) > (${sql.bind(
                after.id,
                'UInt64'
              )}, ${sql.bind(after.index, 'UInt32')})`,
            ]),
        `(${predicate})`,
      ].join(' AND '),
    },
    outputColumns,
    nodeFieldsOfPage(info),
    'output'
  );
  const rows = compiled.decode(
    await db.query('unspent_outputs', compiled.sql, sql.params)
  );
  const nodes = rows.slice(0, args.limit);
  const last = nodes[nodes.length - 1];
  return {
    end_cursor:
      last === undefined
        ? null
        : encodeCursor([
            String(last.transaction_internal_id),
            Number(last.output_index),
          ]),
    has_more: rows.length > args.limit,
    nodes,
  };
};
