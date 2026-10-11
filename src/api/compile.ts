/* eslint-disable camelcase, @typescript-eslint/naming-convention, complexity, functional/no-throw-statement, @typescript-eslint/no-magic-numbers, functional/no-mixed-type, max-params, functional/no-let, no-underscore-dangle, functional/no-loop-statement, functional/no-try-statement, @typescript-eslint/init-declarations, @typescript-eslint/parameter-properties, @typescript-eslint/strict-boolean-expressions */
// cspell:ignore clickhouse unhex subquery
/**
 * Selection-driven SQL for the v2 API: one root field = ONE ClickHouse
 * statement, at any depth.
 *
 * - The root page is a `MATERIALIZED` CTE (`page`). Every relationship the
 *   client selected is a further `MATERIALIZED` CTE (a "level") whose rows are
 *   read through a pinned view restricted to the parent level's keys (the
 *   "link"), filtered by the relationship's `where`, ordered and limited per
 *   parent (`row_number() OVER (PARTITION BY parent key …)`).
 * - Levels are folded back bottom-up: each level groups its rows by parent
 *   key into an array of tuples (`groupArray`, sorted by the per-parent row
 *   number), joined onto its parent level. The statement returns one tuple
 *   per root row with every selected relationship nested inside it.
 * - Nested GraphQL resolvers then only read prefetched data (no SQL), which is
 *   what lets a live query's result be computed once and fanned out.
 *
 * Entities (model.ts) describe, per GraphQL object type, the row source, the
 * scalar fields (SQL expressions over the level row) and the relationships.
 * Per-node relationships take a `node:` argument (default: the node of the
 * level above, ultimately the root's); a level without a node cannot read
 * per-node data (`Sql.nodeView` refuses).
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
import type { Sql } from './sql.js';
import { invalid } from './sql.js';

/* ------------------------------------------------------------------ */
/* selections                                                           */
/* ------------------------------------------------------------------ */

/** One selected field: response key, field name, coerced arguments, sub-selection. */
export interface Sel {
  key: string;
  name: string;
  args: { [name: string]: unknown };
  sels?: Sel[];
}

export interface FieldSelection {
  responseKey: string;
  name: string;
  args: { [name: string]: unknown };
  selectionSet: SelectionSetNode | undefined;
  type: GraphQLObjectType | undefined;
}

const namedObjectType = (type: unknown): GraphQLObjectType | undefined => {
  let current = type as { ofType?: unknown };
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

type InfoLike = Pick<GraphQLResolveInfo, 'fragments' | 'variableValues'>;

/**
 * The fields selected on an object type (fragments flattened, skip/include
 * applied). Two selections of one response key are merged (GraphQL's field
 * merging: validation guarantees equal arguments).
 */
export const collectFields = (
  info: InfoLike,
  parentType: GraphQLObjectType,
  selectionSet: SelectionSetNode | undefined,
  into = new Map<string, FieldSelection>()
): Map<string, FieldSelection> => {
  selectionSet?.selections.forEach((selection) => {
    if (!included(selection, info.variableValues)) return;
    if (selection.kind === 'Field') {
      const responseKey = selection.alias?.value ?? selection.name.value;
      if (selection.name.value.startsWith('__')) return;
      const field = parentType.getFields()[selection.name.value];
      if (field === undefined) return;
      const existing = into.get(responseKey);
      if (existing !== undefined) {
        /* merge the sub-selections of repeated keys */
        if (existing.selectionSet !== undefined && selection.selectionSet) {
          existing.selectionSet = {
            ...existing.selectionSet,
            selections: [
              ...existing.selectionSet.selections,
              ...selection.selectionSet.selections,
            ],
          };
        }
        return;
      }
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

/** The selection tree below an object type. */
export const selectionTree = (
  info: InfoLike,
  type: GraphQLObjectType,
  selectionSet: SelectionSetNode | undefined
): Sel[] =>
  [...collectFields(info, type, selectionSet).values()].map((field) => ({
    args: field.args,
    key: field.responseKey,
    name: field.name,
    ...(field.type === undefined
      ? {}
      : { sels: selectionTree(info, field.type, field.selectionSet) }),
  }));

type RootInfo = Pick<
  GraphQLResolveInfo,
  'fieldNodes' | 'fragments' | 'returnType' | 'variableValues'
>;

/** The root field's own sub-selection (`info.fieldNodes` merged). */
export const rootSels = (info: RootInfo): Sel[] => {
  const type = namedObjectType(info.returnType);
  if (type === undefined) throw new Error('Root field is not an object.');
  const merged = new Map<string, FieldSelection>();
  info.fieldNodes.forEach((node: FieldNode) =>
    collectFields(info, type, node.selectionSet, merged)
  );
  return [...merged.values()].map((field) => ({
    args: field.args,
    key: field.responseKey,
    name: field.name,
    ...(field.type === undefined
      ? {}
      : { sels: selectionTree(info, field.type, field.selectionSet) }),
  }));
};

/** The selections under `nodes` of a page type (all `nodes` aliases merged). */
export const pageNodeSels = (info: RootInfo): Sel[] =>
  rootSels(info)
    .filter((sel) => sel.name === 'nodes')
    .flatMap((sel) => sel.sels ?? []);

/* ------------------------------------------------------------------ */
/* entities                                                             */
/* ------------------------------------------------------------------ */

export type Order = 'ASC' | 'DESC';

/** How a value of an order expression is carried in a cursor. */
export type ValueKind = 'hex' | 'int' | 'time' | 'uint';

export interface OrderTerm {
  /** SQL over the level row's raw columns. */
  expr: string;
  dir: Order;
  kind: ValueKind;
}

/** The per-level compile context. */
export interface Ctx {
  sql: Sql;
  /** The node of this level (per-node relationships default to it). */
  node: number | undefined;
  /** A node name of the request → its id (it must be pinned by the request snapshot). */
  nodeId: (name: string) => number;
}

export interface ScalarField {
  kind: 'scalar';
  /** SQL over the level row's raw columns; the decoded value is stored under the field name. */
  sql: string;
}

/** A field computed by its resolver from other (hidden) selections. */
export interface ComputedField {
  kind: 'computed';
  /** Selections the resolver needs (scalars by name, relationships by `__…` keys). */
  needs: () => Sel[];
}

export interface RelationField {
  kind: 'relation';
  target: () => Entity;
  many: boolean;
  /** The relationship's rows depend on a node (acceptance, mempool …): `node:` argument. */
  perNode?: boolean;
  /** Parent raw columns the child rows are matched on (join keys). */
  parentKeys: readonly string[];
  /** Child raw columns equal to `parentKeys`. */
  childKeys: readonly string[];
  /** Parent raw columns the source reads (default `parentKeys`). */
  linkColumns?: readonly string[];
  /**
   * The child rows (raw columns of the target entity), restricted to the
   * parent level: `link` is a subquery returning the parent's `linkColumns`.
   */
  source: (c: Ctx, link: string, args: { [name: string]: unknown }) => string;
  /** The `where` argument as a predicate on child rows. */
  where?: (c: Ctx, where: unknown) => string;
  /** Order terms from the arguments (undefined: `defaultOrder`). */
  order?: (args: { [name: string]: unknown }) => OrderTerm[] | undefined;
  defaultOrder: readonly OrderTerm[];
}

export type Field = ComputedField | RelationField | ScalarField;

export interface Entity {
  name: string;
  /** Raw columns a source of this entity provides. */
  columns: readonly string[];
  fields: { [name: string]: Field };
}

/* ------------------------------------------------------------------ */
/* decoded rows                                                         */
/* ------------------------------------------------------------------ */

/** Prefetched relationships of a decoded row, by response key. */
export const relations = Symbol('relations');

export interface Row {
  [key: string]: unknown;
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
  return record as Row;
};

/** Read a prefetched relationship (nested resolvers). */
export const prefetched = (parent: unknown, responseKey: string) => {
  const values = (parent as Row)[relations];
  if (values === undefined || !(responseKey in values)) {
    throw new GraphQLError(`Relationship ${responseKey} was not prefetched.`);
  }
  return values[responseKey];
};

/** Positional tuple (array) or named tuple (object) from JSONEachRow. */
const tupleValues = (value: unknown): unknown[] =>
  Array.isArray(value)
    ? value
    : value !== null && typeof value === 'object'
    ? Object.values(value)
    : [];

/* ------------------------------------------------------------------ */
/* statement                                                            */
/* ------------------------------------------------------------------ */

/** Columns referenced by an expression, among an entity's raw columns. */
const referencedColumns = (expr: string, columns: readonly string[]) =>
  columns.filter((column) =>
    new RegExp(`(?<![\\w.])${column}(?![\\w])`, 'u').test(expr)
  );

/** Response-row cap per statement (R33): a bigger result is an error, never silently cut. */
export const maxRowsPerStatement = 500_000;

class Statement {
  readonly ctes: string[] = [];

  private names = 0;

  private joins = 0;

  constructor(readonly sql: Sql) {}

  cteName() {
    this.names += 1;
    return `l${this.names}`;
  }

  joinName() {
    this.joins += 1;
    return `j${this.joins}`;
  }
}

interface LevelPlan {
  /** Tuple element expressions (after the row number). */
  exprs: string[];
  /** LEFT JOINs of child aggregates. */
  joins: string[];
  /** Raw columns the level row must carry. */
  needs: Set<string>;
  /** Tuple (without the leading row number) → decoded row. */
  decode: (values: unknown[], count: { rows: number }) => Row;
}

const nestedLimitCap = 100_000;

const toLimit = (value: unknown, label: string) => {
  if (value === undefined || value === null) return undefined;
  if (
    typeof value !== 'number' ||
    !Number.isInteger(value) ||
    value < 0 ||
    value > nestedLimitCap
  ) {
    throw invalid(`${label} must be between 0 and ${nestedLimitCap}.`);
  }
  return value;
};

const orderSql = (terms: readonly OrderTerm[]) =>
  terms.map((term) => `${term.expr} ${term.dir}`).join(', ');

/** Hidden selections are expanded once per entity type and field. */
const expandComputed = (entity: Entity, sels: readonly Sel[]): Sel[] => {
  const out: Sel[] = [];
  const seen = new Set<string>();
  const add = (sel: Sel) => {
    const field = entity.fields[sel.name];
    if (field === undefined) {
      throw new GraphQLError(`${entity.name}.${sel.name} is not selectable.`);
    }
    if (field.kind === 'computed') {
      field.needs().forEach(add);
      return;
    }
    if (seen.has(sel.key)) return;
    seen.add(sel.key);
    out.push(sel);
  };
  sels.forEach(add);
  return out;
};

const resolveNode = (c: Ctx, args: { [name: string]: unknown }) =>
  typeof args.node === 'string' ? c.nodeId(args.node) : c.node;

/**
 * Plan one level: `cte` is the level's CTE (or subquery) name; its rows carry
 * the entity's raw columns in `needs`.
 */
const planLevel = (
  st: Statement,
  entity: Entity,
  sels: readonly Sel[],
  cte: string,
  c: Ctx
): LevelPlan => {
  const exprs: string[] = [];
  const joins: string[] = [];
  const needs = new Set<string>();
  const decoders: ((
    values: unknown[],
    record: { [key: string]: unknown },
    related: { [key: string]: unknown },
    count: { rows: number }
  ) => void)[] = [];
  expandComputed(entity, sels).forEach((sel) => {
    const field = entity.fields[sel.name]!;
    if (field.kind === 'scalar') {
      const position = exprs.length;
      exprs.push(field.sql);
      referencedColumns(field.sql, entity.columns).forEach((column) =>
        needs.add(column)
      );
      decoders.push((values, record) => {
        record[sel.name] = values[position] ?? null;
      });
      return;
    }
    if (field.kind !== 'relation') return;
    const target = field.target();
    const childNode = field.perNode ? resolveNode(c, sel.args) : c.node;
    const childCtx: Ctx = { ...c, node: childNode };
    const childCte = st.cteName();
    const reserved = st.ctes.length;
    st.ctes.push('');
    const child = planLevel(st, target, sel.sels ?? [], childCte, childCtx);
    (field.linkColumns ?? field.parentKeys).forEach((column) =>
      needs.add(column)
    );
    field.parentKeys.forEach((column) => needs.add(column));
    const link = `SELECT ${(field.linkColumns ?? field.parentKeys).join(
      ', '
    )} FROM ${cte}`;
    const source = field.source(childCtx, link, sel.args);
    const where =
      field.where === undefined ||
      sel.args.where === undefined ||
      sel.args.where === null
        ? '1'
        : field.where(childCtx, sel.args.where);
    const order = field.order?.(sel.args) ?? field.defaultOrder;
    order.forEach((term) => {
      referencedColumns(term.expr, target.columns).forEach((column) =>
        child.needs.add(column)
      );
    });
    const limit = toLimit(sel.args.limit, `${sel.key}.limit`);
    const offset = toLimit(sel.args.offset, `${sel.key}.offset`);
    const pk = field.childKeys
      .map((key, index) => `${key} AS __pk${index}`)
      .join(', ');
    const childNeeds = [...child.needs];
    const rowNumber = `row_number() OVER (PARTITION BY ${field.childKeys.join(
      ', '
    )}${order.length === 0 ? '' : ` ORDER BY ${orderSql(order)}`}) AS __rn`;
    const bounds = [
      ...(limit === undefined
        ? []
        : [`__rn <= ${st.sql.bind(limit + (offset ?? 0), 'UInt64')}`]),
      ...(offset === undefined
        ? []
        : [`__rn > ${st.sql.bind(offset, 'UInt64')}`]),
    ];
    st.ctes[
      reserved
    ] = `${childCte} AS MATERIALIZED (SELECT * FROM (SELECT ${pk}${
      childNeeds.length === 0 ? '' : `, ${childNeeds.join(', ')}`
    }, ${rowNumber} FROM (${source}) WHERE ${where})${
      bounds.length === 0 ? '' : ` WHERE ${bounds.join(' AND ')}`
    })`;
    const join = st.joinName();
    const keys = field.childKeys.map((_, index) => `__pk${index}`);
    joins.push(
      `LEFT JOIN (SELECT ${keys
        .map((key, index) => `${key} AS __k${index}`)
        .join(', ')}, arraySort(x -> x.1, groupArray(tuple(__rn${
        child.exprs.length === 0 ? '' : `, ${child.exprs.join(', ')}`
      }))) AS __v FROM ${childCte} ${child.joins.join(
        ' '
      )} GROUP BY ${keys.join(', ')}) AS ${join} ON ${field.parentKeys
        .map((key, index) => `${join}.__k${index} = ${key}`)
        .join(' AND ')}`
    );
    const position = exprs.length;
    exprs.push(`${join}.__v`);
    decoders.push((values, _record, related, count) => {
      const items = tupleValues(values[position]).map((item) => {
        const tuple = tupleValues(item);
        count.rows += 1;
        return child.decode(tuple.slice(1), count);
      });
      related[sel.key] = field.many ? items : items[0] ?? null;
    });
  });
  return {
    decode: (values, count) => {
      const record: { [key: string]: unknown } = {};
      const related: { [key: string]: unknown } = {};
      decoders.forEach((decode) => {
        decode(values, record, related, count);
      });
      return withRelations(record, related);
    },
    exprs,
    joins,
    needs,
  };
};

/* ------------------------------------------------------------------ */
/* roots                                                                */
/* ------------------------------------------------------------------ */

export interface RootQuery {
  entity: Entity;
  /** Root rows: a FROM item providing the entity's raw columns. */
  source: string;
  /** Predicate over the source's raw columns. */
  where: string;
  order: readonly OrderTerm[];
  /** Rows to fetch (page size + 1 for `has_more`); undefined: all. */
  limit?: number;
  offset?: number;
}

export interface CompiledRoot {
  sql: string;
  /** Result rows → decoded root rows and the cursor values of each. */
  decode: (rows: { [column: string]: unknown }[]) => {
    rows: Row[];
    cursors: string[][];
  };
}

/** A cursor value as text, by kind (the inverse of `cursorBind`). */
const cursorExpr = (term: OrderTerm) => {
  switch (term.kind) {
    case 'hex':
      return `lower(hex(${term.expr}))`;
    case 'time':
      return `toString(toUnixTimestamp64Milli(${term.expr}))`;
    default:
      return `toString(${term.expr})`;
  }
};

/** Compile one root field: root rows from `root`, every selected relationship nested. One statement. */
export const compileRoot = (
  sql: Sql,
  c: Ctx,
  root: RootQuery,
  sels: readonly Sel[]
): CompiledRoot => {
  const st = new Statement(sql);
  const plan = planLevel(st, root.entity, sels, 'page', c);
  root.order.forEach((term) => {
    referencedColumns(term.expr, root.entity.columns).forEach((column) =>
      plan.needs.add(column)
    );
  });
  const needs = [...plan.needs];
  const order = orderSql(root.order);
  const page = `SELECT ${
    needs.length === 0 ? '1 AS __one' : needs.join(', ')
  } FROM (${
    /^\s*(?:SELECT|WITH)\b/iu.test(root.source)
      ? root.source
      : `SELECT * FROM ${root.source}`
  }) WHERE ${root.where}${order === '' ? '' : ` ORDER BY ${order}`}${
    root.limit === undefined
      ? ''
      : ` LIMIT ${sql.bind(root.limit, 'UInt64')}${
          root.offset === undefined || root.offset === 0
            ? ''
            : ` OFFSET ${sql.bind(root.offset, 'UInt64')}`
        }`
  }`;
  const cursorColumns = root.order.map(
    (term, index) => `${cursorExpr(term)} AS __c${index}`
  );
  const select = `SELECT tuple(0${
    plan.exprs.length === 0 ? '' : `, ${plan.exprs.join(', ')}`
  }) AS __row${
    cursorColumns.length === 0 ? '' : `, ${cursorColumns.join(', ')}`
  }`;
  const statement =
    st.ctes.length === 0
      ? `${select} FROM (${page}) AS page${
          order === '' ? '' : ` ORDER BY ${order}`
        }`
      : `WITH page AS MATERIALIZED (${page}),
${st.ctes.join(',\n')}
${select} FROM page ${plan.joins.join('\n')}${
          order === '' ? '' : `\nORDER BY ${order}`
        }`;
  return {
    decode: (rows) => {
      const count = { rows: rows.length };
      const decoded = rows.map((row) =>
        plan.decode(tupleValues(row.__row).slice(1), count)
      );
      if (count.rows > maxRowsPerStatement) {
        throw new GraphQLError(
          `The result has ${count.rows} rows (limit ${maxRowsPerStatement}): page it with limit/after.`,
          { extensions: { code: 'RESULT_TOO_LARGE' } }
        );
      }
      return {
        cursors: rows.map((row) =>
          root.order.map((_, index) => String(row[`__c${index}`]))
        ),
        rows: decoded,
      };
    },
    sql: statement,
  };
};

/* ------------------------------------------------------------------ */
/* cursors and pages                                                    */
/* ------------------------------------------------------------------ */

export interface Page {
  nodes: Row[];
  has_more: boolean;
  end_cursor: string | null;
}

/** An opaque keyset cursor: the order-key values of the last row of a page. */
export const encodeCursor = (values: readonly string[]) =>
  Buffer.from(JSON.stringify(values)).toString('base64url');

const decodeCursorValues = (cursor: string, length: number) => {
  let values: unknown;
  try {
    values = JSON.parse(Buffer.from(cursor, 'base64url').toString());
  } catch {
    values = undefined;
  }
  if (
    !Array.isArray(values) ||
    values.length !== length ||
    !values.every(
      (value) => typeof value === 'string' && /^-?[0-9a-f]*$/u.test(value)
    )
  ) {
    throw invalid('Invalid cursor (for this order).');
  }
  return values as string[];
};

const cursorBind = (sql: Sql, term: OrderTerm, value: string) => {
  switch (term.kind) {
    case 'hex':
      return `unhex(${sql.bind(value, 'String')})`;
    case 'time':
      return `fromUnixTimestamp64Milli(${sql.bind(
        BigInt(value),
        'Int64'
      )}, 'UTC')`;
    case 'int':
      return sql.bind(BigInt(value), 'Int64');
    default:
      return sql.bind(BigInt(value), 'UInt64');
  }
};

/**
 * The keyset condition "after the row with these order values", for any mix
 * of directions: (k1 > v1) OR (k1 = v1 AND k2 > v2) … (< for DESC).
 */
export const afterCursor = (
  sql: Sql,
  order: readonly OrderTerm[],
  cursor: string
) => {
  const values = decodeCursorValues(cursor, order.length);
  const bound = order.map((term, index) =>
    cursorBind(sql, term, values[index]!)
  );
  const allAsc = order.every((term) => term.dir === 'ASC');
  const allDesc = order.every((term) => term.dir === 'DESC');
  if (allAsc || allDesc) {
    return `(${order.map((term) => term.expr).join(', ')}) ${
      allAsc ? '>' : '<'
    } (${bound.join(', ')})`;
  }
  return order
    .map((term, index) =>
      [
        ...order
          .slice(0, index)
          .map((previous, j) => `${previous.expr} = ${bound[j]!}`),
        `${term.expr} ${term.dir === 'ASC' ? '>' : '<'} ${bound[index]!}`,
      ].join(' AND ')
    )
    .map((part) => `(${part})`)
    .join(' OR ');
};

/** Run a compiled root and shape the page (`limit + 1` rows fetched). */
export const runPage = async (
  db: ApiDb,
  label: string,
  sql: Sql,
  compiled: CompiledRoot,
  limit: number,
  cursorOf?: (row: Row, values: string[]) => string
): Promise<Page> => {
  const { cursors, rows } = compiled.decode(
    await db.query(label, compiled.sql, sql.params)
  );
  const nodes = rows.slice(0, limit);
  const last = nodes.length - 1;
  return {
    end_cursor:
      last < 0
        ? null
        : cursorOf === undefined
        ? encodeCursor(cursors[last]!)
        : cursorOf(nodes[last]!, cursors[last]!),
    has_more: rows.length > limit,
    nodes,
  };
};

/** Run a compiled root returning every row (lookups). */
export const runRows = async (
  db: ApiDb,
  label: string,
  sql: Sql,
  compiled: CompiledRoot
) => compiled.decode(await db.query(label, compiled.sql, sql.params)).rows;
