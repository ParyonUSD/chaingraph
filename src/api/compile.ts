/* eslint-disable camelcase, @typescript-eslint/naming-convention, complexity, functional/no-throw-statement, @typescript-eslint/no-magic-numbers, functional/no-mixed-type, max-params, functional/no-let, no-underscore-dangle, functional/no-loop-statement, functional/no-try-statement, @typescript-eslint/init-declarations, @typescript-eslint/parameter-properties, @typescript-eslint/strict-boolean-expressions */
// cspell:ignore clickhouse unhex subquery analysed
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
  /** Rows one statement may return (all levels); default `maxRowsPerStatement`. */
  maxRows?: number;
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
   * `needs`: the raw columns the level reads (its selection, order and
   * filter), so a source may leave out a join no selected field needs.
   */
  source: (
    c: Ctx,
    link: string,
    args: { [name: string]: unknown },
    needs: ReadonlySet<string>
  ) => string;
  /** The `where` argument as a predicate on child rows. */
  /** The `where` argument as a predicate on child rows (`source`: the level's restricted rows). */
  where?: (c: Ctx, where: unknown, source: string) => string;
  /**
   * The source runs a recursive CTE. A scalar subquery containing one is
   * evaluated again at every reference (ClickHouse 26.8), so this level and
   * every level below it are evaluated in the statement's pipeline instead:
   * their rows are MATERIALIZED CTEs (`<level>_rows`, computed once there)
   * and their arrays are cross-joined one-row subqueries of the final SELECT.
   */
  recursive?: boolean;
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

/**
 * The levels of one statement. Each level (the root page included) is a
 * scalar subquery of the statement's `WITH` clause returning ALL its rows as
 * one array of tuples (`groupArray(tuple(…))`); a relationship's rows are read
 * by the keys of its parent level's array (`IN (SELECT tupleElement(__r, i)
 * FROM (SELECT arrayJoin(parent) AS __r))`), and the statement returns the
 * arrays (one row). The rows are nested in TypeScript (`decode`).
 *
 * Why not one `MATERIALIZED` CTE per level joined back in SQL (the stage-1
 * shape): ClickHouse 26.8's analyzer re-analyses a materialized CTE's whole
 * definition, recursively, at every reference, so a 5-level document spent
 * 40-80 ms in query analysis before reading a row. A scalar subquery is
 * analysed and evaluated once and its references are constants
 * (phase2-api.md §6.1).
 */
class Statement {
  /** Key width (child key columns) of each level, by name. */
  readonly keyWidths = new Map<string, number>();

  /** The arrays of pipeline levels (`recursive`): one-row subqueries of the final SELECT. */
  readonly pipelineOutputs: string[] = [];

  constructor(readonly sql: Sql) {}

  levelName(keyWidth: number) {
    const name = `__l${this.keyWidths.size + 1}`;
    this.keyWidths.set(name, keyWidth);
    return name;
  }
}

/** Rows of every level by name: child rows grouped by their parent key, sorted. */
type LevelData = Map<string, Map<string, unknown[][]>>;

interface LevelPlan {
  /** Tuple elements (SQL over the level row's raw columns), after the level's prefix. */
  elements: string[];
  /** Raw columns the level row must carry. */
  needs: Set<string>;
  /** Tuple elements (after the prefix) → decoded row. */
  decode: (values: unknown[], data: LevelData, count: { rows: number }) => Row;
}

/** A key column as text, for matching parent and child rows in TypeScript (lossless for bytes). */
const keyText = (column: string) => `hex(toString(${column}))`;

/** Values of a tuple → the matching key (null: matches nothing, like SQL). */
const keyOf = (values: readonly unknown[]) =>
  values.some((value) => value === null || value === undefined)
    ? undefined
    : values.map(String).join('\u0000');

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
 * Plan one level: `level` is the name of its array (a `WITH` scalar), whose
 * tuples hold `prefix` fixed elements followed by `elements`.
 */
const planLevel = (
  st: Statement,
  entity: Entity,
  sels: readonly Sel[],
  level: string,
  prefix: number,
  c: Ctx,
  pipeline = false
): LevelPlan => {
  const elements: string[] = [];
  const needs = new Set<string>();
  const element = (sql: string) => {
    const existing = elements.indexOf(sql);
    if (existing !== -1) return existing;
    elements.push(sql);
    return elements.length - 1;
  };
  const decoders: ((
    values: unknown[],
    record: { [key: string]: unknown },
    related: { [key: string]: unknown },
    data: LevelData,
    count: { rows: number }
  ) => void)[] = [];
  const { sql } = st;
  expandComputed(entity, sels).forEach((sel) => {
    const field = entity.fields[sel.name]!;
    if (field.kind === 'scalar') {
      const position = element(field.sql);
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
    const child = st.levelName(field.childKeys.length);
    /* the child's WITH items go here: after this level's, before its own children's */
    const reserved = sql.withItems.length;
    sql.withItems.push('');
    const childPrefix = 1 + field.childKeys.length;
    const childPipeline = pipeline || field.recursive === true;
    const plan = planLevel(
      st,
      target,
      sel.sels ?? [],
      child,
      childPrefix,
      childCtx,
      childPipeline
    );
    const linkColumns = field.linkColumns ?? field.parentKeys;
    [...linkColumns, ...field.parentKeys].forEach((column) =>
      needs.add(column)
    );
    const linkPositions = linkColumns.map(element);
    const parentKeyPositions = field.parentKeys.map((column) =>
      element(keyText(column))
    );
    const link = pipeline
      ? `SELECT ${linkColumns.join(', ')} FROM ${level}_rows`
      : `SELECT ${linkColumns
          .map(
            (column, index) =>
              `tupleElement(__r, ${
                prefix + linkPositions[index]! + 1
              }) AS ${column}`
          )
          .join(', ')} FROM (SELECT arrayJoin(${level}) AS __r)`;
    const order = field.order?.(sel.args) ?? field.defaultOrder;
    order.forEach((term) => {
      referencedColumns(term.expr, target.columns).forEach((column) =>
        plan.needs.add(column)
      );
    });
    const filtered =
      field.where !== undefined &&
      sel.args.where !== undefined &&
      sel.args.where !== null;
    const mark = sql.withItems.length;
    const source = field.source(
      childCtx,
      link,
      sel.args,
      /* a filter may read any column */
      filtered ? new Set(target.columns) : plan.needs
    );
    const where = filtered
      ? field.where!(childCtx, sel.args.where, source)
      : '1';
    const limit = toLimit(sel.args.limit, `${sel.key}.limit`);
    const offset = toLimit(sel.args.offset, `${sel.key}.offset`);
    const added = sql.withItems.splice(mark);
    const pk = field.childKeys
      .map((key, index) => `${key} AS __pk${index}`)
      .join(', ');
    const childNeeds = [...plan.needs];
    const rowNumber = `row_number() OVER (PARTITION BY ${field.childKeys.join(
      ', '
    )}${order.length === 0 ? '' : ` ORDER BY ${orderSql(order)}`}) AS __rn`;
    /*
     * A per-parent limit is applied first with `LIMIT n BY` (a top-n per key,
     * cheaper than numbering every row: Node.accepted_blocks(limit: 1)
     * numbered all 327k chipnet blocks, 65 -> 27 ms); the row number is then
     * computed over the kept rows only.
     */
    const kept =
      limit === undefined
        ? undefined
        : sql.bind(limit + (offset ?? 0), 'UInt64');
    const rows =
      kept === undefined
        ? `SELECT * FROM (${source}) WHERE ${where}`
        : `SELECT * FROM (${source}) WHERE ${where} ORDER BY ${[
            ...field.childKeys,
            ...(order.length === 0 ? [] : [orderSql(order)]),
          ].join(', ')} LIMIT ${kept} BY ${field.childKeys.join(', ')}`;
    const bounds = [
      ...(kept === undefined ? [] : [`__rn <= ${kept}`]),
      ...(offset === undefined ? [] : [`__rn > ${sql.bind(offset, 'UInt64')}`]),
    ];
    const childTuple = [
      '__rn',
      ...field.childKeys.map((_, index) => keyText(`__pk${index}`)),
      ...plan.elements,
    ];
    const body = `SELECT * FROM (SELECT ${pk}${
      childNeeds.length === 0 ? '' : `, ${childNeeds.join(', ')}`
    }, ${rowNumber} FROM (${rows}))${
      bounds.length === 0 ? '' : ` WHERE ${bounds.join(' AND ')}`
    }`;
    const array = `groupArray(tuple(${childTuple.join(', ')}))`;
    if (childPipeline) {
      sql.withItems[reserved] = [
        ...added,
        `${child}_rows AS MATERIALIZED (${body})`,
      ].join(',\n');
      st.pipelineOutputs.push(
        `(SELECT ${array} AS ${child} FROM ${child}_rows)`
      );
    } else {
      sql.withItems[reserved] = [
        ...added,
        `(SELECT ${array} FROM (${body})) AS ${child}`,
      ].join(',\n');
    }
    decoders.push((values, _record, related, data, count) => {
      const key = keyOf(parentKeyPositions.map((position) => values[position]));
      const tuples = key === undefined ? [] : data.get(child)?.get(key) ?? [];
      const items = tuples.map((tuple) => {
        count.rows += 1;
        return plan.decode(tuple.slice(childPrefix), data, count);
      });
      related[sel.key] = field.many ? items : items[0] ?? null;
    });
  });
  return {
    decode: (values, data, count) => {
      const record: { [key: string]: unknown } = {};
      const related: { [key: string]: unknown } = {};
      decoders.forEach((decode) => {
        decode(values, record, related, data, count);
      });
      return withRelations(record, related);
    },
    elements,
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

/** `__lN` arrays of a result row → child tuples grouped by parent key, in row-number order. */
const levelData = (
  row: { [column: string]: unknown },
  keyWidths: ReadonlyMap<string, number>
): LevelData => {
  const data: LevelData = new Map();
  Object.entries(row).forEach(([name, value]) => {
    if (!name.startsWith('__l')) return;
    const groups = new Map<string, unknown[][]>();
    (Array.isArray(value) ? value : []).forEach((item) => {
      const tuple = tupleValues(item);
      /* [__rn, key texts…, elements…] */
      const keyLength = keyWidths.get(name) ?? 0;
      const key = keyOf(tuple.slice(1, 1 + keyLength));
      if (key === undefined) return;
      const group = groups.get(key);
      if (group === undefined) groups.set(key, [tuple]);
      else group.push(tuple);
    });
    groups.forEach((group) =>
      group.sort((a, b) => Number(a[0]) - Number(b[0]))
    );
    data.set(name, groups);
  });
  return data;
};

/** Compile one root field: root rows from `root`, every selected relationship nested. One statement. */
export const compileRoot = (
  sql: Sql,
  c: Ctx,
  root: RootQuery,
  sels: readonly Sel[]
): CompiledRoot => {
  const st = new Statement(sql);
  /* the page's WITH item: after the root's own (candidates …), before the levels' */
  const reserved = sql.withItems.length;
  sql.withItems.push('');
  const plan = planLevel(st, root.entity, sels, '__page', 1, c);
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
  const cap = c.maxRows ?? maxRowsPerStatement;
  const checkCap = (rows: number) => {
    if (rows > cap) {
      throw new GraphQLError(
        `The result has ${rows} rows (limit ${cap}): page it with limit/after, or limit the nested lists.`,
        { extensions: { code: 'RESULT_TOO_LARGE' } }
      );
    }
  };
  if (st.keyWidths.size === 0) {
    /* no relationship: the page itself, one row per root row */
    sql.withItems.splice(reserved, 1);
    const cursorColumns = root.order.map(
      (term, index) => `${cursorExpr(term)} AS __c${index}`
    );
    return {
      decode: (rows) => {
        checkCap(rows.length);
        const none: LevelData = new Map();
        const count = { rows: rows.length };
        return {
          cursors: rows.map((row) =>
            root.order.map((_, index) => String(row[`__c${index}`]))
          ),
          rows: rows.map((row) =>
            plan.decode(tupleValues(row.__row).slice(1), none, count)
          ),
        };
      },
      sql: `${sql.withClause()}SELECT tuple(0${
        plan.elements.length === 0 ? '' : `, ${plan.elements.join(', ')}`
      }) AS __row${
        cursorColumns.length === 0 ? '' : `, ${cursorColumns.join(', ')}`
      } FROM (${page}) AS page${order === '' ? '' : ` ORDER BY ${order}`}`,
    };
  }
  /* the page's tuples: [__pos, elements…, cursor values…] */
  const cursorStart = plan.elements.length;
  const elements = [...plan.elements, ...root.order.map(cursorExpr)];
  sql.withItems[reserved] = `(SELECT groupArray(tuple(__pos, ${elements.join(
    ', '
  )})) FROM (SELECT *, row_number() OVER (${
    order === '' ? '' : `ORDER BY ${order}`
  }) AS __pos FROM (${page}))) AS __page`;
  return {
    decode: (rows) => {
      const [row] = rows;
      const tuples = (Array.isArray(row?.__page) ? row.__page : [])
        .map(tupleValues)
        .sort((a, b) => Number(a[0]) - Number(b[0]));
      const data = levelData(row ?? {}, st.keyWidths);
      const count = { rows: tuples.length };
      const decoded = tuples.map((tuple) =>
        plan.decode(tuple.slice(1), data, count)
      );
      checkCap(count.rows);
      return {
        cursors: tuples.map((tuple) =>
          root.order.map((_, index) => String(tuple[1 + cursorStart + index]))
        ),
        rows: decoded,
      };
    },
    sql: `${sql.withClause()}SELECT __page, ${[...st.keyWidths.keys()].join(
      ', '
    )}${
      st.pipelineOutputs.length === 0
        ? ''
        : ` FROM ${st.pipelineOutputs
            .map((output, index) => `${output} AS __o${index}`)
            .join(' CROSS JOIN ')}`
    }`,
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
