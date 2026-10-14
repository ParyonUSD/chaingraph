/* eslint-disable max-lines, camelcase, @typescript-eslint/naming-convention, complexity, functional/no-throw-statement, @typescript-eslint/no-magic-numbers, functional/no-mixed-type, max-params, functional/no-let, no-underscore-dangle, functional/no-loop-statement, functional/no-try-statement, @typescript-eslint/init-declarations, @typescript-eslint/parameter-properties, @typescript-eslint/strict-boolean-expressions */
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
  /** Match relationship levels by group numbers (`GroupLink`); false: by key texts (default true). */
  groupNumbers?: boolean;
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
  /** Raw columns a `where` may read (default: all), in `source`'s `needs` when filtered. */
  filterColumns?: readonly string[];
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
  /**
   * A to-one relationship the parent's source can also join in
   * (fix-pass-11.md §1): when the selection under it is only scalar fields
   * of the target over `columns` (no arguments), the parent's source
   * provides those target columns as `${prefix}${column}` (they are in its
   * `needs`) and the child row is decoded from the parent's tuple: no level
   * of its own. NULL in `${prefix}${key}` (no matching row) is a null child.
   */
  inline?: { prefix: string; columns: readonly string[]; key: string };
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

/* A symbol key: unseen by JSON / Object.keys, a plain store (defineProperty per row: ~10 % of decoding; fix-pass-8b.md §4). */
const withRelations = (
  record: { [key: string]: unknown },
  values: { [key: string]: unknown }
) => {
  (record as Row)[relations] = values;
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

  /**
   * Per level: tuple width and the positions (1-based, prefix included) the
   * TypeScript side reads. The others carry link columns for the levels
   * below (raw hashes, internal ids) and are sent as NULL (`outputColumn`).
   */
  readonly levelOutputs = new Map<
    string,
    { width: number; client: ReadonlySet<number> }
  >();

  /** Relationships matched by group number (`GroupLink`), in planning order. */
  readonly groupLinks: GroupLink[] = [];

  constructor(readonly sql: Sql) {}

  levelName(keyWidth: number) {
    const name = `__l${this.keyWidths.size + 1}`;
    this.keyWidths.set(name, keyWidth);
    return name;
  }
}

/**
 * A relationship whose child rows are matched to parent rows by a group
 * number instead of key texts (fix-pass-10.md §1). The statement's final
 * SELECT numbers the parent level's keys (the 1-based position of the
 * first parent row with that key, `transform` over the parent's key
 * array); each parent row carries its key's number as an extra tuple
 * element and each child row the number of its key in place of the key
 * text. A child row whose key no parent has gets 0 (matches nothing), and
 * a NULL key on either side matches nothing, as before. The 64-character
 * hash texts of every child row (~28 % of L33's result bytes) become small
 * integers, and the client groups by array index instead of hashing texts.
 * Not for pipeline levels (`recursive`): their arrays are not constants of
 * the final SELECT.
 */
interface GroupLink {
  parent: string;
  /** Tuple elements before the parent level's `elements` (`__pos` or `__rn` + key texts). */
  parentPrefix: number;
  /** Positions (0-based, in the parent's `elements`) of the parent's key texts. */
  keyPositions: readonly number[];
  child: string;
  childKeyWidth: number;
  /** Absolute tuple index of the parent's group number (set when the statement is assembled). */
  groupAt: number;
}

/** A level's child tuples by group: key text (`Map`) or group number (array index). */
type Groups = (unknown[][] | undefined)[] | Map<string, unknown[][]>;

/** Rows of every level by name: child rows grouped by their parent key, sorted. */
type LevelData = Map<string, Groups>;

interface LevelPlan {
  /** Tuple elements (SQL over the level row's raw columns), after the level's prefix. */
  elements: string[];
  /** Raw columns the level row must carry. */
  needs: Set<string>;
  /** Positions (0-based, in `elements`) the decoder reads. */
  clientPositions: Set<number>;
  /** Tuple elements (from `offset`, after the prefix) → decoded row. */
  decode: (
    values: readonly unknown[],
    offset: number,
    data: LevelData,
    count: { rows: number }
  ) => Row;
}

/** A key column as text for parent/child matching: lossless, equal across unsigned widths (fix-pass-9.md §4). */
const keyText = (column: string) => `lower(hex(${column}))`;

/** Values of a tuple → the matching key (null: matches nothing, like SQL). */
const keyOf = (values: readonly unknown[]) =>
  values.some((value) => value === null || value === undefined)
    ? undefined
    : values.map(String).join('\u0000');

/** `keyOf` of `width` values of `tuple` from `start`, without copying them (one column: the value itself). */
const keyAt = (tuple: readonly unknown[], start: number, width: number) => {
  const value = tuple[start];
  if (value === null || value === undefined) return undefined;
  if (width === 1) return String(value);
  const second = tuple[start + 1];
  if (width === 2) {
    return second === null || second === undefined
      ? undefined
      : `${String(value)}\u0000${String(second)}`;
  }
  return keyOf(tuple.slice(start, start + width));
};

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

/**
 * True when an inline relationship's selection (`inline`) can be decoded
 * from the parent's tuple: no arguments, and every column it reads (its
 * scalars', its relationships' link and key columns) among the columns the
 * parent provides. Not inside another inlined row (one prefix per level).
 */
const inlinable = (
  field: RelationField,
  target: Entity,
  sel: Sel,
  renamed: boolean
) => {
  const { inline } = field;
  if (inline === undefined || field.many || renamed) return false;
  if (Object.values(sel.args).some((value) => value !== undefined)) {
    return false;
  }
  const provided = new Set(inline.columns);
  return expandComputed(target, sel.sels ?? []).every((child) => {
    const childField = target.fields[child.name]!;
    if (childField.kind === 'scalar') {
      return referencedColumns(childField.sql, target.columns).every((column) =>
        provided.has(column)
      );
    }
    if (childField.kind !== 'relation') return false;
    return [
      ...(childField.linkColumns ?? childField.parentKeys),
      ...childField.parentKeys,
    ].every((column) => provided.has(column));
  });
};

/** `expr` with an entity's columns renamed to `${prefix}${column}`. */
const renameColumns = (
  expr: string,
  columns: readonly string[],
  prefix: string
) =>
  referencedColumns(expr, columns).reduce(
    (text, column) =>
      text.replace(
        new RegExp(`(?<![\\w.])${column}(?![\\w])`, 'gu'),
        `${prefix}${column}`
      ),
    expr
  );

/** A level's tuple elements, needed columns and client positions (shared by an inlined row). */
interface LevelParts {
  elements: string[];
  needs: Set<string>;
  clientPositions: Set<number>;
}

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
  pipeline = false,
  /* an inlined row (`inline`): the parent's parts, its columns as `${rename}${column}` */
  inlined?: { parts: LevelParts; rename: string }
): LevelPlan => {
  const { elements, needs, clientPositions } = inlined?.parts ?? {
    clientPositions: new Set<number>(),
    elements: [],
    needs: new Set<string>(),
  };
  const rename = inlined?.rename;
  const col = (column: string) =>
    rename === undefined ? column : `${rename}${column}`;
  const sqlOf = (expr: string) =>
    rename === undefined ? expr : renameColumns(expr, entity.columns, rename);
  const element = (sql: string) => {
    const existing = elements.indexOf(sql);
    if (existing !== -1) return existing;
    elements.push(sql);
    return elements.length - 1;
  };
  const decoders: ((
    values: readonly unknown[],
    offset: number,
    record: { [key: string]: unknown },
    related: { [key: string]: unknown },
    data: LevelData,
    count: { rows: number }
  ) => void)[] = [];
  const { sql } = st;
  expandComputed(entity, sels).forEach((sel) => {
    const field = entity.fields[sel.name]!;
    if (field.kind === 'scalar') {
      const position = element(sqlOf(field.sql));
      clientPositions.add(position);
      referencedColumns(field.sql, entity.columns).forEach((column) =>
        needs.add(col(column))
      );
      decoders.push((values, base, record) => {
        record[sel.name] = values[base + position] ?? null;
      });
      return;
    }
    if (field.kind !== 'relation') return;
    const target = field.target();
    if (inlinable(field, target, sel, rename !== undefined)) {
      /* the child row from this level's tuple: no level of its own */
      const { prefix: columnPrefix, key } = field.inline!;
      const keyPosition = element(`${columnPrefix}${key}`);
      needs.add(`${columnPrefix}${key}`);
      clientPositions.add(keyPosition);
      const rowPlan = planLevel(
        st,
        target,
        sel.sels ?? [],
        level,
        prefix,
        c,
        pipeline,
        { parts: { clientPositions, elements, needs }, rename: columnPrefix }
      );
      decoders.push((values, base, _record, related, data, count) => {
        const present = values[base + keyPosition];
        related[sel.key] =
          present === null || present === undefined
            ? null
            : rowPlan.decode(values, base, data, count);
      });
      return;
    }
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
      needs.add(col(column))
    );
    const linkPositions = linkColumns.map((column) => element(col(column)));
    const parentKeyPositions = field.parentKeys.map((column) =>
      element(keyText(col(column)))
    );
    /* group numbers for constant levels; key texts (read by the client) for pipeline levels */
    const groupLink: GroupLink | undefined =
      childPipeline || c.groupNumbers === false
        ? undefined
        : {
            child,
            childKeyWidth: field.childKeys.length,
            groupAt: -1,
            keyPositions: parentKeyPositions,
            parent: level,
            parentPrefix: prefix,
          };
    if (groupLink === undefined) {
      parentKeyPositions.forEach((position) => clientPositions.add(position));
    } else {
      st.groupLinks.push(groupLink);
    }
    const link = pipeline
      ? `SELECT ${linkColumns
          .map((column) =>
            col(column) === column ? column : `${col(column)} AS ${column}`
          )
          .join(', ')} FROM ${level}_rows`
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
      /* a filter may read any column it names (default: any) */
      filtered
        ? new Set([...plan.needs, ...(field.filterColumns ?? target.columns)])
        : plan.needs
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
    st.levelOutputs.set(child, {
      client: new Set([
        ...Array.from({ length: childPrefix }, (_, index) => index + 1),
        ...[...plan.clientPositions].map(
          (position) => childPrefix + position + 1
        ),
      ]),
      width: childTuple.length,
    });
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
    /* the child level's groups, looked up once per statement result (not per parent row) */
    let lastData: LevelData | undefined;
    let groups: Groups | undefined;
    const keyWidth = parentKeyPositions.length;
    const consecutive = parentKeyPositions.every(
      (position, index) => position === parentKeyPositions[0]! + index
    );
    decoders.push((values, base, _record, related, data, count) => {
      if (data !== lastData) {
        lastData = data;
        groups = data.get(child);
      }
      let tuples: unknown[][] | undefined;
      if (groupLink === undefined) {
        const key =
          consecutive && keyWidth <= 2
            ? keyAt(values, base + parentKeyPositions[0]!, keyWidth)
            : keyOf(
                parentKeyPositions.map((position) => values[base + position])
              );
        tuples =
          key === undefined
            ? undefined
            : (groups as Map<string, unknown[][]> | undefined)?.get(key);
      } else {
        const group = values[groupLink.groupAt];
        tuples =
          typeof group === 'number' && group > 0
            ? (groups as (unknown[][] | undefined)[] | undefined)?.[group]
            : undefined;
      }
      if (tuples === undefined) {
        related[sel.key] = field.many ? [] : null;
        return;
      }
      count.rows += tuples.length;
      if (!field.many) {
        related[sel.key] = plan.decode(tuples[0]!, childPrefix, data, count);
        return;
      }
      const items = new Array<Row>(tuples.length);
      for (let index = 0; index < tuples.length; index += 1) {
        items[index] = plan.decode(tuples[index]!, childPrefix, data, count);
      }
      related[sel.key] = items;
    });
  });
  return {
    clientPositions,
    decode: (values, base, data, count) => {
      const record: { [key: string]: unknown } = {};
      const related: { [key: string]: unknown } = {};
      for (const decode of decoders) {
        decode(values, base, record, related, data, count);
      }
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
  /**
   * Statement-level values returned next to the rows (`name` → SQL over the
   * statement's WITH scalars), e.g. whether a candidate window was capped.
   * Without a relationship level they come with each row, so an empty page
   * has none.
   */
  extra?: { readonly [name: string]: string };
}

export interface CompiledRoot {
  sql: string;
  /** Result rows → decoded root rows and the cursor values of each. */
  decode: (rows: { [column: string]: unknown }[]) => {
    rows: Row[];
    cursors: string[][];
    /** `RootQuery.extra` as returned (undefined: no result row carried them). */
    extra?: { [name: string]: unknown };
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

/** Suffix of a level's result column when link-only tuple elements are sent as NULL. */
const outputSuffix = '_c';

/** The level a result column holds (`__l3` or `__l3_c`). */
const levelOf = (column: string) =>
  column.endsWith(outputSuffix)
    ? column.slice(0, -outputSuffix.length)
    : column;

/**
 * A level's array as a result column: elements no decoder reads (link
 * columns of the levels below: raw hashes, internal ids) replaced by NULL.
 * L35's inputs level carried every outpoint hash raw next to its hex text
 * (25 % of the level's bytes; fix-pass-8b.md §4).
 */
const outputColumn = (
  name: string,
  output: { width: number; client: ReadonlySet<number> } | undefined,
  groups: {
    /** The level's group number (SQL over `__t`), in place of its key texts (elements 2 …). */
    own?: { sql: string; keyWidth: number };
    /** Its key numbers as parent, appended. */
    extras?: readonly string[];
  } = {},
  /** How the final SELECT names the level's array (`groupNumbers`: a column of the wrapped scalars). */
  ref: (name: string) => string = (array) => array
) => {
  const { own, extras = [] } = groups;
  if (
    output === undefined ||
    (output.client.size >= output.width &&
      own === undefined &&
      extras.length === 0)
  ) {
    return ref(name) === name ? name : `${ref(name)} AS ${name}`;
  }
  const items = Array.from({ length: output.width }, (_, index) =>
    own !== undefined && index >= 1 && index <= own.keyWidth
      ? index === 1
        ? own.sql
        : 'NULL'
      : output.client.has(index + 1)
      ? `tupleElement(__t, ${index + 1})`
      : 'NULL'
  );
  return `arrayMap(__t -> tuple(${[...items, ...extras].join(', ')}), ${ref(
    name
  )}) AS ${name}${outputSuffix}`;
};

/** A key (texts at `elements`, SQL over `__t`) as one non-null String; `none` for a NULL key (matches nothing). */
const groupKey = (elements: readonly string[], none: string) =>
  `ifNull(${
    elements.length === 1 ? elements[0]! : `concat(${elements.join(`, ',', `)})`
  }, ${none})`;

/**
 * The group numbers of the statement's `GroupLink`s: per parent key (level
 * and key positions) one key array (a `WITH` alias), the parent's number
 * appended to its tuples, the child's in place of its key texts. Sets each
 * link's `groupAt`.
 */
const groupNumbers = (
  st: Statement,
  widths: ReadonlyMap<string, number>,
  ref: (name: string) => string
) => {
  const keys = new Map<string, { alias: string; at: number }>();
  const extras = new Map<string, string[]>();
  const own = new Map<string, { sql: string; keyWidth: number }>();
  const withItems: string[] = [];
  st.groupLinks.forEach((link) => {
    const signature = `${link.parent}:${link.keyPositions.join(',')}`;
    let key = keys.get(signature);
    if (key === undefined) {
      const alias = `__k${keys.size + 1}`;
      /* 'p' / 'c': never a hex text, and never equal to each other */
      const parentKey = groupKey(
        link.keyPositions.map(
          (position) => `tupleElement(__t, ${link.parentPrefix + position + 1})`
        ),
        `'p'`
      );
      withItems.push(
        `arrayMap(__t -> ${parentKey}, ${link.parent}) AS ${alias}`
      );
      const list = extras.get(link.parent) ?? [];
      list.push(
        `transform(${parentKey}, ${ref(alias)}, arrayEnumerate(${ref(
          alias
        )}), toUInt32(0))`
      );
      extras.set(link.parent, list);
      key = { alias, at: (widths.get(link.parent) ?? 0) + list.length - 1 };
      keys.set(signature, key);
    }
    link.groupAt = key.at;
    const childKey = groupKey(
      Array.from(
        { length: link.childKeyWidth },
        (_, index) => `tupleElement(__t, ${index + 2})`
      ),
      `'c'`
    );
    own.set(link.child, {
      keyWidth: link.childKeyWidth,
      sql: `transform(${childKey}, ${ref(key.alias)}, arrayEnumerate(${ref(
        key.alias
      )}), toUInt32(0))`,
    });
  });
  return {
    aliases: [...keys.values()].map((key) => key.alias),
    extras,
    own,
    withItems,
  };
};

/** `__lN` arrays of a result row → child tuples grouped by parent key, in row-number order. */
const levelData = (
  row: { [column: string]: unknown },
  keyWidths: ReadonlyMap<string, number>,
  grouped: ReadonlySet<string>
): LevelData => {
  const data: LevelData = new Map();
  Object.entries(row).forEach(([column, value]) => {
    if (!column.startsWith('__l')) return;
    const name = levelOf(column);
    const items = Array.isArray(value) ? (value as unknown[]) : [];
    if (grouped.has(name)) {
      /* [__rn, group number, NULL…, elements…] */
      const tuples = items.map((item) =>
        Array.isArray(item) ? (item as unknown[]) : tupleValues(item)
      );
      let groupCount = 0;
      for (const [, group] of tuples) {
        if (typeof group === 'number' && group > groupCount) groupCount = group;
      }
      /* dense (filled by push): an out-of-order index store into an empty array makes it a dictionary */
      const byGroup: (unknown[][] | undefined)[] = [];
      for (let index = 0; index <= groupCount; index += 1) {
        byGroup.push(undefined);
      }
      for (const tuple of tuples) {
        const [, group] = tuple;
        /* 0: no parent has the key */
        if (typeof group === 'number' && group > 0) {
          const members = byGroup[group];
          if (members === undefined) byGroup[group] = [tuple];
          else members.push(tuple);
        }
      }
      for (const members of byGroup) {
        if (members !== undefined && members.length > 1) {
          members.sort((a, b) => Number(a[0]) - Number(b[0]));
        }
      }
      data.set(name, byGroup);
      return;
    }
    const groups = new Map<string, unknown[][]>();
    /* [__rn, key texts…, elements…] */
    const keyLength = keyWidths.get(name) ?? 0;
    for (const item of items) {
      const tuple = Array.isArray(item)
        ? (item as unknown[])
        : tupleValues(item);
      const key = keyAt(tuple, 1, keyLength);
      const group = key === undefined ? undefined : groups.get(key);
      if (group !== undefined) group.push(tuple);
      else if (key !== undefined) groups.set(key, [tuple]);
    }
    groups.forEach((group) => {
      if (group.length > 1) group.sort((a, b) => Number(a[0]) - Number(b[0]));
    });
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
  const extraNames = Object.keys(root.extra ?? {});
  const extraColumns = extraNames.map(
    (name) => `${root.extra![name]!} AS ${name}`
  );
  const extraOf = (row: { [column: string]: unknown } | undefined) =>
    extraNames.length === 0 || row === undefined
      ? undefined
      : Object.fromEntries(extraNames.map((name) => [name, row[name]]));
  if (st.keyWidths.size === 0) {
    /* no relationship: the page itself, one row per root row */
    sql.withItems.splice(reserved, 1);
    const cursorColumns = [
      ...root.order.map((term, index) => `${cursorExpr(term)} AS __c${index}`),
      ...extraColumns,
    ];
    return {
      decode: (rows) => {
        checkCap(rows.length);
        const none: LevelData = new Map();
        const count = { rows: rows.length };
        return {
          cursors: rows.map((row) =>
            root.order.map((_, index) => String(row[`__c${index}`]))
          ),
          extra: extraOf(rows[0]),
          rows: rows.map((row) =>
            plan.decode(tupleValues(row.__row), 1, none, count)
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
  const pageOutput = {
    client: new Set([
      1,
      ...[...plan.clientPositions].map((position) => position + 2),
      ...root.order.map((_, index) => cursorStart + index + 2),
    ]),
    width: 1 + elements.length,
  };
  /*
   * With group numbers, the final SELECT reads the levels and key arrays as
   * columns of a one-row subquery: referenced directly, each `transform`
   * re-resolved its key array's alias and the scalar subquery behind it
   * (tree hash of the level's whole subquery) during analysis, a few ms per
   * relationship whatever the page's size (chipnet X5: 70 -> 119 ms;
   * wrapped: 70 ms; fix-pass-10.md §1). Not with pipeline levels (their
   * arrays come from the FROM clause).
   */
  const wrapped = st.groupLinks.length > 0 && st.pipelineOutputs.length === 0;
  const ref = wrapped ? (name: string) => `${name}_v` : (name: string) => name;
  const groups = groupNumbers(
    st,
    new Map([
      ['__page', pageOutput.width],
      ...[...st.levelOutputs].map(
        ([name, output]) => [name, output.width] as [string, number]
      ),
    ]),
    ref
  );
  sql.withItems.push(...groups.withItems);
  const wrappedColumns = [
    '__page',
    ...st.keyWidths.keys(),
    ...groups.aliases,
  ].map((name) => `${name} AS ${ref(name)}`);
  const grouped = new Set(groups.own.keys());
  return {
    decode: (rows) => {
      const [row] = rows;
      const pageColumn = row?.__page_c ?? row?.__page;
      const tuples = (Array.isArray(pageColumn) ? pageColumn : [])
        .map(tupleValues)
        .sort((a, b) => Number(a[0]) - Number(b[0]));
      const data = levelData(row ?? {}, st.keyWidths, grouped);
      const count = { rows: tuples.length };
      const decoded = tuples.map((tuple) => plan.decode(tuple, 1, data, count));
      checkCap(count.rows);
      return {
        cursors: tuples.map((tuple) =>
          root.order.map((_, index) => String(tuple[1 + cursorStart + index]))
        ),
        extra: extraOf(row),
        rows: decoded,
      };
    },
    sql: `${sql.withClause()}SELECT ${[
      outputColumn(
        '__page',
        pageOutput,
        { extras: groups.extras.get('__page') ?? [] },
        ref
      ),
      ...[...st.keyWidths.keys()].map((name) =>
        outputColumn(
          name,
          st.levelOutputs.get(name),
          {
            extras: groups.extras.get(name) ?? [],
            own: groups.own.get(name),
          },
          ref
        )
      ),
      ...extraColumns,
    ].join(', ')}${
      wrapped
        ? ` FROM (SELECT ${wrappedColumns.join(', ')})`
        : st.pipelineOutputs.length === 0
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
  cursorOf?: (row: Row, values: string[]) => string,
  onExtra?: (extra: { [name: string]: unknown } | undefined) => void
): Promise<Page> => {
  const { cursors, rows, extra } = compiled.decode(
    await db.query(label, compiled.sql, sql.params)
  );
  onExtra?.(extra);
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
