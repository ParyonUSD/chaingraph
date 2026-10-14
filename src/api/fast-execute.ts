/* eslint-disable functional/no-throw-statement, functional/no-let, functional/no-loop-statement, max-classes-per-file, complexity, max-params, @typescript-eslint/init-declarations, no-control-regex, @typescript-eslint/parameter-properties, @typescript-eslint/member-ordering, functional/no-try-statement, @typescript-eslint/no-magic-numbers */
// cspell:ignore clickhouse serialises serialised serialisation
/**
 * The response of a query operation written as JSON directly (fix-pass-9.md
 * §4), instead of graphql-js building a result tree that Yoga then
 * stringifies. On mainnet L33 / L35 spent 311 / 581 ms of API CPU after
 * their statements; graphql-js execution, the result tree's garbage and
 * `JSON.stringify` were about half of it locally.
 *
 * The executor follows graphql-js's algorithm with graphql-js's own field
 * collection (fragments, `@skip` / `@include`), argument coercion and
 * resolve info, so it answers exactly what graphql-js answers, for the
 * subset it handles:
 * - query operations; root fields resolved concurrently, as graphql-js does;
 * - below the roots, synchronous resolvers only (a promise, an error, a
 *   non-null violation, an abstract type or introspection: it gives up);
 * - a resolver that can read only its parent (one parameter) is called
 *   with its parent alone, and a registered relationship resolver
 *   (`keyResolvers`: its parent and `info.path.key`, schema.ts) with its
 *   parent and path, without building a resolve info per row and field.
 * Giving up falls back to graphql-js on the same operation; the root
 * fields' values (or errors) are reused (`rootMemo`), so no statement runs
 * twice and graphql-js reports errors exactly as always.
 */
import type {
  DocumentNode,
  ExecutionArgs,
  ExecutionResult,
  FieldNode,
  GraphQLField,
  GraphQLObjectType,
  GraphQLOutputType,
  GraphQLSchema,
} from 'graphql';
import {
  isAbstractType,
  isLeafType,
  isListType,
  isNonNullType,
  isObjectType,
} from 'graphql';
import {
  collectFields,
  collectSubfields,
} from 'graphql/execution/collectFields.js';
import type { ExecutionContext } from 'graphql/execution/execute.js';
import {
  buildExecutionContext,
  buildResolveInfo,
  defaultFieldResolver,
  getFieldDef,
} from 'graphql/execution/execute.js';
import { getArgumentValues } from 'graphql/execution/values.js';
import type { Path } from 'graphql/jsutils/Path.js';
import { addPath } from 'graphql/jsutils/Path.js';

/** Resolvers that read only their parent and `info.path.key` (schema.ts registers them). */
// eslint-disable-next-line @typescript-eslint/ban-types
export const keyResolvers = new WeakSet<Function>();

/** The root values of an execution, by response key (the fallback reuses them). */
const rootMemo = Symbol('rootMemo');

interface MemoRoot {
  [rootMemo]?: Map<string, Promise<unknown>>;
}

/** Why the fast path gave up (stats). */
class GiveUp extends Error {}

const isPromiseLike = (value: unknown): boolean =>
  value !== null &&
  (typeof value === 'object' || typeof value === 'function') &&
  typeof (value as { then?: unknown }).then === 'function';

type Resolver = (
  source: unknown,
  args: unknown,
  context: unknown,
  info: unknown
) => unknown;

/**
 * Every root field of the schema's query type looks up the execution's
 * memo first (the fallback after the fast path gave up): one resolution
 * per root field and request. Installed once per schema.
 */
const installed = new WeakSet<GraphQLSchema>();
const installRootMemo = (schema: GraphQLSchema) => {
  if (installed.has(schema)) return;
  installed.add(schema);
  const query = schema.getQueryType();
  if (query === null || query === undefined) return;
  Object.values(query.getFields()).forEach((field) => {
    const resolve = (field.resolve ?? defaultFieldResolver) as Resolver;
    const memoised: Resolver = (source, args, context, info) => {
      const memo = (source as MemoRoot | null | undefined)?.[rootMemo];
      const key = String((info as { path: Path }).path.key);
      const known = memo?.get(key);
      if (known !== undefined) return known;
      const value = resolve(source, args, context, info);
      memo?.set(key, Promise.resolve(value));
      return value;
    };
    (field as { resolve?: Resolver }).resolve = memoised;
  });
};

export interface FastExecutionStats {
  /** Operations answered by the fast path. */
  fast: number;
  /** Operations handed to graphql-js after the fast path gave up. */
  gaveUp: number;
  /** Operations not attempted (not a query, invalid arguments). */
  skipped: number;
}

export const fastExecutionStats: FastExecutionStats = {
  fast: 0,
  gaveUp: 0,
  skipped: 0,
};

/** A result whose JSON is already written (Yoga calls `stringify`). */
const writtenResult = (text: string): ExecutionResult => {
  let parsed: ExecutionResult | undefined;
  const result = {
    stringify: () => text,
  } as unknown as ExecutionResult & { stringify: () => string };
  Object.defineProperty(result, 'data', {
    enumerable: true,
    get: () => {
      parsed ??= JSON.parse(text) as ExecutionResult;
      return parsed.data;
    },
  });
  return result;
};

/** How a field's resolver is called. */
type CallMode = 'generic' | 'key' | 'parent';

interface FieldPlan {
  responseName: string;
  /** `"name":` (the first field) and `,"name":`. */
  first: string;
  next: string;
  fieldDef: GraphQLField<unknown, unknown>;
  nodes: readonly FieldNode[];
  mode: CallMode;
  /** The resolve info a `key` resolver reads (the same for every row). */
  keyInfo: { path: { key: string } };
}

interface ObjectPlan {
  fields: FieldPlan[];
  /** A generic resolver below: paths must be built. */
  needsPath: boolean;
}

/** A string JSON writes as itself between quotes. */
const plainString = /^[^"\\\u0000-\u001f\u007f-\uffff]*$/u;

class Writer {
  /** The response's JSON, in pieces (joined once). */
  readonly out: string[] = [];

  private readonly plans = new WeakMap<
    readonly FieldNode[],
    Map<GraphQLObjectType, ObjectPlan>
  >();

  constructor(private readonly context: ExecutionContext) {}

  private planOf(type: GraphQLObjectType, nodes: readonly FieldNode[]) {
    let byType = this.plans.get(nodes);
    if (byType === undefined) {
      byType = new Map();
      this.plans.set(nodes, byType);
    }
    let plan = byType.get(type);
    if (plan !== undefined) return plan;
    const { schema, fragments, variableValues } = this.context;
    const fields: FieldPlan[] = [];
    let needsPath = false;
    for (const [responseName, fieldNodes] of collectSubfields(
      schema,
      fragments,
      variableValues,
      type,
      nodes
    )) {
      const fieldDef = getFieldDef(schema, type, fieldNodes[0]!);
      if (fieldDef !== undefined && fieldDef !== null) {
        const own = fieldDef.resolve as Resolver | undefined;
        /* one parameter: it cannot read anything but its parent */
        const mode: CallMode =
          own !== undefined && own.length === 1
            ? 'parent'
            : own !== undefined && keyResolvers.has(own)
            ? 'key'
            : 'generic';
        if (mode === 'generic') needsPath = true;
        const quoted = JSON.stringify(responseName);
        fields.push({
          fieldDef,
          first: `${quoted}:`,
          keyInfo: { path: { key: responseName } },
          mode,
          next: `,${quoted}:`,
          nodes: fieldNodes,
          responseName,
        });
      }
    }
    plan = { fields, needsPath };
    byType.set(type, plan);
    /* a path is needed when any object below needs one: decided as the plans below are built */
    return plan;
  }

  /** Write a completed value (graphql-js `completeValue`). `path`: undefined when nothing below reads it. */
  value(
    type: GraphQLOutputType,
    nodes: readonly FieldNode[],
    path: Path | undefined,
    result: unknown
  ): void {
    if (result instanceof Error) throw new GiveUp('error');
    if (isNonNullType(type)) {
      if (result === null || result === undefined) throw new GiveUp('null');
      const mark = this.out.length;
      this.value(type.ofType, nodes, path, result);
      if (this.out.length === mark + 1 && this.out[mark] === 'null') {
        throw new GiveUp('null');
      }
      return;
    }
    if (result === null || result === undefined) {
      this.out.push('null');
      return;
    }
    if (isListType(type)) {
      if (!Array.isArray(result)) throw new GiveUp('list');
      const { out } = this;
      out.push('[');
      for (let index = 0; index < result.length; index += 1) {
        const item: unknown = result[index];
        if (isPromiseLike(item)) throw new GiveUp('promise');
        if (index > 0) out.push(',');
        this.value(
          type.ofType,
          nodes,
          path === undefined ? undefined : addPath(path, index, undefined),
          item
        );
      }
      out.push(']');
      return;
    }
    if (isLeafType(type)) {
      const serialized: unknown = type.serialize(result);
      if (serialized === undefined) throw new GiveUp('serialize');
      if (typeof serialized === 'string' && plainString.test(serialized)) {
        this.out.push('"', serialized, '"');
      } else {
        this.out.push(JSON.stringify(serialized));
      }
      return;
    }
    if (isAbstractType(type) || !isObjectType(type)) {
      throw new GiveUp('abstract');
    }
    if (type.isTypeOf !== undefined) throw new GiveUp('isTypeOf');
    this.object(type, nodes, path, result);
  }

  object(
    type: GraphQLObjectType,
    nodes: readonly FieldNode[],
    path: Path | undefined,
    source: unknown
  ): void {
    const plan = this.planOf(type, nodes);
    const { out } = this;
    out.push('{');
    const { fields } = plan;
    for (let index = 0; index < fields.length; index += 1) {
      const field = fields[index]!;
      const fieldPath =
        path === undefined
          ? undefined
          : addPath(path, field.responseName, type.name);
      const value = this.resolve(field, type, fieldPath, source);
      if (isPromiseLike(value)) throw new GiveUp('promise');
      out.push(index === 0 ? field.first : field.next);
      this.value(field.fieldDef.type, field.nodes, fieldPath, value);
    }
    out.push('}');
  }

  private resolve(
    field: FieldPlan,
    parentType: GraphQLObjectType,
    path: Path | undefined,
    source: unknown
  ): unknown {
    const { fieldDef, mode } = field;
    try {
      if (mode === 'parent') {
        return (fieldDef.resolve as Resolver)(
          source,
          undefined,
          undefined,
          undefined
        );
      }
      if (mode === 'key') {
        return (fieldDef.resolve as Resolver)(
          source,
          undefined,
          undefined,
          field.keyInfo
        );
      }
      if (path === undefined) throw new GiveUp('path');
      const { contextValue, variableValues } = this.context;
      const args = getArgumentValues(fieldDef, field.nodes[0]!, variableValues);
      const info = buildResolveInfo(
        this.context,
        fieldDef,
        field.nodes as FieldNode[],
        parentType,
        path
      );
      const resolve = (fieldDef.resolve ??
        this.context.fieldResolver) as Resolver;
      return resolve(source, args, contextValue, info);
    } catch {
      throw new GiveUp('resolver');
    }
  }

  /** Whether any object reached from `type` / `nodes` has a generic resolver (paths needed). */
  needsPath(
    type: GraphQLOutputType,
    nodes: readonly FieldNode[],
    seen = new Set<GraphQLObjectType>()
  ): boolean {
    let named: GraphQLOutputType = type;
    while (isNonNullType(named) || isListType(named)) named = named.ofType;
    if (!isObjectType(named) || seen.has(named)) return false;
    seen.add(named);
    const plan = this.planOf(named, nodes);
    return (
      plan.needsPath ||
      plan.fields.some((field) =>
        this.needsPath(field.fieldDef.type, field.nodes, seen)
      )
    );
  }
}

/**
 * Execute `args` (a query operation): the fast path, or graphql-js
 * (`fallback`) when it gives up or does not apply.
 */
export const fastExecute = async (
  args: ExecutionArgs,
  fallback: (args: ExecutionArgs) => unknown
): Promise<unknown> => {
  installRootMemo(args.schema);
  const memo = new Map<string, Promise<unknown>>();
  const rootValue: MemoRoot = { [rootMemo]: memo };
  const withMemo = { ...args, rootValue };
  const context = buildExecutionContext(withMemo);
  if (
    Array.isArray(context) ||
    (context as ExecutionContext).operation.operation !== 'query'
  ) {
    fastExecutionStats.skipped += 1;
    return fallback(args);
  }
  const exe = context as ExecutionContext;
  const { schema, fragments, variableValues, operation } = exe;
  const queryType = schema.getQueryType()!;
  const roots = [
    ...collectFields(
      schema,
      fragments,
      variableValues,
      queryType,
      operation.selectionSet
    ),
  ];
  const writer = new Writer(exe);
  try {
    const parts = await Promise.all(
      roots.map(async ([responseName, fieldNodes]) => {
        const fieldDef = getFieldDef(schema, queryType, fieldNodes[0]!);
        if (fieldDef === undefined || fieldDef === null) return undefined;
        if (responseName.startsWith('__') || fieldDef.name.startsWith('__')) {
          throw new GiveUp('introspection');
        }
        const path = addPath(undefined, responseName, queryType.name);
        const info = buildResolveInfo(
          exe,
          fieldDef,
          fieldNodes,
          queryType,
          path
        );
        const resolve = (fieldDef.resolve ?? exe.fieldResolver) as Resolver;
        const argValues = getArgumentValues(
          fieldDef,
          fieldNodes[0]!,
          variableValues
        );
        const value = await resolve(
          rootValue,
          argValues,
          exe.contextValue,
          info
        );
        return { fieldDef, fieldNodes, path, responseName, value };
      })
    );
    /* written in field order once every root resolved (graphql-js completes them as they resolve: same JSON) */
    writer.out.push('{"data":{');
    let first = true;
    for (const part of parts) {
      if (part !== undefined) {
        writer.out.push(
          `${first ? '' : ','}${JSON.stringify(part.responseName)}:`
        );
        first = false;
        writer.value(
          part.fieldDef.type,
          part.fieldNodes,
          writer.needsPath(part.fieldDef.type, part.fieldNodes)
            ? part.path
            : undefined,
          part.value
        );
      }
    }
    writer.out.push('}}');
    fastExecutionStats.fast += 1;
    return writtenResult(writer.out.join(''));
  } catch {
    /* gave up (GiveUp), or a root field failed: graphql-js reports it */
    fastExecutionStats.gaveUp += 1;
    /* graphql-js on the same operation; the root fields resolve from the memo */
    await Promise.allSettled(memo.values());
    return fallback(withMemo);
  }
};

export type FastExecutionMode = 'off' | 'on' | 'verify';

/** CHAINGRAPH_API_FAST_EXECUTION: on (default), off, or verify (tests: graphql-js too, compared). */
export const fastExecutionModeFromEnv = (
  env: NodeJS.ProcessEnv = process.env
): FastExecutionMode => {
  const raw = env.CHAINGRAPH_API_FAST_EXECUTION ?? 'on';
  if (raw === '' || raw === 'on') return 'on';
  if (raw === 'off' || raw === 'verify') return raw;
  throw new Error(
    `CHAINGRAPH_API_FAST_EXECUTION must be 'on', 'off' or 'verify'.`
  );
};

/** Yoga's serialisation of a result without errors (its `stringify`, else JSON). */
export const resultText = (result: unknown) => {
  const written = (result as { stringify?: (r: unknown) => string }).stringify;
  return typeof written === 'function'
    ? written(result)
    : JSON.stringify(result);
};

/** Deep equality of parsed JSON, object keys in any order, arrays in order. */
const sameJson = (a: unknown, b: unknown): boolean => {
  if (a === b) return true;
  if (
    a === null ||
    b === null ||
    typeof a !== 'object' ||
    typeof b !== 'object' ||
    Array.isArray(a) !== Array.isArray(b)
  ) {
    return false;
  }
  if (Array.isArray(a)) {
    const other = b as unknown[];
    return (
      a.length === other.length &&
      a.every((item, index) => sameJson(item, other[index]))
    );
  }
  const left = a as { [key: string]: unknown };
  const right = b as { [key: string]: unknown };
  const keys = Object.keys(left);
  return (
    keys.length === Object.keys(right).length &&
    keys.every((key) => key in right && sameJson(left[key], right[key]))
  );
};

/** Verify mode: fast-path answers that differed from graphql-js's (tests read and clear it). */
export const fastExecutionMismatches: {
  document: string;
  fast: string;
  graphql: string;
}[] = [];

/**
 * `verify`: the fast path's answer, then graphql-js's on the same arguments
 * (its statements run again), compared; a difference is recorded and fails
 * the request.
 */
export const verifiedExecute = async (
  args: ExecutionArgs,
  fallback: (args: ExecutionArgs) => unknown,
  documentText: (document: DocumentNode) => string
): Promise<unknown> => {
  const fast = await fastExecute(args, fallback);
  if (typeof (fast as { stringify?: unknown }).stringify !== 'function') {
    return fast;
  }
  const reference = await fallback(args);
  const fastText = resultText(fast);
  const graphqlText = JSON.stringify(reference);
  /*
   * Yoga's executor (@graphql-tools/executor) orders the root fields by
   * completion; the fast path by the document (the spec's order): compared
   * without object key order.
   */
  if (
    fastText !== graphqlText &&
    !sameJson(JSON.parse(fastText), JSON.parse(graphqlText))
  ) {
    fastExecutionMismatches.push({
      document: documentText(args.document),
      fast: fastText.slice(0, 2_000),
      graphql: graphqlText.slice(0, 2_000),
    });
    throw new Error('Fast execution differs from graphql-js (verify mode).');
  }
  return fast;
};
