/* eslint-disable functional/no-throw-statement, @typescript-eslint/no-magic-numbers, complexity, functional/no-loop-statement, max-params, @typescript-eslint/naming-convention, @typescript-eslint/parameter-properties, @typescript-eslint/no-unsafe-argument */
// cspell:ignore clickhouse
/**
 * Operational parts of the v2 API (R34, R37):
 * - query limits (depth, aliases, root fields, total fields) as validation
 *   rules: refused before any snapshot is read;
 * - trusted (persisted) documents: a request names a document by its
 *   sha256 (`extensions.persistedQuery.sha256Hash` or `documentId`); with
 *   `allowArbitraryDocuments: false` nothing else runs;
 * - a response cache keyed on the document, its variables and the
 *   watermarks the response was computed at: while the watermarks stand
 *   still, a repeated request costs one small watermark query and no data
 *   query.
 */
import { createHash } from 'node:crypto';
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import type {
  ASTVisitor,
  DocumentNode,
  ExecutionResult,
  FragmentDefinitionNode,
  SelectionSetNode,
  ValidationContext,
  ValidationRule,
} from 'graphql';
import { getOperationAST, GraphQLError, Kind, print } from 'graphql';
import type { Plugin } from 'graphql-yoga';

import type { ApiDb } from './db.js';
import type { ApiContext } from './schema.js';

/* ------------------------------------------------------------------ */
/* limits                                                               */
/* ------------------------------------------------------------------ */

export interface QueryLimits {
  /** Nesting of selection sets (root fields at depth 1). */
  maxDepth: number;
  /** Aliased fields in one operation. */
  maxAliases: number;
  /** Root fields of one operation. */
  maxRootFields: number;
  /** Fields in one operation, fragments expanded. */
  maxFields: number;
}

export const defaultQueryLimits: QueryLimits = {
  maxAliases: 64,
  maxDepth: 16,
  maxFields: 2_000,
  maxRootFields: 32,
};

const tooComplex = (message: string) =>
  new GraphQLError(message, { extensions: { code: 'QUERY_TOO_COMPLEX' } });

/** One validation rule enforcing every limit (fragments expanded, cycles are validation errors anyway). */
export const queryLimitsRule =
  (limits: QueryLimits): ValidationRule =>
  (context: ValidationContext): ASTVisitor => {
    const fragments = new Map<string, FragmentDefinitionNode>();
    context.getDocument().definitions.forEach((definition) => {
      if (definition.kind === Kind.FRAGMENT_DEFINITION) {
        fragments.set(definition.name.value, definition);
      }
    });
    const measure = (
      selectionSet: SelectionSetNode,
      depth: number,
      seen: ReadonlySet<string>,
      totals: { aliases: number; depth: number; fields: number }
    ) => {
      selectionSet.selections.forEach((selection) => {
        if (selection.kind === Kind.FIELD) {
          totals.fields += 1;
          if (selection.alias !== undefined) totals.aliases += 1;
          totals.depth = Math.max(totals.depth, depth);
          if (selection.selectionSet !== undefined) {
            measure(selection.selectionSet, depth + 1, seen, totals);
          }
        } else if (selection.kind === Kind.INLINE_FRAGMENT) {
          measure(selection.selectionSet, depth, seen, totals);
        } else {
          const name = selection.name.value;
          const fragment = fragments.get(name);
          if (fragment !== undefined && !seen.has(name)) {
            measure(
              fragment.selectionSet,
              depth,
              new Set([...seen, name]),
              totals
            );
          }
        }
      });
    };
    return {
      OperationDefinition: (operation) => {
        const totals = { aliases: 0, depth: 0, fields: 0 };
        measure(operation.selectionSet, 1, new Set(), totals);
        const roots = operation.selectionSet.selections.length;
        if (totals.depth > limits.maxDepth) {
          context.reportError(
            tooComplex(
              `Query depth ${totals.depth} exceeds ${limits.maxDepth}.`
            )
          );
        }
        if (totals.aliases > limits.maxAliases) {
          context.reportError(
            tooComplex(`${totals.aliases} aliases exceed ${limits.maxAliases}.`)
          );
        }
        if (roots > limits.maxRootFields) {
          context.reportError(
            tooComplex(`${roots} root fields exceed ${limits.maxRootFields}.`)
          );
        }
        if (totals.fields > limits.maxFields) {
          context.reportError(
            tooComplex(`${totals.fields} fields exceed ${limits.maxFields}.`)
          );
        }
      },
    };
  };

export const useQueryLimits = (limits: QueryLimits): Plugin<ApiContext> => ({
  onValidate: ({ addValidationRule }) => {
    addValidationRule(queryLimitsRule(limits));
  },
});

/* ------------------------------------------------------------------ */
/* trusted documents                                                    */
/* ------------------------------------------------------------------ */

export interface TrustedDocuments {
  /** sha256 (hex) of the document text → document text. */
  documents: ReadonlyMap<string, string>;
  /** false: only trusted documents run (by hash; a full query is refused). */
  allowArbitraryDocuments: boolean;
}

export const documentHash = (query: string) =>
  createHash('sha256').update(query).digest('hex');

/**
 * Trusted documents from a directory of `.graphql` files (each file one
 * document; `#` comment lines are part of the text the hash covers).
 */
export const loadTrustedDocuments = (directory: string) =>
  new Map(
    readdirSync(directory)
      .filter((file) => file.endsWith('.graphql'))
      .map((file) => {
        const text = readFileSync(join(directory, file), 'utf8');
        return [documentHash(text), text] as const;
      })
  );

interface RequestParams {
  query?: string;
  documentId?: string;
  extensions?: { persistedQuery?: { sha256Hash?: string } };
}

/** The query text of a request (by its trusted hash), or an error. */
export const resolveTrustedDocument = (
  trusted: TrustedDocuments,
  params: RequestParams
): string => {
  const hash =
    params.documentId ?? params.extensions?.persistedQuery?.sha256Hash;
  if (hash !== undefined) {
    const document = trusted.documents.get(hash.replace(/^sha256:/u, ''));
    if (document === undefined) {
      throw new GraphQLError('PersistedQueryNotFound', {
        extensions: { code: 'PERSISTED_QUERY_NOT_FOUND' },
      });
    }
    if (
      params.query !== undefined &&
      params.query !== '' &&
      params.query !== document
    ) {
      throw new GraphQLError('The query does not match its hash.', {
        extensions: { code: 'PERSISTED_QUERY_MISMATCH' },
      });
    }
    return document;
  }
  if (!trusted.allowArbitraryDocuments || params.query === undefined) {
    throw new GraphQLError(
      'Only trusted documents run here: send extensions.persistedQuery.sha256Hash (or documentId).',
      { extensions: { code: 'PERSISTED_QUERY_ONLY' } }
    );
  }
  return params.query;
};

export const useTrustedDocuments = (
  trusted: TrustedDocuments
): Plugin<ApiContext> => ({
  onParams: ({ params, setParams }) => {
    const query = resolveTrustedDocument(trusted, params as RequestParams);
    setParams({ ...params, query });
  },
});

/* ------------------------------------------------------------------ */
/* response cache                                                       */
/* ------------------------------------------------------------------ */

export interface ResponseCacheOptions {
  /** Cached responses (least recently used are dropped). Default 1,000. */
  maxEntries?: number;
}

export interface ResponseCacheStats {
  hits: number;
  misses: number;
  /** Requests not cacheable (subscriptions, mutations, node connection metadata, errors). */
  skipped: number;
}

interface CacheEntry {
  /** node id → watermark at which the response was computed (node 0: visible0). */
  marks: Map<number, bigint>;
  result: ExecutionResult;
}

/**
 * Fields whose values change without a watermark move (node connection
 * metadata): responses selecting them are not cached.
 */
const uncachedFields =
  /\b(?:user_agent|protocol_version|first_connected_at|latest_connection_began_at)\b/u;

const cacheKey = (
  document: DocumentNode,
  operationName: string | null | undefined,
  variables: unknown
) =>
  documentHash(
    `${operationName ?? ''}\n${print(document)}\n${JSON.stringify(
      variables ?? {},
      (_key, value: unknown) =>
        typeof value === 'bigint' ? value.toString() : value
    )}`
  );

/**
 * R37: a response depends only on the document, its variables and the
 * snapshot; the snapshot is identified by the watermarks of the nodes it
 * pins and node 0 (the same rule as the snapshot cache, fix-pass-3.md §4).
 * A request first reads those watermarks (one small query); if a cached
 * response was computed at the same ones, it is returned without a snapshot
 * or data query.
 *
 * The snapshot rule, as restated by the owner (2026-10-11): every answer
 * corresponds to exactly one consistent snapshot. A miss reads one
 * (`readSnapshot(Multi)`, once for the whole request); a hit returns an
 * answer computed entirely at one earlier snapshot whose watermarks equal
 * the ones the request just read. Commits that land without moving a
 * watermark (the committed tail above visible(0), the spend watermark) can
 * be newer than a hit's snapshot, exactly as with the snapshot cache; the
 * spend watermark never changes an answer (the spend relation and the
 * authchain walk are exact at any index lag). On by default in `main.ts`
 * (`CHAINGRAPH_API_RESPONSE_CACHE=off` disables it); off in `createApi`
 * unless asked for.
 */
export class ResponseCache {
  readonly stats: ResponseCacheStats = { hits: 0, misses: 0, skipped: 0 };

  private readonly entries = new Map<string, CacheEntry>();

  private readonly maxEntries: number;

  constructor(private readonly db: ApiDb, options: ResponseCacheOptions = {}) {
    this.maxEntries = options.maxEntries ?? 1_000;
  }

  plugin(
    nodeNames: (args: {
      document: DocumentNode;
      operationName?: string | null;
      variableValues?: { [name: string]: unknown } | null;
    }) => string[]
  ): Plugin<ApiContext> {
    return {
      onExecute: async ({ args, setResultAndStopExecution }) => {
        const operation = getOperationAST(
          args.document,
          args.operationName ?? undefined
        );
        const text = print(args.document);
        if (operation?.operation !== 'query' || uncachedFields.test(text)) {
          this.stats.skipped += 1;
          return undefined;
        }
        const key = cacheKey(
          args.document,
          args.operationName,
          args.variableValues
        );
        const names = nodeNames(args as never);
        const ids = await Promise.all(
          names.map(async (name) => this.db.nodeId(name))
        );
        const marks = await this.db.watermarks([0, ...ids]);
        const cached = this.entries.get(key);
        if (
          cached !== undefined &&
          [...cached.marks].every(
            ([node, mark]) => (marks.get(node) ?? 0n) === mark
          )
        ) {
          this.stats.hits += 1;
          this.entries.delete(key);
          this.entries.set(key, cached);
          setResultAndStopExecution(cached.result);
          return undefined;
        }
        this.stats.misses += 1;
        return {
          onExecuteDone: ({ result }) => {
            const single = result as ExecutionResult;
            if (
              Symbol.asyncIterator in (result as object) ||
              (single.errors !== undefined && single.errors.length > 0)
            ) {
              return;
            }
            const snapshot = (args.contextValue as ApiContext).snapshot
              ?.resolved;
            if (snapshot === undefined) return;
            const computed = new Map<number, bigint>([
              [0, snapshot.visible0],
              ...[...snapshot.visibleByNode].filter(([node]) => node !== 0),
            ]);
            this.entries.delete(key);
            this.entries.set(key, { marks: computed, result: single });
            while (this.entries.size > this.maxEntries) {
              this.entries.delete(this.entries.keys().next().value!);
            }
          },
        };
      },
    };
  }
}
