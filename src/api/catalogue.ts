/* eslint-disable @typescript-eslint/naming-convention, functional/no-loop-statement, functional/no-let, no-await-in-loop, functional/no-throw-statement, @typescript-eslint/no-magic-numbers, complexity, @typescript-eslint/require-array-sort-compare, max-params, @typescript-eslint/consistent-type-definitions, functional/no-try-statement, no-unmodified-loop-condition, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/init-declarations */
// cspell:ignore clickhouse chipnet paryon
/**
 * The v2 document catalogue (docs/clickhouse-port/v2-documents): every
 * paryon_library document L1–L47 and every chaingraph.cash example X1–X16
 * ported to the v2 schema, with chipnet variables. For each document the
 * runner fetches the v1 result (Chaingraph v1: Hasura over the Postgres
 * reference `chipnet_pg`, read-only) and the v2 result (this API over the
 * ClickHouse dataset `chipnet_ch_pp2`), walking every page on both sides as
 * the library does, normalises both (hex prefix, integer types, internal ids
 * and per-sync timestamps dropped, unordered lists sorted) and compares them.
 * It also records the v2 latency (p50 of warm requests, first page).
 */
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { Api } from './server.js';

export const catalogueDirectory = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../docs/clickhouse-port/v2-documents'
);

type Variables = { [name: string]: unknown };

export interface CatalogueEntry {
  id: string;
  name: string;
  kind: 'query' | 'subscription';
  file: string;
  v1: {
    file: string;
    /** A chipnet version of the v1 document (chaingraph.cash examples carry mainnet literals). */
    query?: string;
    variables: Variables;
    root: string;
    paginate: 'inputKeyset' | 'keyset' | 'offset' | null;
  };
  v2: {
    variables: Variables;
    root: string;
    paginate: 'afterInternalId' | 'cursor' | 'offset' | null;
  };
  compare: {
    unorderedRoot?: boolean;
    wrapObject?: boolean;
    ignoreRoots?: string[];
    unorderedPaths?: string[];
  };
  /** Expected equality outcome when it is a documented divergence (default: equal). */
  expect?: string;
}

export const loadCatalogue = (): CatalogueEntry[] =>
  JSON.parse(
    readFileSync(join(catalogueDirectory, 'catalogue.json'), 'utf8')
  ) as CatalogueEntry[];

/** A document file without its `#` header lines. */
export const documentText = (file: string) =>
  readFileSync(join(catalogueDirectory, file), 'utf8')
    .split('\n')
    .filter((line) => !line.startsWith('#'))
    .join('\n');

/* ------------------------------------------------------------------ */
/* fetching                                                             */
/* ------------------------------------------------------------------ */

interface GraphQLResponse {
  data?: { [key: string]: unknown };
  errors?: { message: string }[];
}

export interface V1Endpoint {
  url: string;
  adminSecret: string;
}

export const fetchV1 = async (
  endpoint: V1Endpoint,
  query: string,
  variables: Variables
): Promise<GraphQLResponse> => {
  const response = await fetch(endpoint.url, {
    body: JSON.stringify({ query, variables }),
    headers: {
      'content-type': 'application/json',
      'x-hasura-admin-secret': endpoint.adminSecret,
    },
    method: 'POST',
  });
  return (await response.json()) as GraphQLResponse;
};

/** A v2 request through the API (in process): a query, or a subscription's first payload (SSE). */
export const fetchV2 = async (
  api: Api,
  query: string,
  variables: Variables
): Promise<GraphQLResponse> => {
  const subscription = /^\s*subscription\b/u.test(query);
  const abort = new AbortController();
  const response = await api.yoga.fetch('http://localhost/graphql', {
    body: JSON.stringify({ query, variables }),
    headers: {
      accept: subscription ? 'text/event-stream' : 'application/json',
      'content-type': 'application/json',
    },
    method: 'POST',
    signal: abort.signal,
  });
  if (!subscription) return (await response.json()) as GraphQLResponse;
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const events = buffer.split('\n\n');
      buffer = events.pop() ?? '';
      for (const event of events) {
        const data = event
          .split('\n')
          .filter((line) => line.startsWith('data:'))
          .map((line) => line.slice(5).trim())
          .join('');
        if (event.includes('event: next') && data !== '') {
          return JSON.parse(data) as GraphQLResponse;
        }
        if (event.includes('event: complete')) {
          return { errors: [{ message: 'subscription completed' }] };
        }
      }
    }
  } finally {
    abort.abort();
    await reader.cancel().catch(() => undefined);
  }
  return { errors: [{ message: 'subscription ended without a payload' }] };
};

/* ------------------------------------------------------------------ */
/* paging                                                               */
/* ------------------------------------------------------------------ */

const maxPages = 50;

const failed = (response: GraphQLResponse) =>
  response.errors !== undefined && response.errors.length > 0
    ? response.errors.map((error) => error.message).join('; ')
    : undefined;

/** Every v1 row (or the single result), walking pages as the library does. */
export const walkV1 = async (
  endpoint: V1Endpoint,
  entry: CatalogueEntry,
  query: string
): Promise<{ data: { [key: string]: unknown }; pages: number }> => {
  const v1Query = query.replace(/^\s*subscription\b/u, 'query');
  const { root, paginate, variables } = entry.v1;
  if (paginate === null) {
    const response = await fetchV1(endpoint, v1Query, variables);
    const error = failed(response);
    if (error !== undefined) throw new Error(`v1: ${error}`);
    return { data: response.data!, pages: 1 };
  }
  const limit = Number(variables.limit);
  const rows: { [key: string]: unknown }[] = [];
  let cursor: Variables =
    paginate === 'offset'
      ? { offset: 0 }
      : paginate === 'inputKeyset'
      ? { afterInputIndex: '-1', afterInternalId: '-1' }
      : { afterInternalId: '-1' };
  let pages = 0;
  for (;;) {
    const response = await fetchV1(endpoint, v1Query, {
      ...variables,
      ...cursor,
    });
    const error = failed(response);
    if (error !== undefined) throw new Error(`v1: ${error}`);
    const page = response.data![root] as { [key: string]: unknown }[];
    rows.push(...page);
    pages += 1;
    if (page.length < limit || pages >= maxPages) break;
    const last = page[page.length - 1]!;
    cursor =
      paginate === 'offset'
        ? { offset: rows.length }
        : paginate === 'inputKeyset'
        ? {
            afterInputIndex: String(last.input_index),
            afterInternalId: String(last.transaction_internal_id),
          }
        : { afterInternalId: String(last.internal_id) };
  }
  return { data: { [root]: rows }, pages };
};

interface V2Page {
  nodes: { [key: string]: unknown }[];
  has_more?: boolean;
  end_cursor?: string | null;
}

const isPage = (value: unknown): value is V2Page =>
  value !== null &&
  typeof value === 'object' &&
  Array.isArray((value as V2Page).nodes);

/** Every v2 row (or the single result), walking `has_more` pages. */
export const walkV2 = async (
  api: Api,
  entry: CatalogueEntry,
  query: string
): Promise<{ data: { [key: string]: unknown }; pages: number }> => {
  const { root, paginate, variables } = entry.v2;
  const first = await fetchV2(api, query, variables);
  const error = failed(first);
  if (error !== undefined) throw new Error(`v2: ${error}`);
  const data = { ...first.data! };
  const value = data[root];
  if (!isPage(value)) return { data, pages: 1 };
  const rows = [...value.nodes];
  let page = value;
  let pages = 1;
  while (
    paginate !== null &&
    page.has_more === true &&
    pages < maxPages &&
    !/^\s*subscription\b/u.test(query)
  ) {
    const next = await fetchV2(api, query, {
      ...variables,
      ...(paginate === 'offset'
        ? { offset: rows.length }
        : { after: page.end_cursor }),
    });
    const nextError = failed(next);
    if (nextError !== undefined) throw new Error(`v2: ${nextError}`);
    page = next.data![root] as V2Page;
    rows.push(...page.nodes);
    pages += 1;
  }
  data[root] = rows;
  return { data, pages };
};

/* ------------------------------------------------------------------ */
/* normalising and comparing                                            */
/* ------------------------------------------------------------------ */

/**
 * Keys dropped before comparing: internal ids (each store assigns its own)
 * and the timestamps each agent records at its own observation time (the two
 * stores were synced independently).
 */
const droppedKeys =
  /(?:^|_)internal_id$|^(?:validated_at|accepted_at|replaced_at|first_connected_at|latest_connection_began_at|end_cursor|has_more)$/u;

/** v1 SETOF-style computed fields (identity/signing output, authchain migration transaction). */
const singletonKeys = new Set([
  'identity_output',
  'signing_output',
  'transaction',
]);

const canonical = (value: unknown): string =>
  value === undefined ? 'undefined' : JSON.stringify(value);

const floatPattern = /^-?\d+\.\d+$/u;

export const normalise = (
  value: unknown,
  path: readonly string[] = [],
  unorderedRoot = false
): unknown => {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') {
    return Number.isInteger(value) ? String(value) : value.toFixed(4);
  }
  if (typeof value === 'string') {
    const text = value.startsWith('\\x') ? value.slice(2) : value;
    return floatPattern.test(text)
      ? Number(text).toFixed(4)
      : text.toLowerCase();
  }
  if (typeof value === 'boolean') return value;
  if (Array.isArray(value)) {
    const items = value.map((item) => normalise(item, path));
    /* nested lists are compared as sets of rows; the root keeps its order unless unordered */
    return path.length === 0 && !unorderedRoot
      ? items
      : [...items].sort((a, b) => canonical(a).localeCompare(canonical(b)));
  }
  const out: { [key: string]: unknown } = {};
  Object.keys(value as object)
    .sort()
    .forEach((key) => {
      if (droppedKeys.test(key)) return;
      let child = (value as { [key: string]: unknown })[key];
      /* v1 exposes these single-row computed fields as lists of at most one row; v2 as objects */
      if (singletonKeys.has(key) && Array.isArray(child) && child.length <= 1) {
        child = child[0] ?? null;
      }
      out[key] = normalise(child, [...path, key]);
    });
  /* v1 returns a missing single-row computed field as a row of nulls */
  const values = Object.values(out);
  return values.length > 0 && values.every((item) => item === null)
    ? null
    : out;
};

/** The first differing path of two normalised values (for the report). */
export const firstDifference = (
  a: unknown,
  b: unknown,
  path = '$'
): string | undefined => {
  if (canonical(a) === canonical(b)) return undefined;
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) {
      return `${path}: ${a.length} vs ${b.length} rows`;
    }
    for (let index = 0; index < a.length; index += 1) {
      const difference = firstDifference(
        a[index],
        b[index],
        `${path}[${index}]`
      );
      if (difference !== undefined) return difference;
    }
    return path;
  }
  if (
    a !== null &&
    b !== null &&
    typeof a === 'object' &&
    typeof b === 'object'
  ) {
    const keys = [
      ...new Set([...Object.keys(a as object), ...Object.keys(b as object)]),
    ];
    for (const key of keys) {
      const difference = firstDifference(
        (a as { [key: string]: unknown })[key],
        (b as { [key: string]: unknown })[key],
        `${path}.${key}`
      );
      if (difference !== undefined) return difference;
    }
    return path;
  }
  return `${path}: ${canonical(a).slice(0, 120)} vs ${canonical(b).slice(
    0,
    120
  )}`;
};

/** v1 and v2 data of an entry as comparable values (root by root). */
export const comparable = (
  entry: CatalogueEntry,
  v1: { [key: string]: unknown },
  v2: { [key: string]: unknown }
) => {
  const ignored = new Set(entry.compare.ignoreRoots ?? []);
  const roots = Object.keys(v1).filter((root) => !ignored.has(root));
  const shape = (data: { [key: string]: unknown }, isV2: boolean) =>
    Object.fromEntries(
      roots.map((root) => {
        let value = data[root];
        if (isV2 && entry.compare.wrapObject === true) {
          value = value === null || value === undefined ? [] : [value];
        }
        return [root, normalise(value, [], entry.compare.unorderedRoot)];
      })
    );
  return { v1: shape(v1, false), v2: shape(v2, true) };
};

/* ------------------------------------------------------------------ */
/* running                                                              */
/* ------------------------------------------------------------------ */

export interface CatalogueResult {
  id: string;
  name: string;
  /** works: the v2 document runs; error: it fails (message). */
  status: 'error' | 'works';
  error?: string;
  /** equal | equal-unordered | differs: <path> | not compared: <reason> */
  equality: string;
  v1Rows: number;
  v2Rows: number;
  v1Pages: number;
  v2Pages: number;
  /** p50 of warm requests (first page), ms. */
  v2P50?: number;
  v1P50?: number;
}

const rowCount = (data: { [key: string]: unknown }, root: string) => {
  const value = data[root];
  return Array.isArray(value) ? value.length : value === null ? 0 : 1;
};

const median = (values: number[]) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)]!;
};

const timed = async (runs: number, run: () => Promise<unknown>) => {
  const times: number[] = [];
  for (let index = 0; index < runs; index += 1) {
    const started = performance.now();
    await run();
    times.push(performance.now() - started);
  }
  return median(times);
};

/**
 * The root rows' order in the v2 result by its own (store-local) ids: the
 * two stores were synced independently, so their internal ids, and the
 * order of rows sorted by them, differ; the rows are equal as a set.
 */
const orderNote = (rows: unknown) => {
  if (!Array.isArray(rows) || rows.length < 2) return '';
  const keys = rows.map((row: { [key: string]: unknown }) => {
    const id = row.internal_id ?? row.transaction_internal_id;
    const index = row.output_index ?? row.input_index ?? 0;
    return id === undefined ? undefined : [BigInt(String(id)), Number(index)];
  });
  if (keys.some((key) => key === undefined)) {
    return ' (root order: by store-local ids, not selected)';
  }
  const compare = (a: (bigint | number)[], b: (bigint | number)[]) =>
    a[0]! < b[0]! ? -1 : a[0]! > b[0]! ? 1 : Number(a[1]) - Number(b[1]);
  const pairs = keys.slice(1).map((key, index) => compare(keys[index]!, key!));
  if (pairs.every((value) => value < 0)) {
    return ' (v2 rows ascending by v2 internal ids)';
  }
  if (pairs.every((value) => value > 0)) {
    return ' (v2 rows descending by v2 internal ids)';
  }
  return ' (v2 root order NOT by internal id)';
};

export const runEntry = async (
  api: Api,
  endpoint: V1Endpoint,
  entry: CatalogueEntry,
  options: { latencyRuns?: number; warmups?: number } = {}
): Promise<CatalogueResult> => {
  const v2Query = documentText(entry.file);
  const v1Query = entry.v1.query ?? documentText(entry.v1.file);
  const result: CatalogueResult = {
    equality: 'not compared',
    id: entry.id,
    name: entry.name,
    status: 'works',
    v1Pages: 0,
    v1Rows: 0,
    v2Pages: 0,
    v2Rows: 0,
  };
  let v2: { data: { [key: string]: unknown }; pages: number };
  try {
    v2 = await walkV2(api, entry, v2Query);
  } catch (error) {
    result.status = 'error';
    result.error = String((error as Error).message).slice(0, 500);
    result.equality = 'not compared: v2 error';
    return result;
  }
  result.v2Pages = v2.pages;
  result.v2Rows = rowCount(v2.data, entry.v2.root);
  try {
    const v1 = await walkV1(endpoint, entry, v1Query);
    result.v1Pages = v1.pages;
    result.v1Rows = rowCount(v1.data, entry.v1.root);
    const { v1: a, v2: b } = comparable(entry, v1.data, v2.data);
    const difference = firstDifference(a, b);
    if (difference === undefined) {
      result.equality = 'equal';
    } else {
      const unordered = comparable(
        { ...entry, compare: { ...entry.compare, unorderedRoot: true } },
        v1.data,
        v2.data
      );
      result.equality =
        firstDifference(unordered.v1, unordered.v2) === undefined
          ? `equal-unordered${orderNote(v2.data[entry.v2.root])}`
          : `differs: ${difference}`;
    }
  } catch (error) {
    result.equality = `not compared: ${String((error as Error).message).slice(
      0,
      300
    )}`;
  }
  const runs = options.latencyRuns ?? 0;
  if (runs > 0) {
    const asQuery = v2Query.replace(/^\s*subscription\b/u, 'query');
    const v2Run = async () => fetchV2(api, asQuery, entry.v2.variables);
    const v1Run = async () =>
      fetchV1(
        endpoint,
        v1Query.replace(/^\s*subscription\b/u, 'query'),
        entry.v1.paginate === null
          ? entry.v1.variables
          : {
              ...entry.v1.variables,
              ...(entry.v1.paginate === 'offset'
                ? { offset: 0 }
                : entry.v1.paginate === 'inputKeyset'
                ? { afterInputIndex: '-1', afterInternalId: '-1' }
                : { afterInternalId: '-1' }),
            }
      );
    for (let index = 0; index < (options.warmups ?? 2); index += 1) {
      await v2Run();
      await v1Run();
    }
    result.v2P50 = await timed(runs, v2Run);
    result.v1P50 = await timed(runs, v1Run);
  }
  return result;
};
