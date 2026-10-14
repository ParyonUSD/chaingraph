/* eslint-disable functional/no-loop-statement, functional/no-let, functional/no-throw-statement, functional/no-try-statement, no-continue, no-await-in-loop, complexity, @typescript-eslint/naming-convention, @typescript-eslint/no-magic-numbers */
// cspell:ignore clickhouse
/**
 * Start-up warm-up (fix-pass-11.md §5): before the API listens, it runs a
 * list of requests (trusted documents with representative variables)
 * through its own in-process server, so the first client requests find
 *
 * - the admission cost model seeded: a document nothing is known about is
 *   classed heavy and charged the default (`unknownCost`); after the
 *   warm-up its plan key has a measured cost and class;
 * - ClickHouse's caches loaded for the tables and projections those
 *   documents read (primary indexes, marks, uncompressed blocks): golden
 *   mainnet's first two AllHolders after an API start took 102 / 110 ms
 *   against 19 ms warm (fix-pass-10 session, U3);
 * - V8's code for the compile, decode and write paths compiled.
 *
 * CHAINGRAPH_API_WARMUP names a JSON file: an array of
 * `{ "query" | "documentId", "variables"?, "operationName"? }` (other fields
 * ignored: a catalogue soak-mix file serves). A missing file is no warm-up. A
 * failing entry is logged and skipped; the warm-up never stops the start. The
 * whole warm-up is bounded by CHAINGRAPH_API_WARMUP_TIMEOUT_MS (default
 * 120 s): entries after the deadline are skipped.
 */
import { existsSync, readFileSync } from 'node:fs';

export interface WarmupEntry {
  query?: string;
  documentId?: string;
  operationName?: string;
  variables?: { [name: string]: unknown };
}

export interface WarmupResult {
  entries: number;
  ok: number;
  failed: number;
  skipped: number;
  ms: number;
}

/** Default bound of the whole warm-up, ms. */
export const defaultWarmupTimeoutMs = 120_000;

/**
 * Warm-up entries from a JSON file (an array of entries); none when the file
 * does not exist (an optional mount not created yet).
 */
export const loadWarmupEntries = (path: string): WarmupEntry[] => {
  if (!existsSync(path)) return [];
  const parsed: unknown = JSON.parse(readFileSync(path, 'utf8'));
  if (!Array.isArray(parsed)) {
    throw new Error(`${path}: a warm-up file is a JSON array of requests.`);
  }
  return parsed.map((entry: unknown, index) => {
    const value = entry as WarmupEntry | null;
    if (
      value === null ||
      typeof value !== 'object' ||
      (typeof value.query !== 'string' && typeof value.documentId !== 'string')
    ) {
      throw new Error(
        `${path}: entry ${index} needs a "query" or a "documentId".`
      );
    }
    return value;
  });
};

type Fetch = (url: string, init: RequestInit) => Promise<Response>;

/**
 * Run the entries one after another through `fetch` (an API's
 * `yoga.fetch`), each `rounds` times. Errors (in the response or thrown)
 * count as failed; they never throw.
 */
export const runWarmup = async (
  fetch: Fetch,
  entries: readonly WarmupEntry[],
  options: {
    rounds?: number;
    timeoutMs?: number;
    log?: (line: string) => void;
  } = {}
): Promise<WarmupResult> => {
  const started = performance.now();
  const deadline = started + (options.timeoutMs ?? defaultWarmupTimeoutMs);
  const rounds = Math.max(1, options.rounds ?? 1);
  const result: WarmupResult = {
    entries: entries.length,
    failed: 0,
    ms: 0,
    ok: 0,
    skipped: 0,
  };
  for (let round = 0; round < rounds; round += 1) {
    for (const [index, entry] of entries.entries()) {
      if (performance.now() > deadline) {
        result.skipped += 1;
        continue;
      }
      try {
        /* only the request's own fields (a soak-mix file also carries `id` and a v1 twin) */
        const { documentId, operationName, query, variables } = entry;
        const response = await fetch('http://localhost/graphql', {
          body: JSON.stringify({ documentId, operationName, query, variables }),
          headers: { 'content-type': 'application/json' },
          method: 'POST',
        });
        const body = (await response.json()) as { errors?: unknown[] };
        if (body.errors !== undefined && body.errors.length > 0) {
          result.failed += 1;
          options.log?.(
            `warm-up entry ${index}: ${JSON.stringify(body.errors).slice(
              0,
              300
            )}`
          );
        } else {
          result.ok += 1;
        }
      } catch (error: unknown) {
        result.failed += 1;
        options.log?.(`warm-up entry ${index}: ${String(error)}`);
      }
    }
  }
  result.ms = Math.round(performance.now() - started);
  return result;
};
