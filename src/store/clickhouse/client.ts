/* eslint-disable camelcase, @typescript-eslint/naming-convention, functional/no-try-statement, @typescript-eslint/no-magic-numbers, complexity, max-params */
// cspell:ignore clickhouse dedup retryable
/**
 * A thin wrapper over `@clickhouse/client` for the Chaingraph ClickHouse
 * store (WP4).
 *
 * - Every SQL value is bound as a `{name:Type}` query parameter; nothing is
 *   string-interpolated. Only identifiers (table and column names) are
 *   inlined, and they are validated against a strict pattern first.
 * - Commit-critical writes (`insertRowBinary`, `insertSelect`) run with
 *   `async_insert = 0` and an `insert_deduplication_token`, so a retry of the
 *   same step is idempotent (see docs/clickhouse-port/wp4-commit-and-visibility.md).
 * - Credentials are never logged: they are removed from the URL, kept out of
 *   `toString`/`toJSON`/`util.inspect`, and never placed in error messages.
 */
import { Readable } from 'node:stream';
import { finished } from 'node:stream/promises';
import { inspect } from 'node:util';

import {
  ClickHouseError,
  ClickHouseLogLevel,
  createClient,
} from '@clickhouse/client';
import type {
  ClickHouseSettings,
  ClickHouseClient as RawClickHouseClient,
} from '@clickhouse/client';

export type { ClickHouseSettings } from '@clickhouse/client';

// eslint-disable-next-line functional/no-mixed-type
export interface ClickHouseConnectionConfig {
  /** The HTTP(S) endpoint, e.g. `http://localhost:18123`. Credentials in the URL are moved to `username`/`password`. */
  url: string;
  database: string;
  username: string;
  password: string;
  /** Per-request timeout. */
  requestTimeoutMs: number;
  /**
   * HTTP sockets (the library default is 10). The writer runs many commits
   * concurrently and a block's commit waits for its parent's, so a small pool
   * turns queueing into commit latency. Default 64.
   */
  maxOpenConnections?: number;
  /**
   * Test hook: called before every attempt of a query or insert (inside the
   * retry loop); throwing simulates a transport or server error of that
   * attempt. Never set in production.
   */
  faultBeforeRequest?: (request: ClickHouseRequestInfo) => Promise<void> | void;
}

/** What `faultBeforeRequest` is told about a request. */
export interface ClickHouseRequestInfo {
  kind: 'insert' | 'query';
  sql: string;
  /** An insert's deduplication token (`seq:table:chunk`). */
  deduplicationToken?: string;
  /** A query's `query_id`, when the caller set one (`QueryOptions.queryId`). */
  queryId?: string;
}

export interface QueryParams {
  [key: string]: unknown;
}

export interface IdempotentInsertOptions {
  /** `insert_deduplication_token`: `seq:table:chunk` for data (see `dedupToken`). */
  deduplicationToken: string;
  /** Extra settings; cannot override `async_insert` or the token. */
  settings?: ClickHouseSettings;
  /** Retries after a transport or transient server error (`isRetryable`). Default 3. */
  retries?: number;
}

const defaultDatabase = 'cg';
const defaultRequestTimeoutMs = 300_000;
const defaultInsertRetries = 3;
/** Retries of a read (`query`) after a transport or transient server error. */
const defaultQueryRetries = 3;
const defaultMaxOpenConnections = 64;
const retryBaseDelayMs = 100;
const identifierPattern = /^[A-Za-z_][A-Za-z0-9_]*$/u;

/**
 * Read the connection settings from the environment
 * (`CHAINGRAPH_CLICKHOUSE_URL`, `_DATABASE`, `_USER`, `_PASSWORD`).
 */
export const clickHouseConfigFromEnv = (
  env: NodeJS.ProcessEnv = process.env
): ClickHouseConnectionConfig => {
  const url = env.CHAINGRAPH_CLICKHOUSE_URL;
  if (url === undefined || url === '') {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error('CHAINGRAPH_CLICKHOUSE_URL is not set.');
  }
  return {
    database: env.CHAINGRAPH_CLICKHOUSE_DATABASE ?? defaultDatabase,
    password: env.CHAINGRAPH_CLICKHOUSE_PASSWORD ?? '',
    requestTimeoutMs: defaultRequestTimeoutMs,
    url,
    username: env.CHAINGRAPH_CLICKHOUSE_USER ?? '',
  };
};

/**
 * Split credentials out of a URL: `http://u:p@host:8123` becomes
 * `http://host:8123` plus `u`/`p`. Explicit (non-empty) config values win;
 * the user defaults to `default`.
 */
export const splitCredentials = (
  config: ClickHouseConnectionConfig
): ClickHouseConnectionConfig => {
  const parsed = new URL(config.url);
  const urlUser = decodeURIComponent(parsed.username);
  const urlPassword = decodeURIComponent(parsed.password);
  parsed.username = '';
  parsed.password = '';
  return {
    ...config,
    password: config.password === '' ? urlPassword : config.password,
    url: parsed.toString().replace(/\/$/u, ''),
    username: config.username || urlUser || 'default',
  };
};

/** Validate and back-quote an identifier (table or column name). */
export const quoteIdentifier = (identifier: string) => {
  if (!identifierPattern.test(identifier)) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(`Invalid ClickHouse identifier: ${identifier}`);
  }
  return `\`${identifier}\``;
};

const sleep = async (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

/**
 * Server error codes that say nothing about the statement itself: the
 * connection or the server's socket read gave out (the client stalled past
 * `receive_timeout`, a reset mid-body), or the server is shedding load.
 * Retrying them is safe for a read and for a deduplicated insert.
 * 3 UNEXPECTED_END_OF_FILE, 32 ATTEMPT_TO_READ_AFTER_EOF, 202
 * TOO_MANY_SIMULTANEOUS_QUERIES, 209 SOCKET_TIMEOUT, 210 NETWORK_ERROR,
 * 252 TOO_MANY_PARTS. (G1 c1: a 209 "Timeout exceeded while reading from
 * socket (30000 ms)" failed a whole 64-block batch; docs/clickhouse-port/g1-fix-pass.md.)
 */
export const transientServerCodes: ReadonlySet<string> = new Set([
  '3',
  '32',
  '202',
  '209',
  '210',
  '252',
]);

/**
 * Transport errors (no answer: ECONNRESET "socket hang up", timeouts) and
 * transient server errors are retried; any other server error is not.
 */
export const isRetryable = (error: unknown) =>
  !(error instanceof ClickHouseError) || transientServerCodes.has(error.code);

/**
 * The client's own per-request timeout (`requestTimeoutMs`) fired: the
 * statement may still be running on the server. Retrying it repeats the
 * same work on a server that is already busy with it (golden 2026-10-11: a
 * 300 s read was retried and both ran to the driver's limit).
 */
export const isRequestTimeout = (error: unknown) =>
  error instanceof Error &&
  !(error instanceof ClickHouseError) &&
  error.message.startsWith('Timeout error');

/** Per-read options of `ClickHouseClient.query`. */
/** What an `INSERT … SELECT` wrote (the server's summary; 0 when absent). */
export interface InsertSelectSummary {
  /** Rows inserted into the target table (`result_rows`, else `written_rows`). */
  writtenRows: number;
}

export interface QueryOptions {
  /**
   * Retry a read whose request timed out on the client (default true).
   * The API turns it off: its statements carry a server-side
   * `max_execution_time` and a timeout means the statement is too costly,
   * not that the connection failed.
   */
  retryTimeouts?: boolean;
  /**
   * Abort the read (the API: the HTTP client went away). A request still
   * waiting for a socket is never sent; one in flight is closed, and the
   * caller should also `kill` its `queryId` (the server may keep running
   * a statement whose connection closed). An aborted read is not retried.
   */
  abortSignal?: AbortSignal;
  /** The statement's `query_id` (default: a random one). */
  queryId?: string;
  /**
   * Called with the rows the statement read as the server reports them
   * (`X-ClickHouse-Summary`: the larger of `read_rows` and
   * `total_rows_to_read`), when it does.
   */
  onRowsRead?: (rows: number) => void;
  /**
   * Called when the server's response starts (headers received), before
   * its rows are read and decoded: the API attributes the event loop's time
   * to requests not waiting for ClickHouse (admission.ts). Called once per
   * attempt.
   */
  onResponse?: () => void;
}

/** Rows read from an `X-ClickHouse-Summary` header (undefined if absent or unreadable). */
export const summaryRowsRead = (header: unknown): number | undefined => {
  const text = Array.isArray(header) ? (header[0] as unknown) : header;
  if (typeof text !== 'string') return undefined;
  try {
    const summary = JSON.parse(text) as {
      read_rows?: string;
      total_rows_to_read?: string;
    };
    const rows = Math.max(
      Number(summary.read_rows ?? 0),
      Number(summary.total_rows_to_read ?? 0)
    );
    return Number.isFinite(rows) ? rows : undefined;
  } catch {
    return undefined;
  }
};

/** INSERT returns no rows: discard the response body. */
const drain = async (stream: Readable) => {
  stream.resume();
  await finished(stream);
};

const idempotentSettings = (
  options: IdempotentInsertOptions
): ClickHouseSettings => {
  if (options.deduplicationToken === '') {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error('A commit-critical insert needs a deduplication token.');
  }
  return {
    ...options.settings,
    async_insert: 0,
    insert_deduplicate: 1,
    insert_deduplication_token: options.deduplicationToken,
    wait_end_of_query: 1,
  };
};

const withRetries = async <T>(
  retries: number | undefined,
  attempt: () => Promise<T>,
  retryable: (error: unknown) => boolean = isRetryable
): Promise<T> => {
  const maxRetries = retries ?? defaultInsertRetries;
  // eslint-disable-next-line functional/no-loop-statement, functional/no-let
  for (let tryIndex = 0; ; tryIndex += 1) {
    try {
      // eslint-disable-next-line no-await-in-loop
      return await attempt();
    } catch (error) {
      if (tryIndex >= maxRetries || !retryable(error)) {
        // eslint-disable-next-line functional/no-throw-statement
        throw error;
      }
      // eslint-disable-next-line no-await-in-loop
      await sleep(retryBaseDelayMs * 2 ** tryIndex);
    }
  }
};

export class ClickHouseClient {
  readonly database: string;

  /** The endpoint without credentials (safe to log). */
  readonly endpoint: string;

  private readonly raw: RawClickHouseClient;

  /** `createClient` options (without the pool size), for the kill client. */
  private readonly rawConfig: Parameters<typeof createClient>[0];

  private killer: RawClickHouseClient | undefined;

  private readonly faultBeforeRequest:
    | ((request: ClickHouseRequestInfo) => Promise<void> | void)
    | undefined;

  constructor(config: ClickHouseConnectionConfig) {
    const clean = splitCredentials(config);
    this.faultBeforeRequest = config.faultBeforeRequest;
    this.database = clean.database;
    this.endpoint = clean.url;
    this.rawConfig = {
      clickhouse_settings: {
        // UInt64/Int64 come back as JSON strings; callers parse them with BigInt.
        output_format_json_quote_64bit_integers: 1,
      },
      database: clean.database,
      log: { level: ClickHouseLogLevel.OFF },
      password: clean.password,
      request_timeout: clean.requestTimeoutMs,
      url: clean.url,
      username: clean.username,
    };
    this.raw = createClient({
      ...this.rawConfig,
      max_open_connections:
        config.maxOpenConnections ?? defaultMaxOpenConnections,
    });
  }

  static fromEnv(env: NodeJS.ProcessEnv = process.env) {
    return new ClickHouseClient(clickHouseConfigFromEnv(env));
  }

  /** Run a statement that returns no rows (DDL, `INSERT … SELECT` without a token, `ALTER`). */
  async command(
    sql: string,
    params: QueryParams = {},
    settings: ClickHouseSettings = {}
  ): Promise<void> {
    await this.raw.command({
      clickhouse_settings: settings,
      query: sql,
      query_params: params,
    });
  }

  /**
   * Run a `SELECT`; rows are decoded from `JSONEachRow` (64-bit integers as
   * strings). Reads are idempotent: a transport or transient server error
   * is retried (`defaultQueryRetries`, exponential backoff), as inserts are;
   * a client-side request timeout only when `options.retryTimeouts` is not
   * false.
   */
  async query<T = { [key: string]: unknown }>(
    sql: string,
    params: QueryParams = {},
    settings: ClickHouseSettings = {},
    options: QueryOptions = {}
  ): Promise<T[]> {
    const { abortSignal } = options;
    const retryable = (error: unknown) =>
      abortSignal?.aborted !== true &&
      (options.retryTimeouts === false
        ? !isRequestTimeout(error) && isRetryable(error)
        : isRetryable(error));
    return withRetries(
      defaultQueryRetries,
      async () => {
        await this.faultBeforeRequest?.({
          kind: 'query',
          sql,
          ...(options.queryId === undefined
            ? {}
            : { queryId: options.queryId }),
        });
        abortSignal?.throwIfAborted();
        const result = await this.raw.query({
          ...(abortSignal === undefined ? {} : { abort_signal: abortSignal }),
          clickhouse_settings: settings,
          format: 'JSONEachRow',
          query: sql,
          query_params: params,
          ...(options.queryId === undefined
            ? {}
            : { query_id: options.queryId }),
        });
        options.onResponse?.();
        const rows = await result.json<T>();
        if (options.onRowsRead !== undefined) {
          const rowsRead = summaryRowsRead(
            result.response_headers['x-clickhouse-summary']
          );
          if (rowsRead !== undefined) options.onRowsRead(rowsRead);
        }
        return rows;
      },
      retryable
    );
  }

  /**
   * `INSERT INTO table (columns) FORMAT RowBinary` with the rows in `rows`
   * (encode them with `RowBinaryWriter`). Synchronous (`async_insert = 0`),
   * deduplicated by `deduplicationToken`, retried on transport errors.
   * An empty buffer sends nothing.
   */
  async insertRowBinary(
    table: string,
    columns: readonly string[],
    rows: Uint8Array,
    options: IdempotentInsertOptions
  ): Promise<void> {
    if (rows.length === 0) {
      return;
    }
    if (columns.length === 0) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error('insertRowBinary needs at least one column.');
    }
    const sql = `INSERT INTO ${quoteIdentifier(table)} (${columns
      .map(quoteIdentifier)
      .join(', ')}) FORMAT RowBinary`;
    await withRetries(options.retries, async () => {
      await this.faultBeforeRequest?.({
        deduplicationToken: options.deduplicationToken,
        kind: 'insert',
        sql,
      });
      const result = await this.raw.exec({
        clickhouse_settings: idempotentSettings(options),
        query: sql,
        values: Readable.from([Buffer.from(rows)]),
      });
      await drain(result.stream);
    });
  }

  /**
   * A single-statement `INSERT … SELECT` (bookkeeping rows built from
   * parameters), with the same guarantees as `insertRowBinary`.
   */
  async insertSelect(
    sql: string,
    params: QueryParams,
    options: IdempotentInsertOptions
  ): Promise<void> {
    await this.insertSelectCounted(sql, params, options);
  }

  /** `insertSelect`, returning what the server reports it wrote. */
  async insertSelectCounted(
    sql: string,
    params: QueryParams,
    options: IdempotentInsertOptions
  ): Promise<InsertSelectSummary> {
    return withRetries(options.retries, async () => {
      await this.faultBeforeRequest?.({
        deduplicationToken: options.deduplicationToken,
        kind: 'insert',
        sql,
      });
      const result = await this.raw.command({
        clickhouse_settings: idempotentSettings(options),
        query: sql,
        query_params: params,
      });
      return {
        writtenRows: Number(
          result.summary?.result_rows ?? result.summary?.written_rows ?? 0
        ),
      };
    });
  }

  /**
   * `KILL QUERY … ASYNC` of a statement by its `query_id` (the API's
   * abandoned reads), over a small connection pool of its own so a kill
   * never waits behind the reads it is meant to stop. Never throws.
   */
  async kill(queryId: string): Promise<void> {
    this.killer ??= createClient({
      ...this.rawConfig,
      max_open_connections: 2,
      request_timeout: 10_000,
    });
    try {
      await this.killer.command({
        query: 'KILL QUERY WHERE query_id = {id:String} ASYNC',
        query_params: { id: queryId },
      });
    } catch {
      /* best effort: the statement's own budget still bounds it */
    }
  }

  /** True if the server answers; never throws. */
  async ping(): Promise<boolean> {
    try {
      const result = await this.raw.ping();
      return result.success;
    } catch {
      return false;
    }
  }

  async close(): Promise<void> {
    await this.killer?.close();
    await this.raw.close();
  }

  toString() {
    return `ClickHouseClient(${this.endpoint}, database ${this.database})`;
  }

  toJSON() {
    return { database: this.database, endpoint: this.endpoint };
  }

  [inspect.custom]() {
    return this.toString();
  }
}
