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

/*
 * @clickhouse/client 1.24's typings use `Symbol.asyncDispose`/`Symbol.dispose`
 * (lib.esnext.disposable, TS >= 5.2) and `NodeJS.AsyncIterator` (newer
 * @types/node). This repo is on TypeScript 4.9 / @types/node 18, so declare the
 * two symbols and the alias as those libs do. Remove when the toolchain is
 * upgraded.
 */
declare global {
  interface SymbolConstructor {
    readonly asyncDispose: unique symbol;
    readonly dispose: unique symbol;
  }
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace NodeJS {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-empty-interface
    interface AsyncIterator<T, TReturn = any, TNext = undefined>
      extends globalThis.AsyncIterator<T, TReturn, TNext> {}
  }
}

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
}

export interface QueryParams {
  [key: string]: unknown;
}

export interface IdempotentInsertOptions {
  /** `insert_deduplication_token`: `seq:table:chunk` for data (see `dedupToken`). */
  deduplicationToken: string;
  /** Extra settings; cannot override `async_insert` or the token. */
  settings?: ClickHouseSettings;
  /** Retries after a transport error (not after a server error). Default 3. */
  retries?: number;
}

const defaultDatabase = 'cg';
const defaultRequestTimeoutMs = 300_000;
const defaultInsertRetries = 3;
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

/** Transport errors are retried; a server error (it answered) is not. */
const isRetryable = (error: unknown) => !(error instanceof ClickHouseError);

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

const withRetries = async (
  retries: number | undefined,
  attempt: () => Promise<void>
): Promise<void> => {
  const maxRetries = retries ?? defaultInsertRetries;
  // eslint-disable-next-line functional/no-loop-statement, functional/no-let
  for (let tryIndex = 0; ; tryIndex += 1) {
    try {
      // eslint-disable-next-line no-await-in-loop
      await attempt();
      return;
    } catch (error) {
      if (tryIndex >= maxRetries || !isRetryable(error)) {
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

  constructor(config: ClickHouseConnectionConfig) {
    const clean = splitCredentials(config);
    this.database = clean.database;
    this.endpoint = clean.url;
    this.raw = createClient({
      clickhouse_settings: {
        // UInt64/Int64 come back as JSON strings; callers parse them with BigInt.
        output_format_json_quote_64bit_integers: 1,
      },
      database: clean.database,
      log: { level: ClickHouseLogLevel.OFF },
      max_open_connections:
        config.maxOpenConnections ?? defaultMaxOpenConnections,
      password: clean.password,
      request_timeout: clean.requestTimeoutMs,
      url: clean.url,
      username: clean.username,
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

  /** Run a `SELECT`; rows are decoded from `JSONEachRow` (64-bit integers as strings). */
  async query<T = { [key: string]: unknown }>(
    sql: string,
    params: QueryParams = {},
    settings: ClickHouseSettings = {}
  ): Promise<T[]> {
    const result = await this.raw.query({
      clickhouse_settings: settings,
      format: 'JSONEachRow',
      query: sql,
      query_params: params,
    });
    return result.json<T>();
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
    await withRetries(options.retries, async () => {
      await this.raw.command({
        clickhouse_settings: idempotentSettings(options),
        query: sql,
        query_params: params,
      });
    });
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
