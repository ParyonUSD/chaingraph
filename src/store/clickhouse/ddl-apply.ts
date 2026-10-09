// cspell:ignore clickhouse
/**
 * Apply the ClickHouse DDL (`ddl/NNN_*.sql`, in order) from TypeScript, with
 * the same parsing as `ddl/apply.sh`: full-line `--` comments are dropped,
 * the literal database `cg` is rewritten to the target database, and
 * statements are split on a `;` at the end of a line. Every statement is
 * idempotent, so re-applying is safe.
 *
 * Used by the e2e harness (`CHAINGRAPH_E2E_STORE=clickhouse`) and the
 * ingestion gate (`--store clickhouse`). Not used by the agent.
 */
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { ClickHouseClient } from './client.js';

/** `src/store/clickhouse/ddl` (also correct when running from `build/`). */
export const ddlDirectory = fileURLToPath(
  new URL('../../../src/store/clickhouse/ddl', import.meta.url)
);

const ddlFilePattern = /^\d{3}_.*\.sql$/u;
const databasePattern = /^[A-Za-z_][0-9A-Za-z_]*$/u;
const applyTimeoutMs = 300_000;

/** The DDL files, in apply order. */
export const ddlFiles = (directory = ddlDirectory) =>
  readdirSync(directory)
    .filter((name) => ddlFilePattern.test(name))
    .sort((a, b) => (a < b ? -1 : Number(a > b)))
    .map((name) => join(directory, name));

/** One file's statements, for `database`. */
export const ddlStatements = (sql: string, database: string) => {
  if (!databasePattern.test(database)) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(`Invalid ClickHouse database name: ${database}`);
  }
  return sql
    .replace(/^\s*--.*(?:\n|$)/gmu, '')
    .replace(/\bcg\./gu, `${database}.`)
    .replace(
      /DATABASE IF NOT EXISTS cg\b/gu,
      `DATABASE IF NOT EXISTS ${database}`
    )
    .split(/;[ \t]*(?:\n|$)/u)
    .filter((statement) => /\S/u.test(statement));
};

export interface ClickHouseServer {
  url: string;
  username?: string;
  password?: string;
}

/**
 * A client on the server's `default` database (the target database may not
 * exist yet).
 */
const adminClient = (server: ClickHouseServer) =>
  new ClickHouseClient({
    database: 'default',
    password: server.password ?? '',
    requestTimeoutMs: applyTimeoutMs,
    url: server.url,
    username: server.username ?? '',
  });

/** `DROP DATABASE IF EXISTS <database> SYNC`. */
export const dropClickHouseDatabase = async (
  server: ClickHouseServer,
  database: string
) => {
  ddlStatements('', database);
  const client = adminClient(server);
  // eslint-disable-next-line functional/no-try-statement
  try {
    await client.command(`DROP DATABASE IF EXISTS ${database} SYNC`);
  } finally {
    await client.close();
  }
};

/**
 * Apply every DDL file to `database` (created if missing). With
 * `recreate`, drop the database first. Returns the number of statements run.
 */
export const applyClickHouseDdl = async (
  server: ClickHouseServer,
  database: string,
  { directory = ddlDirectory, recreate = false } = {}
): Promise<number> => {
  if (recreate) {
    await dropClickHouseDatabase(server, database);
  }
  const statements = ddlFiles(directory).flatMap((file) =>
    ddlStatements(readFileSync(file, 'utf8'), database)
  );
  const client = adminClient(server);
  // eslint-disable-next-line functional/no-try-statement
  try {
    await statements.reduce<Promise<void>>(
      async (previous, statement) =>
        previous.then(async () => client.command(statement)),
      Promise.resolve()
    );
  } finally {
    await client.close();
  }
  return statements.length;
};
