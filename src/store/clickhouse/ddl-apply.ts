// cspell:ignore clickhouse
/**
 * Apply the ClickHouse DDL (`ddl/NNN_*.sql`, in order) from TypeScript, with
 * the same parsing as `ddl/apply.sh`: full-line `--` comments are dropped,
 * the literal database `cg` is rewritten to the target database, and
 * statements are split on a `;` at the end of a line. Every statement is
 * idempotent, so re-applying is safe.
 *
 * Used by the e2e harness (`CHAINGRAPH_E2E_STORE=clickhouse`), the
 * ingestion gate (`--store clickhouse`) and the standalone DDL CLI
 * (`bin/chaingraph-clickhouse-ddl.js`, see `ddl-cli.ts`). Not used by the
 * agent itself.
 */
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { ClickHouseClient } from './client.js';

const ddlFilePattern = /^\d{3}_.*\.sql$/u;

/**
 * Where the DDL may live, relative to this (compiled) file, in order:
 * - `./ddl`: next to the compiled file; the agent image copies
 *   `src/store/clickhouse/ddl` to `build/store/clickhouse/ddl`.
 * - `../../../src/store/clickhouse/ddl`: a source checkout, running from
 *   `build/store/clickhouse/` (or from `src/store/clickhouse/`).
 */
const ddlDirectoryCandidates = [
  './ddl',
  '../../../src/store/clickhouse/ddl',
].map((relative) => fileURLToPath(new URL(relative, import.meta.url)));

const hasDdlFiles = (directory: string) =>
  existsSync(directory) &&
  readdirSync(directory).some((name) => ddlFilePattern.test(name));

/**
 * The DDL directory: `CHAINGRAPH_CLICKHOUSE_DDL_DIR` if set (must contain
 * `NNN_*.sql` files), else the first candidate next to this file that does.
 */
export const resolveDdlDirectory = (
  env: { [name: string]: string | undefined } = process.env
) => {
  const override = env.CHAINGRAPH_CLICKHOUSE_DDL_DIR;
  if (override !== undefined && override !== '') {
    if (!hasDdlFiles(override)) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error(
        `CHAINGRAPH_CLICKHOUSE_DDL_DIR (${override}) contains no NNN_*.sql files.`
      );
    }
    return override;
  }
  const found = ddlDirectoryCandidates.find(hasDdlFiles);
  if (found === undefined) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(
      `No ClickHouse DDL found (looked in ${ddlDirectoryCandidates.join(
        ', '
      )}); set CHAINGRAPH_CLICKHOUSE_DDL_DIR.`
    );
  }
  return found;
};
const databasePattern = /^[A-Za-z_][0-9A-Za-z_]*$/u;
const applyTimeoutMs = 300_000;

/** The DDL files, in apply order. */
export const ddlFiles = (directory = resolveDdlDirectory()) =>
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
  { directory = resolveDdlDirectory(), recreate = false } = {}
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

/** The table/view names in `database` (empty if it does not exist). */
export const listClickHouseTables = async (
  server: ClickHouseServer,
  database: string
) => {
  ddlStatements('', database);
  const client = adminClient(server);
  // eslint-disable-next-line functional/no-try-statement
  try {
    const rows = await client.query<{ name: string }>(
      'SELECT name FROM system.tables WHERE database = {database:String} ORDER BY name',
      { database }
    );
    return rows.map((row) => row.name);
  } finally {
    await client.close();
  }
};

/**
 * Part cleanup settings: how long merged-away parts stay on disk. The
 * server defaults (480 s, cleanup every 30-300 s) let a sync of small blocks
 * fill a disk with inactive parts (fix-pass-3.md §3). They can change on a
 * live table, so the DDL CLI aligns them (`alignTableSettings`).
 */
const alterableTableSettings: ReadonlySet<string> = new Set([
  'old_parts_lifetime',
  'cleanup_delay_period',
  'max_cleanup_delay_period',
  'cleanup_delay_period_random_add',
]);

/**
 * Table settings the DDL pins: the granularity and block sizes the agent's
 * lookups depend on (fixed at CREATE: a differing table must be recreated)
 * and the part cleanup settings (altered in place).
 */
const checkedTableSettings = [
  'index_granularity',
  'min_compress_block_size',
  'max_compress_block_size',
  ...alterableTableSettings,
] as const;

const settingValue = (settings: string, name: string) =>
  new RegExp(`\\b${name}\\s*=\\s*(\\d+)`, 'u').exec(settings)?.[1];

/**
 * Per table, the checked settings its `CREATE TABLE` in the DDL declares
 * (tables declaring none are left out).
 */
export const ddlTableSettings = (directory = resolveDdlDirectory()) => {
  const expected = new Map<string, Map<string, string>>();
  ddlFiles(directory)
    .flatMap((file) => ddlStatements(readFileSync(file, 'utf8'), 'cg'))
    .forEach((statement) => {
      const create =
        /CREATE TABLE IF NOT EXISTS cg\.(?<table>\w+)[\s\S]*\)\s*ENGINE[\s\S]*?\bSETTINGS\b(?<settings>[\s\S]*)$/u.exec(
          statement
        );
      if (create === null) return;
      const settings = new Map<string, string>();
      checkedTableSettings.forEach((name) => {
        const value = settingValue(create.groups!.settings!, name);
        if (value !== undefined) settings.set(name, value);
      });
      if (settings.size > 0) expected.set(create.groups!.table!, settings);
    });
  return expected;
};

export interface TableSettingMismatch {
  table: string;
  setting: string;
  expected: string;
  /** `undefined`: the table does not set it (server default) or is missing. */
  actual: string | undefined;
  /** The table does not exist. */
  missing: boolean;
}

/**
 * Compare the checked settings of the tables in `database` with the DDL.
 * `CREATE TABLE IF NOT EXISTS` never changes an existing table, so a
 * database created by an older DDL (or by hand) keeps its old granularity:
 * this is how the DDL CLI notices (g1-fix-pass-2.md §2).
 */
export const checkTableSettings = async (
  server: ClickHouseServer,
  database: string,
  { directory = resolveDdlDirectory() } = {}
) => {
  ddlStatements('', database);
  const expected = ddlTableSettings(directory);
  const client = adminClient(server);
  // eslint-disable-next-line functional/no-try-statement
  try {
    const rows = await client.query<{ name: string; engine: string }>(
      'SELECT name, engine_full AS engine FROM system.tables WHERE database = {database:String}',
      { database }
    );
    const engineOf = new Map(rows.map((row) => [row.name, row.engine]));
    const mismatches: TableSettingMismatch[] = [];
    const applied: string[] = [];
    expected.forEach((settings, table) => {
      settings.forEach((value, setting) => {
        const actual = settingValue(engineOf.get(table) ?? '', setting);
        if (actual === value) {
          applied.push(`${table}.${setting}=${value}`);
        } else {
          mismatches.push({
            actual,
            expected: value,
            missing: !engineOf.has(table),
            setting,
            table,
          });
        }
      });
    });
    return { applied, mismatches };
  } finally {
    await client.close();
  }
};

/**
 * The ALTER path for settings that can change on a live table (the part
 * cleanup settings): `ALTER TABLE … MODIFY SETTING` for every such mismatch
 * with the DDL. Others (granularity, block sizes) are left for
 * `checkTableSettings` to report. Returns the settings changed
 * (`table.setting: old→new`).
 */
export const alignTableSettings = async (
  server: ClickHouseServer,
  database: string,
  { directory = resolveDdlDirectory() } = {}
) => {
  const { mismatches } = await checkTableSettings(server, database, {
    directory,
  });
  const byTable = new Map<string, TableSettingMismatch[]>();
  mismatches
    .filter(
      (mismatch) =>
        alterableTableSettings.has(mismatch.setting) &&
        // a missing table is not altered (the check reports it)
        !mismatch.missing
    )
    .forEach((mismatch) => {
      byTable.set(mismatch.table, [
        ...(byTable.get(mismatch.table) ?? []),
        mismatch,
      ]);
    });
  if (byTable.size === 0) return [];
  const client = adminClient(server);
  const changed: string[] = [];
  // eslint-disable-next-line functional/no-try-statement
  try {
    // eslint-disable-next-line functional/no-loop-statement
    for (const [table, items] of byTable) {
      // eslint-disable-next-line no-await-in-loop
      await client.command(
        `ALTER TABLE ${database}.${table} MODIFY SETTING ${items
          .map((item) => `${item.setting} = ${item.expected}`)
          .join(', ')}`
      );
      items.forEach((item) => {
        changed.push(
          `${table}.${item.setting}: ${item.actual ?? 'unset'}→${item.expected}`
        );
      });
    }
  } finally {
    await client.close();
  }
  return changed;
};
