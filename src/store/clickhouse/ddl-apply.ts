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

/**
 * Optional DDL items (`ddl/optional/<name>.sql`), off by default; applied
 * after the numbered files when named in `CHAINGRAPH_CLICKHOUSE_DDL_OPTIONAL`
 * (comma-separated) or the DDL CLI's `--optional <name>`. A projection item
 * added to a table that already has rows is materialized once (an
 * asynchronous mutation), never again on a re-apply.
 */
export const optionalDdlItems: {
  readonly [name: string]: { table: string; projection: string };
} = {
  // eslint-disable-next-line @typescript-eslint/naming-convention, camelcase
  p_node_tx: { projection: 'p_node_tx', table: 'tx_acceptance' },
};

/** The optional items named in `value` (comma-separated; unknown names throw). */
export const parseOptionalDdl = (value: string | undefined): string[] => {
  const names = (value ?? '')
    .split(',')
    .map((name) => name.trim())
    .filter((name) => name !== '');
  names.forEach((name) => {
    if (optionalDdlItems[name] === undefined) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error(
        `Unknown optional DDL item ${name} (known: ${Object.keys(
          optionalDdlItems
        ).join(', ')}).`
      );
    }
  });
  return [...new Set(names)];
};

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

const runStatements = async (
  client: ClickHouseClient,
  statements: readonly string[]
) =>
  statements.reduce<Promise<void>>(
    async (previous, statement) =>
      previous.then(async () => client.command(statement)),
    Promise.resolve()
  );

/**
 * Apply one optional item; a projection added to a table with rows is
 * materialized (once: it was not there before). Returns the statements run.
 */
const applyOptionalItem = async (
  client: ClickHouseClient,
  { database, directory }: { database: string; directory: string },
  name: string
) => {
  const item = optionalDdlItems[name]!;
  const [state] = await client.query<{ present: number; rows: string }>(
    `SELECT
       (SELECT count() FROM system.projections WHERE database = {database:String}
          AND table = {table:String} AND name = {projection:String}) AS present,
       (SELECT toString(sum(rows)) FROM system.parts WHERE database = {database:String}
          AND table = {table:String} AND active) AS rows`,
    { database, projection: item.projection, table: item.table }
  );
  const statements = ddlStatements(
    readFileSync(join(directory, 'optional', `${name}.sql`), 'utf8'),
    database
  );
  await runStatements(client, statements);
  const materialize =
    Number(state?.present ?? 0) === 0 && BigInt(state?.rows ?? '0') > 0n;
  if (materialize) {
    await client.command(
      `ALTER TABLE ${database}.${item.table} MATERIALIZE PROJECTION ${item.projection}`
    );
  }
  return statements.length + Number(materialize);
};

/**
 * Apply every DDL file to `database` (created if missing), then the
 * `optional` items. With `recreate`, drop the database first. Returns the
 * number of statements run.
 */
export const applyClickHouseDdl = async (
  server: ClickHouseServer,
  database: string,
  {
    directory = resolveDdlDirectory(),
    recreate = false,
    optional = parseOptionalDdl(process.env.CHAINGRAPH_CLICKHOUSE_DDL_OPTIONAL),
  }: {
    directory?: string;
    recreate?: boolean;
    optional?: readonly string[];
  } = {}
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
    await runStatements(client, statements);
    return await parseOptionalDdl(optional.join(',')).reduce<Promise<number>>(
      async (previous, name) =>
        previous.then(
          async (count) =>
            count +
            (await applyOptionalItem(client, { database, directory }, name))
        ),
      Promise.resolve(statements.length)
    );
  } finally {
    await client.close();
  }
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
 * Schema of a database created before 2026-10-11 that the current DDL cannot
 * bring up to date in place (CREATE TABLE IF NOT EXISTS and ADD PROJECTION IF
 * NOT EXISTS leave the old objects as they are): `input` with the spent
 * output's columns, its `p_spent_*` projections, `pending_spend` or the
 * stand-in tables. Such a database must be re-synced into a new one
 * (docs/clickhouse-port/spend-index.md, "Upgrade"). Empty when the database
 * is new or current.
 */
export const legacySchemaFindings = async (
  server: ClickHouseServer,
  database: string
) => {
  ddlStatements('', database);
  const client = adminClient(server);
  // eslint-disable-next-line functional/no-try-statement
  try {
    const [columns, tables, projections] = await Promise.all([
      client.query<{ name: string }>(
        `SELECT name FROM system.columns WHERE database = {database:String} AND table = 'input'
           AND name IN ('value_satoshis', 'locking_bytecode', 'token_category')`,
        { database }
      ),
      client.query<{ name: string }>(
        `SELECT name FROM system.tables WHERE database = {database:String}
           AND name IN ('pending_spend', 'input_stand_in', 'input_stand_in_resolution')`,
        { database }
      ),
      client.query<{ name: string }>(
        `SELECT DISTINCT name FROM system.projection_parts WHERE database = {database:String}
           AND table = 'input' AND name IN ('p_spent_script', 'p_spent_category') AND active`,
        { database }
      ),
    ]);
    return [
      ...columns.map((row) => `input.${row.name}`),
      ...tables.map((row) => `table ${row.name}`),
      ...projections.map((row) => `projection input.${row.name}`),
    ];
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

/**
 * A key the DDL declares that `IF NOT EXISTS` cannot change on an existing
 * object: a table's explicit `PRIMARY KEY` or a projection's sort key.
 */
export interface DdlKey {
  /** `table` or `table.projection`. */
  object: string;
  kind: 'primary key' | 'projection sort key';
  columns: string[];
}

const keyColumns = (key: string) =>
  key
    .trim()
    .replace(/^\((?<inner>[\s\S]*)\)$/u, '$<inner>')
    .split(',')
    .map((column) => column.trim())
    .filter((column) => column !== '');

const keyExpression = String.raw`(\([^)]*\)|\w+)`;

/**
 * The keys of the DDL (planning-floor.md): every explicit `PRIMARY KEY` of a
 * table and every projection's `ORDER BY` in the numbered files.
 */
export const ddlKeys = (directory = resolveDdlDirectory()): DdlKey[] =>
  ddlFiles(directory)
    .flatMap((file) => ddlStatements(readFileSync(file, 'utf8'), 'cg'))
    .flatMap((statement): DdlKey[] => {
      const table =
        /CREATE TABLE IF NOT EXISTS cg\.(?<table>\w+)[\s\S]*\)\s*(?<engine>ENGINE[\s\S]*)$/u.exec(
          statement
        );
      if (table !== null) {
        const primary = new RegExp(
          String.raw`\bPRIMARY KEY\s+${keyExpression}`,
          'u'
        ).exec(table.groups!.engine!);
        return primary === null
          ? []
          : [
              {
                columns: keyColumns(primary[1]!),
                kind: 'primary key',
                object: table.groups!.table!,
              },
            ];
      }
      const projection = new RegExp(
        String.raw`ALTER TABLE cg\.(\w+) ADD PROJECTION IF NOT EXISTS (\w+)\s*\(\s*SELECT[\s\S]*?\bORDER BY\s+${keyExpression}`,
        'u'
      ).exec(statement);
      return projection === null
        ? []
        : [
            {
              columns: keyColumns(projection[3]!),
              kind: 'projection sort key',
              object: `${projection[1]!}.${projection[2]!}`,
            },
          ];
    });

export interface KeyMismatch extends DdlKey {
  /** `undefined`: the table or projection does not exist. */
  actual: string[] | undefined;
}

/**
 * Compare the keys of `database` with the DDL. A database created by an
 * older DDL keeps its keys (the statements are `IF NOT EXISTS`); its results
 * are the same, but statements pay the planning cost the newer keys remove
 * (planning-floor.md). A projection can be rebuilt in place (DROP, ADD,
 * MATERIALIZE); a primary key needs a new table.
 */
export const checkKeys = async (
  server: ClickHouseServer,
  database: string,
  { directory = resolveDdlDirectory() } = {}
): Promise<KeyMismatch[]> => {
  ddlStatements('', database);
  const client = adminClient(server);
  // eslint-disable-next-line functional/no-try-statement
  try {
    const [tables, projections] = await Promise.all([
      client.query<{ name: string; key: string }>(
        'SELECT name, primary_key AS key FROM system.tables WHERE database = {database:String}',
        { database }
      ),
      client.query<{ name: string; key: string[] }>(
        `SELECT concat(table, '.', name) AS name, sorting_key AS key FROM system.projections
           WHERE database = {database:String}`,
        { database }
      ),
    ]);
    const actual = new Map<string, string[]>([
      ...tables.map((row): [string, string[]] => [
        row.name,
        keyColumns(row.key),
      ]),
      ...projections.map((row): [string, string[]] => [row.name, row.key]),
    ]);
    return ddlKeys(directory)
      .map((key) => ({ ...key, actual: actual.get(key.object) }))
      .filter((key) => key.actual?.join(', ') !== key.columns.join(', '));
  } finally {
    await client.close();
  }
};
