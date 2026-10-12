// cspell:ignore clickhouse
/**
 * Standalone ClickHouse DDL step (`bin/chaingraph-clickhouse-ddl.js`,
 * `yarn clickhouse:ddl`): apply every `ddl/NNN_*.sql` to
 * `CHAINGRAPH_CLICKHOUSE_DATABASE` (default `cg`, created if missing) on
 * `CHAINGRAPH_CLICKHOUSE_URL`, as `CHAINGRAPH_CLICKHOUSE_USER` /
 * `CHAINGRAPH_CLICKHOUSE_PASSWORD`. Idempotent; exits 0 on success, 1 on
 * failure. Meant for a Kubernetes init container or Job.
 *
 * DDL items (`ddl/optional/<name>.sql`): the default ones (`p_node_tx`)
 * are applied unless skipped with `CHAINGRAPH_CLICKHOUSE_DDL_SKIP`
 * (comma-separated) or `--skip <name>` (repeatable); others only when named
 * in `CHAINGRAPH_CLICKHOUSE_DDL_OPTIONAL` or with `--optional <name>`.
 *
 * Reads only these env vars (not `config.ts`, so no Postgres connection
 * string is needed). The password is never logged; the URL is logged without
 * credentials.
 */
import {
  alignTableSettings,
  applyClickHouseDdl,
  checkKeys,
  checkTableSettings,
  ddlFiles,
  legacySchemaFindings,
  listClickHouseTables,
  resolveDdlDirectory,
  resolveDdlItems,
} from './ddl-apply.js';

const log = (message: string) => {
  // eslint-disable-next-line no-console
  console.log(`clickhouse-ddl: ${message}`);
};

const fail = (message: string): never => {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(message);
};

const nonEmpty = (value: string | undefined, fallback: string) =>
  value === undefined || value === '' ? fallback : value;

/** The connection from env; `username` empty means the URL's user, else `default`. */
/** Values of `--flag a --flag b` / `--flag=a,b`, comma-joined. */
const flagValues = (argv: readonly string[], flag: string) =>
  argv
    .flatMap((arg, index) =>
      arg === flag
        ? [argv[index + 1] ?? '']
        : arg.startsWith(`${flag}=`)
        ? [arg.slice(flag.length + 1)]
        : []
    )
    .join(',');

/** The default items plus `--optional` / the env list, minus `--skip` / the env list. */
const optionalItems = (env: NodeJS.ProcessEnv, argv: readonly string[]) =>
  resolveDdlItems({
    add: [
      env.CHAINGRAPH_CLICKHOUSE_DDL_OPTIONAL ?? '',
      flagValues(argv, '--optional'),
    ].join(','),
    skip: [
      env.CHAINGRAPH_CLICKHOUSE_DDL_SKIP ?? '',
      flagValues(argv, '--skip'),
    ].join(','),
  });

const settingsFromEnv = (env: NodeJS.ProcessEnv) => ({
  database: nonEmpty(env.CHAINGRAPH_CLICKHOUSE_DATABASE, 'cg'),
  // eslint-disable-next-line @typescript-eslint/no-magic-numbers
  optional: optionalItems(env, process.argv.slice(2)),
  password: env.CHAINGRAPH_CLICKHOUSE_PASSWORD ?? '',
  url: env.CHAINGRAPH_CLICKHOUSE_URL ?? '',
  username: env.CHAINGRAPH_CLICKHOUSE_USER ?? '',
});

const tryParseUrl = (url: string) => {
  // eslint-disable-next-line functional/no-try-statement
  try {
    return new URL(url);
  } catch {
    return undefined;
  }
};

const parseUrl = (url: string) =>
  tryParseUrl(url) ??
  fail('CHAINGRAPH_CLICKHOUSE_URL is missing or not a URL (value not shown).');

const redact = (text: string, secrets: string[]) =>
  secrets
    .filter((secret) => secret !== '')
    .reduce((redacted, secret) => redacted.split(secret).join('***'), text);

// eslint-disable-next-line complexity
const run = async (settings: ReturnType<typeof settingsFromEnv>) => {
  const { database, optional, password, url, username } = settings;
  const safeUrl = parseUrl(url);
  safeUrl.username = '';
  safeUrl.password = '';
  const directory = resolveDdlDirectory();
  const files = ddlFiles(directory).map((file) =>
    file.slice(directory.length + 1)
  );
  log(
    `${safeUrl.origin} database=${database} user=${nonEmpty(
      username,
      '(from URL or default)'
    )} dir=${directory} files=${files.join(',')} items=${
      optional.length === 0 ? '(none)' : optional.join(',')
    }`
  );
  const server = { password, url, username };
  const started = Date.now();
  const legacy = await legacySchemaFindings(server, database);
  if (legacy.length > 0) {
    fail(
      `${database} was created before 2026-10-11 (${legacy.join(
        ', '
      )}): input no longer stores the spent output's columns and the current DDL cannot change it in place. Re-sync into a new database (docs/clickhouse-port/spend-index.md, "Upgrade").`
    );
  }
  const statements = await applyClickHouseDdl(server, database, {
    directory,
    optional,
  });
  const tables = await listClickHouseTables(server, database);
  if (tables.length === 0) {
    fail(`No tables in ${database} after applying the DDL.`);
  }
  // settings that can change on a live table (part cleanup): aligned in place
  const altered = await alignTableSettings(server, database, { directory });
  if (altered.length > 0) {
    log(`altered to the DDL's settings: ${altered.join(' ')}`);
  }
  const { applied, mismatches } = await checkTableSettings(server, database, {
    directory,
  });
  if (mismatches.length > 0) {
    fail(
      `table settings differ from the DDL (CREATE TABLE IF NOT EXISTS keeps an existing table as it is; recreate these tables): ${mismatches
        .map(
          ({ actual, expected, setting, table }) =>
            `${table}.${setting} is ${actual ?? 'unset'}, DDL says ${expected}`
        )
        .join('; ')}`
    );
  }
  log(`table settings as in the DDL: ${applied.join(' ')}`);
  const keys = await checkKeys(server, database, { directory });
  if (keys.length > 0) {
    log(
      `WARNING: keys differ from the DDL (results are the same; statements pay the planning cost these keys remove, docs/clickhouse-port/planning-floor.md; a projection can be rebuilt in place, a primary key needs a new table): ${keys
        .map(
          (key) =>
            `${key.object} ${key.kind} is ${
              key.actual === undefined
                ? 'missing'
                : `(${key.actual.join(', ')})`
            }, DDL says (${key.columns.join(', ')})`
        )
        .join('; ')}`
    );
  }
  log(
    `ok, ${statements} statement(s) in ${
      Date.now() - started
    } ms; ${database} has ${tables.length} table(s)/view(s): ${tables.join(
      ','
    )}`
  );
};

const main = async () => {
  const settings = settingsFromEnv(process.env);
  const urlPassword = decodeURIComponent(
    tryParseUrl(settings.url)?.password ?? ''
  );
  return run(settings).then(
    () => 0,
    (error: unknown) => {
      // eslint-disable-next-line no-console
      console.error(
        `clickhouse-ddl: FAILED: ${redact(
          error instanceof Error ? error.message : String(error),
          [settings.password, urlPassword]
        )}`
      );
      return 1;
    }
  );
};

process.exitCode = await main();
