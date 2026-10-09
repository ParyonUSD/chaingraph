// cspell:ignore clickhouse
/**
 * Standalone ClickHouse DDL step (`bin/chaingraph-clickhouse-ddl.js`,
 * `yarn clickhouse:ddl`): apply every `ddl/NNN_*.sql` to
 * `CHAINGRAPH_CLICKHOUSE_DATABASE` (default `cg`, created if missing) on
 * `CHAINGRAPH_CLICKHOUSE_URL`, as `CHAINGRAPH_CLICKHOUSE_USER` /
 * `CHAINGRAPH_CLICKHOUSE_PASSWORD`. Idempotent; exits 0 on success, 1 on
 * failure. Meant for a Kubernetes init container or Job.
 *
 * Reads only these env vars (not `config.ts`, so no Postgres connection
 * string is needed). The password is never logged; the URL is logged without
 * credentials.
 */
import {
  applyClickHouseDdl,
  ddlFiles,
  listClickHouseTables,
  resolveDdlDirectory,
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
const settingsFromEnv = (env: NodeJS.ProcessEnv) => ({
  database: nonEmpty(env.CHAINGRAPH_CLICKHOUSE_DATABASE, 'cg'),
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

const run = async (settings: ReturnType<typeof settingsFromEnv>) => {
  const { database, password, url, username } = settings;
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
    )} dir=${directory} files=${files.join(',')}`
  );
  const server = { password, url, username };
  const started = Date.now();
  const statements = await applyClickHouseDdl(server, database, {
    directory,
  });
  const tables = await listClickHouseTables(server, database);
  if (tables.length === 0) {
    fail(`No tables in ${database} after applying the DDL.`);
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
