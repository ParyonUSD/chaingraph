// cspell:ignore clickhouse
/**
 * Backend selection for the e2e suite (`CHAINGRAPH_E2E_STORE`).
 *
 * - `postgres` (default): unchanged; the suite recreates
 *   `chaingraph_e2e_test` on CHAINGRAPH_E2E_POSTGRES_HOST/_PORT.
 * - `clickhouse`: a fresh database `cg_e2e_<pid>` on
 *   CHAINGRAPH_E2E_CLICKHOUSE_URL (user/password from
 *   CHAINGRAPH_E2E_CLICKHOUSE_USER/_PASSWORD) with the DDL applied; the agent
 *   runs with CHAINGRAPH_STORE=clickhouse; reads go through the ClickHouse
 *   checker; `[postgres]`-tagged tests are registered as skipped; the
 *   database is dropped after the run.
 */
import test from 'ava';

import { parseStoreBackend } from '../store/checker-factory.js';
import { ClickHouseClient } from '../store/clickhouse/client.js';
import type { ClickHouseServer } from '../store/clickhouse/ddl-apply.js';
import { dropClickHouseDatabase } from '../store/clickhouse/ddl-apply.js';

export const e2eStore = parseStoreBackend(process.env.CHAINGRAPH_E2E_STORE);
export const isClickHouseE2e = e2eStore === 'clickhouse';

const clickHouseUrl = process.env.CHAINGRAPH_E2E_CLICKHOUSE_URL ?? '';
if (isClickHouseE2e && clickHouseUrl === '') {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    'CHAINGRAPH_E2E_STORE=clickhouse requires CHAINGRAPH_E2E_CLICKHOUSE_URL (e.g. http://localhost:18123).'
  );
}

export const e2eClickHouseServer: ClickHouseServer = {
  password: process.env.CHAINGRAPH_E2E_CLICKHOUSE_PASSWORD ?? '',
  url: clickHouseUrl,
  username: process.env.CHAINGRAPH_E2E_CLICKHOUSE_USER ?? '',
};

/** One database per run, so concurrent runs never share a writer lease. */
export const e2eClickHouseDatabase = `cg_e2e_${process.pid}`;

const isRunning = (pid: number) => {
  // eslint-disable-next-line functional/no-try-statement
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

/**
 * A run never lasts this long, so a `cg_e2e_<pid>` database older than this
 * is stale even if its pid is in use (a pid can be reused).
 */
const staleDatabaseAgeSeconds = 3600;

/**
 * Drop `cg_e2e_<pid>` databases left behind by other runs: the pid no longer
 * exists (a run killed by an uncaught exception or an AVA timeout never
 * reaches `test.after.always`), or the database is older than an hour (its
 * oldest table's `metadata_modification_time`). Returns the dropped names.
 */
export const dropStaleClickHouseE2eDatabases = async () => {
  const client = new ClickHouseClient({
    database: 'default',
    password: e2eClickHouseServer.password ?? '',
    requestTimeoutMs: 60_000,
    url: e2eClickHouseServer.url,
    username: e2eClickHouseServer.username ?? '',
  });
  // eslint-disable-next-line functional/no-try-statement
  try {
    const candidates = await client.query<{
      ageSeconds: number | string | null;
      name: string;
    }>(
      `SELECT d.name AS name,
              if(t.oldest IS NULL, NULL, dateDiff('second', t.oldest, now())) AS ageSeconds
         FROM system.databases AS d
         LEFT JOIN (SELECT database, min(metadata_modification_time) AS oldest
                      FROM system.tables
                      WHERE match(database, '^cg_e2e_[0-9]+$')
                      GROUP BY database) AS t
           ON t.database = d.name
         WHERE match(d.name, '^cg_e2e_[0-9]+$')
         SETTINGS join_use_nulls = 1`
    );
    const stale = candidates
      .filter(
        ({ ageSeconds, name }) =>
          name !== e2eClickHouseDatabase &&
          (!isRunning(Number(name.slice('cg_e2e_'.length))) ||
            Number(ageSeconds ?? 0) > staleDatabaseAgeSeconds)
      )
      .map(({ name }) => name);
    await stale.reduce<Promise<void>>(
      async (previous, name) =>
        previous.then(async () =>
          dropClickHouseDatabase(e2eClickHouseServer, name)
        ),
      Promise.resolve()
    );
    return stale;
  } finally {
    await client.close();
  }
};

/**
 * Extra agent environment for the selected backend. The agent's config
 * still requires CHAINGRAPH_POSTGRES_CONNECTION_STRING, so the caller keeps
 * passing it (unused on ClickHouse).
 */
/**
 * ClickHouse store tuning variables passed through from the test process to
 * the spawned agent when set (e.g. `CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES=16`
 * to run the suite under the lab's in-flight cap). A set
 * CHAINGRAPH_CLICKHOUSE_PENDING_SPEND_TIMEOUT_MS replaces the suite's 1 ms.
 */
const clickHousePassThroughVariables = [
  'CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES',
  'CHAINGRAPH_CLICKHOUSE_MAX_BLOCKS_PER_COMMIT',
  'CHAINGRAPH_CLICKHOUSE_MAX_BYTES_PER_COMMIT',
  'CHAINGRAPH_CLICKHOUSE_LEASE_TTL_MS',
  'CHAINGRAPH_CLICKHOUSE_PENDING_SPEND_TIMEOUT_MS',
  'CHAINGRAPH_CLICKHOUSE_UTXO',
] as const;

/**
 * The agent's `CHAINGRAPH_CLICKHOUSE_UTXO` (passed through): the checker
 * answers `unspent` at query time when it is `off`.
 */
export const e2eClickHouseUtxo = (): 'off' | 'on' =>
  process.env.CHAINGRAPH_CLICKHOUSE_UTXO === 'off' ? 'off' : 'on';

const clickHousePassThroughEnvironment = (): { [key: string]: string } =>
  Object.fromEntries(
    clickHousePassThroughVariables.flatMap((name) => {
      const value = process.env[name];
      return value === undefined || value === '' ? [] : [[name, value]];
    })
  );

export const e2eStoreEnvironment = (): { [key: string]: string } =>
  isClickHouseE2e
    ? {
        /* eslint-disable @typescript-eslint/naming-convention */
        CHAINGRAPH_CLICKHOUSE_DATABASE: e2eClickHouseDatabase,
        CHAINGRAPH_CLICKHOUSE_PASSWORD: e2eClickHouseServer.password ?? '',
        /*
         * The mockchain's non-coinbase inputs spend random outpoints that
         * never exist ("inputs do not spend prior outputs"); a ClickHouse
         * block save waits this long for a spent output before taking it as
         * unknown (Postgres stores such inputs at once). 1 ms keeps tip-mode
         * blocks, which wait for their predecessors' rows, from queueing.
         */
        CHAINGRAPH_CLICKHOUSE_PENDING_SPEND_TIMEOUT_MS: '1',
        CHAINGRAPH_CLICKHOUSE_URL: e2eClickHouseServer.url,
        CHAINGRAPH_CLICKHOUSE_USER: e2eClickHouseServer.username ?? '',
        CHAINGRAPH_STORE: 'clickhouse',
        /* eslint-enable @typescript-eslint/naming-convention */
        ...clickHousePassThroughEnvironment(),
      }
    : // eslint-disable-next-line @typescript-eslint/naming-convention
      { CHAINGRAPH_STORE: 'postgres' };

/**
 * `test` / `test.serial` for `[postgres]`-tagged tests: registered as
 * skipped when the suite runs against ClickHouse (they read Postgres
 * catalogs, call SQL functions or write fixtures with raw SQL).
 * Equivalent CLI filter: `--match '*[e2e]*' --match '!*[postgres]*'`.
 */
export const postgresTest: {
  concurrent: typeof test.skip;
  serial: typeof test.serial.skip;
} = isClickHouseE2e
  ? { concurrent: test.skip, serial: test.serial.skip }
  : { concurrent: test, serial: test.serial };
