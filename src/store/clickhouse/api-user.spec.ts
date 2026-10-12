/* eslint-disable @typescript-eslint/no-magic-numbers, @typescript-eslint/naming-convention, functional/no-try-statement, functional/no-let, camelcase, @typescript-eslint/no-loop-func */
// cspell:ignore clickhouse
/**
 * The API user's `max_concurrent_queries_for_user` backstop (fix-pass-5.md):
 * the formula, both renderings and, against a server with SQL access
 * management (CHAINGRAPH_E2E_CLICKHOUSE_URL), that the server refuses the
 * statement over the limit, that a session cannot raise it, and that the
 * user can still read what the API needs and kill its own statements (the
 * margin's purpose: a KILL QUERY is itself a statement of the user).
 */
import { randomBytes } from 'node:crypto';

import test from 'ava';

import { readClickHouseServerLimits } from '../../api/admission.js';

import {
  apiMaxConcurrentQueries,
  apiUserBackstopFromEnv,
  apiUserSql,
  apiUserXml,
} from './api-user.js';
import { ClickHouseClient } from './client.js';
import { connectionFor } from './spec-fixtures.js';
import { e2eClickHouseUrl } from './test-support.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

test('api user: backstop = pods x per pod + margin, from the environment', (t) => {
  t.deepEqual(apiUserBackstopFromEnv({}), {
    margin: 10,
    pods: 1,
    statementsPerPod: 30,
  });
  t.is(apiMaxConcurrentQueries(apiUserBackstopFromEnv({})), 40);
  t.is(
    apiMaxConcurrentQueries(
      apiUserBackstopFromEnv({ CHAINGRAPH_CLICKHOUSE_API_PODS: '2' })
    ),
    70
  );
  t.is(
    apiMaxConcurrentQueries(
      apiUserBackstopFromEnv({
        CHAINGRAPH_CLICKHOUSE_API_PODS: '3',
        CHAINGRAPH_CLICKHOUSE_API_STATEMENTS_MARGIN: '0',
        CHAINGRAPH_CLICKHOUSE_API_STATEMENTS_PER_POD: '12',
      })
    ),
    36
  );
  t.throws(() =>
    apiUserBackstopFromEnv({ CHAINGRAPH_CLICKHOUSE_API_PODS: '0' })
  );
  t.throws(() =>
    apiUserBackstopFromEnv({ CHAINGRAPH_CLICKHOUSE_API_PODS: 'two' })
  );
});

test('api user: XML and SQL renderings carry the limit, never the password', (t) => {
  const definition = {
    backstop: { margin: 10, pods: 2, statementsPerPod: 30 },
    database: 'cg',
    password: 'secret-password',
    user: 'api_ro',
  };
  const xml = apiUserXml(definition);
  t.regex(
    xml,
    /<max_concurrent_queries_for_user>70<\/max_concurrent_queries_for_user>/u
  );
  t.regex(xml, /<max_concurrent_queries_for_user><const\/>/u);
  t.regex(xml, /<readonly>2<\/readonly>/u);
  t.regex(xml, /<profile>chaingraph_api<\/profile>/u);
  t.false(xml.includes('secret-password'));
  const sql = apiUserSql(definition).join('\n');
  t.regex(sql, /max_concurrent_queries_for_user = 70 CONST/u);
  t.false(sql.includes('secret-password'));
  t.throws(() => apiUserSql({ ...definition, user: 'a; DROP' }));
});

e2e(
  '[e2e] api user: the server enforces the backstop on the API user',
  async (t) => {
    t.timeout(60_000);
    const suffix = randomBytes(4).toString('hex');
    const user = `cg_spec_api_${suffix}`;
    const profile = `cg_spec_api_profile_${suffix}`;
    const password = randomBytes(12).toString('hex');
    const admin = new ClickHouseClient(connectionFor('default'));
    t.teardown(async () => {
      await admin.command(`DROP USER IF EXISTS ${user}`).catch(() => undefined);
      await admin
        .command(`DROP SETTINGS PROFILE IF EXISTS ${profile}`)
        .catch(() => undefined);
      await admin.close();
    });
    const statements = apiUserSql({
      backstop: { margin: 1, pods: 1, statementsPerPod: 2 },
      database: 'default',
      password,
      profile,
      user,
    });
    try {
      await admin.command(statements[0]!);
    } catch (error: unknown) {
      t.log(`no SQL access management on this server: ${String(error)}`);
      t.pass();
      return;
    }
    // eslint-disable-next-line functional/no-loop-statement
    for (const statement of statements.slice(1)) {
      // eslint-disable-next-line no-await-in-loop
      await admin.command(statement);
    }
    const api = new ClickHouseClient({
      ...connectionFor('default'),
      password,
      username: user,
    });
    t.teardown(async () => api.close());
    const limits = await readClickHouseServerLimits(api);
    t.is(limits.maxConcurrentQueriesForUser, 3);
    t.true((limits.maxThreads ?? 0) > 0);
    /* store features read by ApiDb */
    await api.query(
      `SELECT (SELECT count() FROM system.projections), (SELECT count() FROM system.tables)`
    );
    /* a session cannot raise the limit */
    const raised = await api
      .query('SELECT 1', {}, { max_concurrent_queries_for_user: '50' })
      .then(
        () => 'ok',
        (error: unknown) => String(error)
      );
    t.regex(raised, /SETTING_CONSTRAINT_VIOLATION|should not be changed/u);
    const sleeper = async (queryId: string) =>
      api
        .query('SELECT sleep(3)', {}, {}, { queryId, retryTimeouts: false })
        .then(
          () => 'done',
          (error: unknown) => String(error)
        );
    const waitRunning = async (count: number) => {
      let visible = 0;
      // eslint-disable-next-line functional/no-loop-statement
      for (let attempt = 0; attempt < 60 && visible !== count; attempt += 1) {
        // eslint-disable-next-line no-await-in-loop
        const [row] = await admin.query<{ c: string }>(
          `SELECT count() AS c FROM system.processes WHERE user = {user:String}`,
          { user }
        );
        visible = Number(row?.c ?? 0);
        // eslint-disable-next-line no-await-in-loop
        await new Promise((resolve) => {
          setTimeout(resolve, 50);
        });
      }
      return visible;
    };
    /* the margin leaves room for the kill of an abandoned statement */
    const id = (n: number) => `cg-spec-${suffix}-${n}`;
    const first = sleeper(id(1));
    const second = sleeper(id(2));
    t.is(await waitRunning(2), 2, 'two statements running');
    await api.kill(id(1));
    t.regex(await first, /QUERY_WAS_CANCELLED|394|cancel/iu);
    /* three running statements fill the limit, the next is refused at once */
    const third = sleeper(id(3));
    const fourth = sleeper(id(4));
    t.is(await waitRunning(3), 3, 'three statements running');
    const started = Date.now();
    const refused = await api
      .query('SELECT 1', {}, {}, { retryTimeouts: false })
      .then(
        () => 'ok',
        (error: unknown) => String(error)
      );
    t.regex(refused, /TOO_MANY_SIMULTANEOUS_QUERIES|Too many simultaneous/u);
    t.true(Date.now() - started < 2_000, 'refused at once');
    t.deepEqual(await Promise.all([second, third, fourth]), [
      'done',
      'done',
      'done',
    ]);
  }
);
