/* eslint-disable complexity */
// cspell:ignore clickhouse
/**
 * `yarn clickhouse:api-user <xml|sql|apply>` (bin/chaingraph-clickhouse-api-user.js):
 * the API's ClickHouse user with the `max_concurrent_queries_for_user`
 * backstop of its profile (api-user.ts; fix-pass-5.md).
 * - `xml`: print a users.d file (mount it into
 *   /etc/clickhouse-server/users.d/; Kubernetes: from a Secret);
 * - `sql`: print the SQL;
 * - `apply`: run the SQL on CHAINGRAPH_CLICKHOUSE_URL as
 *   CHAINGRAPH_CLICKHOUSE_USER / _PASSWORD (an admin with access management).
 * Env: CHAINGRAPH_CLICKHOUSE_API_USER (default `chaingraph_api`),
 * CHAINGRAPH_CLICKHOUSE_API_PASSWORD (required; only its sha256 is
 * written), CHAINGRAPH_CLICKHOUSE_DATABASE (default `cg`),
 * CHAINGRAPH_CLICKHOUSE_API_PODS (default 1),
 * CHAINGRAPH_CLICKHOUSE_API_STATEMENTS_PER_POD (default 30),
 * CHAINGRAPH_CLICKHOUSE_API_STATEMENTS_MARGIN (default 10).
 */
import {
  apiMaxConcurrentQueries,
  apiUserBackstopFromEnv,
  apiUserSql,
  apiUserXml,
} from './api-user.js';
import { ClickHouseClient, clickHouseConfigFromEnv } from './client.js';

const nonEmpty = (value: string | undefined, fallback: string) =>
  value === undefined || value === '' ? fallback : value;

const main = async () => {
  const { env } = process;
  const mode = process.argv[2] ?? '';
  const password = env.CHAINGRAPH_CLICKHOUSE_API_PASSWORD ?? '';
  if (!['apply', 'sql', 'xml'].includes(mode) || password === '') {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(
      'usage: CHAINGRAPH_CLICKHOUSE_API_PASSWORD=… chaingraph-clickhouse-api-user <xml|sql|apply>'
    );
  }
  const definition = {
    backstop: apiUserBackstopFromEnv(env),
    database: nonEmpty(env.CHAINGRAPH_CLICKHOUSE_DATABASE, 'cg'),
    password,
    user: nonEmpty(env.CHAINGRAPH_CLICKHOUSE_API_USER, 'chaingraph_api'),
  };
  if (mode === 'xml') {
    process.stdout.write(apiUserXml(definition));
    return;
  }
  const statements = apiUserSql(definition);
  if (mode === 'sql') {
    process.stdout.write(`${statements.join(';\n')};\n`);
    return;
  }
  const client = new ClickHouseClient(clickHouseConfigFromEnv(env));
  // eslint-disable-next-line functional/no-try-statement
  try {
    // eslint-disable-next-line functional/no-loop-statement
    for (const statement of statements) {
      // eslint-disable-next-line no-await-in-loop
      await client.command(statement);
    }
  } finally {
    await client.close();
  }
  // eslint-disable-next-line no-console
  console.log(
    `clickhouse-api-user: ${definition.user} on ${
      client.endpoint
    }: max_concurrent_queries_for_user = ${apiMaxConcurrentQueries(
      definition.backstop
    )} (${definition.backstop.pods} pods x ${
      definition.backstop.statementsPerPod
    } + ${definition.backstop.margin})`
  );
};

main().catch((error: unknown) => {
  // eslint-disable-next-line no-console
  console.error(
    `clickhouse-api-user: ${
      error instanceof Error ? error.message : String(error)
    }`
  );
  process.exitCode = 1;
});
