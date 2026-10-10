// cspell:ignore clickhouse
/**
 * `yarn api:dev`: the Phase 2 spike API on CHAINGRAPH_API_HOST:CHAINGRAPH_API_PORT
 * (default 127.0.0.1:4000),
 * reading the ClickHouse store named by CHAINGRAPH_CLICKHOUSE_URL /
 * CHAINGRAPH_CLICKHOUSE_DATABASE (default `cg`). GraphiQL at /graphql.
 */
import { ClickHouseClient } from '../store/clickhouse/client.js';

import { createApi, listen } from './server.js';

const defaultPort = 4000;

const main = async () => {
  const client = ClickHouseClient.fromEnv();
  const api = createApi(client, {
    graphiql: true,
    live: {
      onError: (error) => {
        // eslint-disable-next-line no-console
        console.error('live query error', error);
      },
    },
  });
  const port = Number(process.env.CHAINGRAPH_API_PORT ?? defaultPort);
  const server = await listen(
    api,
    port,
    process.env.CHAINGRAPH_API_HOST ?? '127.0.0.1'
  );
  // eslint-disable-next-line no-console
  console.log(
    `Chaingraph v2 API spike: ${server.url} (ws: ${
      server.wsUrl
    }), store ${client.toString()}`
  );
  const stop = () => {
    server
      .close()
      .then(async () => client.close())
      .catch(() => undefined);
  };
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
};

main().catch((error: unknown) => {
  // eslint-disable-next-line no-console
  console.error(error);
  process.exitCode = 1;
});
