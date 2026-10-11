/* eslint-disable complexity */
// cspell:ignore clickhouse
/**
 * `yarn api:dev`: the Chaingraph v2 API on CHAINGRAPH_API_HOST:CHAINGRAPH_API_PORT
 * (default 127.0.0.1:4000), reading the ClickHouse store named by
 * CHAINGRAPH_CLICKHOUSE_URL / CHAINGRAPH_CLICKHOUSE_DATABASE (default `cg`).
 * GraphiQL at /graphql. Options:
 * - the watermark-keyed response cache is on by default (every answer still
 *   corresponds to exactly one consistent snapshot: plugins.ts
 *   `ResponseCache`); CHAINGRAPH_API_RESPONSE_CACHE=off disables it;
 * - CHAINGRAPH_API_TRUSTED_DOCUMENTS=<dir of .graphql files> registers trusted
 *   documents (by sha256), CHAINGRAPH_API_TRUSTED_DOCUMENTS_ONLY=true runs
 *   nothing else;
 * - CHAINGRAPH_API_AGENT_URL=<agent internal API> enables send_transaction;
 * - CHAINGRAPH_API_MAX_RESULT_ROWS caps the rows of one root statement.
 */
import { ClickHouseClient } from '../store/clickhouse/client.js';

import { loadTrustedDocuments } from './plugins.js';
import { createApi, listen } from './server.js';

const defaultPort = 4000;

const main = async () => {
  const client = ClickHouseClient.fromEnv();
  const { env } = process;
  const trusted = env.CHAINGRAPH_API_TRUSTED_DOCUMENTS;
  const api = createApi(client, {
    ...(env.CHAINGRAPH_API_AGENT_URL === undefined
      ? {}
      : { agentUrl: env.CHAINGRAPH_API_AGENT_URL }),
    ...(env.CHAINGRAPH_API_MAX_RESULT_ROWS === undefined
      ? {}
      : { maxResultRows: Number(env.CHAINGRAPH_API_MAX_RESULT_ROWS) }),
    responseCache: env.CHAINGRAPH_API_RESPONSE_CACHE === 'off' ? false : {},
    ...(trusted === undefined
      ? {}
      : {
          trustedDocuments: {
            allowArbitraryDocuments:
              env.CHAINGRAPH_API_TRUSTED_DOCUMENTS_ONLY !== 'true',
            documents: loadTrustedDocuments(trusted),
          },
        }),
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
    `Chaingraph v2 API: ${server.url} (ws: ${
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
