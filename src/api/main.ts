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
 * - CHAINGRAPH_API_MAX_RESULT_ROWS caps the rows of one root statement;
 * - CHAINGRAPH_API_AUTHCHAIN_MAX_BRANCHES (default 1000) caps the branches
 *   `authchain_branches` builds per base: above it the field fails with
 *   AUTHCHAIN_TOO_WIDE (no truncated tree);
 * - CHAINGRAPH_API_UTXO_SET=off reads unspent outputs at query time even
 *   where the store has a stored UTXO set (default on);
 *   CHAINGRAPH_API_UTXO_SET_MAX_LAG_ROWS (default 100000) is its plan bound;
 * - CHAINGRAPH_API_QUERY_TIMEOUT_MS (default 60000) is the server-side time
 *   budget of a data statement, CHAINGRAPH_API_AGGREGATE_TIMEOUT_MS (default
 *   30000, at most the former unless set) that of an aggregate / group-by
 *   statement; over budget, ClickHouse stops the statement and the API
 *   answers a QUERY_TIMEOUT error. The ClickHouse client's request timeout is
 *   the larger budget plus a margin, and a timed-out read is not retried;
 * - admission control (admission.ts, fix-pass-8.md): each query request is
 *   admitted or refused before any statement runs, by its predicted
 *   ClickHouse CPU and event-loop time against two token budgets
 *   (ClickHouse: the server's cores × CHAINGRAPH_API_CPU_TARGET / API
 *   processes; the loop: CHAINGRAPH_API_LOOP_TARGET), light requests with a
 *   reserved share; refused, the API answers OVERLOADED (HTTP 503,
 *   Retry-After) at once. CHAINGRAPH_API_ADMISSION=off disables it. The
 *   cores are read at startup (`max_threads` of the API user;
 *   CHAINGRAPH_API_CLICKHOUSE_CORES overrides); one `admission:` line logs
 *   the sizing; admissionOptionsFromEnv lists the tuning variables;
 * - CHAINGRAPH_API_WORKERS (default 1): worker processes sharing the port
 *   (node:cluster), each with its own event loop and its share of the
 *   ClickHouse budget: a single loop is the API's limit at mainnet scale
 *   (fix-pass-8.md §2);
 * - a request whose client goes away has its unsent statements dropped and
 *   its running ones cancelled (KILL QUERY by query_id).
 */
import cluster from 'node:cluster';

import {
  ClickHouseClient,
  clickHouseConfigFromEnv,
} from '../store/clickhouse/client.js';

import {
  admissionOptionsFromEnv,
  admissionSummary,
  readClickHouseServerLimits,
} from './admission.js';
import {
  authchainMaxBranchesFromEnv,
  requestTimeoutMarginMs,
  statementTimeoutsFromEnv,
  utxoSetPolicyFromEnv,
} from './db.js';
import { loadTrustedDocuments } from './plugins.js';
import { createApi, listen } from './server.js';

const defaultPort = 4000;

/** CHAINGRAPH_API_WORKERS: worker processes (default 1: no cluster). */
const workersFromEnv = (env: NodeJS.ProcessEnv) => {
  const raw = env.CHAINGRAPH_API_WORKERS;
  if (raw === undefined || raw === '') return 1;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error('CHAINGRAPH_API_WORKERS must be an integer >= 1.');
  }
  return value;
};

const main = async () => {
  const { env } = process;
  const workers = workersFromEnv(env);
  const statementTimeouts = statementTimeoutsFromEnv(env);
  const client = new ClickHouseClient({
    ...clickHouseConfigFromEnv(env),
    requestTimeoutMs:
      Math.max(statementTimeouts.data, statementTimeouts.aggregate) +
      requestTimeoutMarginMs,
  });
  /* lane sizes follow the ClickHouse server's cores (fix-pass-4.md §8) */
  const serverLimits = await readClickHouseServerLimits(client);
  const admission = admissionOptionsFromEnv(
    env,
    serverLimits.maxThreads,
    workers
  );
  if (cluster.isPrimary || cluster.worker?.id === 1) {
    // eslint-disable-next-line no-console
    console.log(
      `${admissionSummary(
        admission,
        serverLimits.maxConcurrentQueriesForUser
      )} workers=${workers}`
    );
  }
  const trusted = env.CHAINGRAPH_API_TRUSTED_DOCUMENTS;
  const api = createApi(client, {
    ...(env.CHAINGRAPH_API_AGENT_URL === undefined
      ? {}
      : { agentUrl: env.CHAINGRAPH_API_AGENT_URL }),
    ...(env.CHAINGRAPH_API_MAX_RESULT_ROWS === undefined
      ? {}
      : { maxResultRows: Number(env.CHAINGRAPH_API_MAX_RESULT_ROWS) }),
    admission,
    authchainMaxBranches: authchainMaxBranchesFromEnv(env),
    responseCache: env.CHAINGRAPH_API_RESPONSE_CACHE === 'off' ? false : {},
    statementTimeouts,
    utxoSet: utxoSetPolicyFromEnv(env),
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

const workerCount = workersFromEnv(process.env);
if (workerCount > 1 && cluster.isPrimary) {
  /* the primary only forks (and replaces a worker that exits); workers share the port */
  Array.from({ length: workerCount }).forEach(() => cluster.fork());
  cluster.on('exit', (worker, code, signal) => {
    // eslint-disable-next-line no-console
    console.error(
      `API worker ${String(worker.process.pid)} exited (${String(
        signal || code
      )}); starting another`
    );
    cluster.fork();
  });
  const stopAll = () => {
    cluster.removeAllListeners('exit');
    Object.values(cluster.workers ?? {}).forEach((worker) => worker?.kill());
  };
  process.once('SIGINT', stopAll);
  process.once('SIGTERM', stopAll);
} else {
  main().catch((error: unknown) => {
    // eslint-disable-next-line no-console
    console.error(error);
    process.exitCode = 1;
  });
}
