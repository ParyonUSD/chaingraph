/* eslint-disable complexity, no-process-exit, functional/no-let, @typescript-eslint/no-magic-numbers, max-params */
// cspell:ignore clickhouse serialisation serialised recognise
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
 * - CHAINGRAPH_API_HEAVY_THREADS (default 1; 0: none): per process, worker
 *   threads that answer admitted heavy requests (heavy-lane.ts, front.ts),
 *   so their decoding, execution and serialisation never block the loop
 *   that serves light requests, refusals and cache hits (fix-pass-9.md §3).
 *   Off with admission (the predicted class routes a request);
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
import {
  defaultWarmupTimeoutMs,
  loadWarmupEntries,
  runWarmup,
} from './warmup.js';

const defaultPort = 4000;

/** An integer variable of at least `min` (`fallback` when unset). */
const integerFromEnv = (
  env: NodeJS.ProcessEnv,
  name: string,
  fallback: number,
  min: number
) => {
  const raw = env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < min) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(`${name} must be an integer >= ${min}.`);
  }
  return value;
};

/** CHAINGRAPH_API_WORKERS: worker processes (default 1: no cluster). */
const workersFromEnv = (env: NodeJS.ProcessEnv) =>
  integerFromEnv(env, 'CHAINGRAPH_API_WORKERS', 1, 1);

/** CHAINGRAPH_API_HEAVY_THREADS: heavy-lane worker threads per process (heavy-lane.ts). */
export const defaultHeavyThreads = 1;

const heavyThreadsFromEnv = (env: NodeJS.ProcessEnv) =>
  integerFromEnv(env, 'CHAINGRAPH_API_HEAVY_THREADS', defaultHeavyThreads, 0);

const main = async () => {
  const { env } = process;
  const workers = workersFromEnv(env);
  const statementTimeouts = statementTimeoutsFromEnv(env);
  const clickhouse = {
    ...clickHouseConfigFromEnv(env),
    requestTimeoutMs:
      Math.max(statementTimeouts.data, statementTimeouts.aggregate) +
      requestTimeoutMarginMs,
  };
  /* lane sizes follow the ClickHouse server's cores (fix-pass-4.md §8) */
  const probe = new ClickHouseClient(clickhouse);
  const serverLimits = await readClickHouseServerLimits(probe);
  await probe.close();
  const admissionOff = env.CHAINGRAPH_API_ADMISSION === 'off';
  /* the lane needs admission: the predicted class routes a request */
  const heavyThreads = admissionOff ? 0 : heavyThreadsFromEnv(env);
  const admission = admissionOptionsFromEnv(
    env,
    serverLimits.maxThreads,
    workers,
    heavyThreads
  );
  /*
   * A process's statements stay within its 64 sockets (the backstop counts
   * 72 per process, api-user.ts): the heavy lane's threads get the heavy
   * requests in flight plus snapshot reads, the main thread the rest.
   */
  const laneSockets =
    heavyThreads === 0 ? 0 : Math.min(48, admission.heavyMaxInFlight + 4);
  const client = new ClickHouseClient({
    ...clickhouse,
    maxOpenConnections: 64 - laneSockets,
  });
  if (cluster.isPrimary || cluster.worker?.id === 1) {
    // eslint-disable-next-line no-console
    console.log(
      `${admissionSummary(
        admission,
        serverLimits.maxConcurrentQueriesForUser
      )} workers=${workers} heavy_threads=${heavyThreads}`
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
    ...(heavyThreads === 0
      ? {}
      : {
          heavyLane: {
            clickhouse: {
              ...clickhouse,
              maxOpenConnections: Math.max(
                2,
                Math.ceil(laneSockets / heavyThreads)
              ),
            },
            threads: heavyThreads,
          },
        }),
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
  /* before listening: the first client requests find costs, caches and code warm (warmup.ts) */
  const warmup = env.CHAINGRAPH_API_WARMUP;
  if (warmup !== undefined && warmup !== '') {
    const result = await runWarmup(
      async (url, init) => api.yoga.fetch(url, init),
      loadWarmupEntries(warmup),
      {
        log: (line) => {
          // eslint-disable-next-line no-console
          console.error(line);
        },
        rounds: integerFromEnv(env, 'CHAINGRAPH_API_WARMUP_ROUNDS', 1, 1),
        timeoutMs: integerFromEnv(
          env,
          'CHAINGRAPH_API_WARMUP_TIMEOUT_MS',
          defaultWarmupTimeoutMs,
          0
        ),
      }
    );
    if (cluster.isPrimary || cluster.worker?.id === 1) {
      // eslint-disable-next-line no-console
      console.log(
        `warm-up: ${result.ok} ok, ${result.failed} failed, ${result.skipped} skipped of ${result.entries} entries in ${result.ms} ms`
      );
    }
  }
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
      .catch(() => undefined)
      .finally(() => {
        /* a cluster worker exits once closed (its primary waits for it) */
        if (cluster.isWorker) process.exit(0);
      });
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
    const workers = Object.values(cluster.workers ?? {});
    let running = workers.length;
    cluster.on('exit', () => {
      running -= 1;
      if (running <= 0) process.exit(0);
    });
    workers.forEach((worker) => worker?.process.kill('SIGTERM'));
    /* a worker that does not close in time is killed */
    setTimeout(() => {
      workers.forEach((worker) => worker?.process.kill('SIGKILL'));
      process.exit(0);
    }, 10_000).unref();
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
