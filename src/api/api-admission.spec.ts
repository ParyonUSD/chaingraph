/* eslint-disable @typescript-eslint/no-magic-numbers, @typescript-eslint/naming-convention, functional/no-let, functional/no-loop-statement, no-await-in-loop, camelcase, complexity, functional/no-throw-statement */
// cspell:ignore clickhouse
/**
 * Fix pass 4 (docs/clickhouse-port/fix-pass-4.md §3a, §3b):
 * - admission control: per-lane concurrency, a bounded queue and a bounded
 *   wait; refusals are fast OVERLOADED errors (HTTP 503 + Retry-After);
 *   heavy statements never take light slots;
 * - abandoned work: a request whose client went away leaves the queue, and
 *   its running statement is cancelled on the server (KILL by query_id).
 */
import test from 'ava';
import { GraphQLError } from 'graphql';

import { ClickHouseClient } from '../store/clickhouse/client.js';
import {
  acceptance,
  connectionFor,
  notSaved,
  registerNodes,
  scratch,
  threeBlockChain,
} from '../store/clickhouse/spec-fixtures.js';
import { e2eClickHouseUrl } from '../store/clickhouse/test-support.js';

import type { AdmissionOptions } from './admission.js';
import {
  Admission,
  admissionOptionsFromEnv,
  admissionSummary,
  defaultAdmissionOptions,
  laneConcurrencyForCores,
  overloadedCode,
  requestAbortedCode,
} from './admission.js';
import { ApiDb, apiQueryIdPrefix, requestScope } from './db.js';
import { createApi, listen } from './server.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const options = (
  light: Partial<AdmissionOptions['light']>,
  heavy: Partial<AdmissionOptions['heavy']> = {},
  heavyRows = 1_000
): AdmissionOptions => ({
  ...defaultAdmissionOptions,
  heavy: { ...defaultAdmissionOptions.heavy, ...heavy },
  heavyRows,
  light: { ...defaultAdmissionOptions.light, ...light },
});

const sleep = async (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

const deferred = () => {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
};

const codeOf = (error: unknown) =>
  error instanceof GraphQLError ? error.extensions.code : undefined;

test('admission: a full queue and a long wait are fast OVERLOADED errors with Retry-After', async (t) => {
  const admission = new Admission(
    options({ maxConcurrent: 1, maxQueued: 1, maxWaitMs: 200 })
  );
  const hold = deferred();
  const first = admission.run('a', false, undefined, async () => hold.promise);
  const second = admission.run('a', false, undefined, async () => 'second');
  const started = Date.now();
  const third = await admission
    .run('a', false, undefined, async () => 'third')
    .catch((error: unknown) => error);
  t.true(Date.now() - started < 50, 'a full queue refuses at once');
  t.is(codeOf(third), overloadedCode);
  t.deepEqual((third as GraphQLError).extensions.http, {
    headers: { 'Retry-After': '2' },
    status: 503,
  });
  const waited = await second.catch((error: unknown) => error);
  t.is(codeOf(waited), overloadedCode, 'no slot within maxWaitMs');
  t.is(admission.lanes.light.stats.rejectedFull, 1);
  t.is(admission.lanes.light.stats.rejectedWait, 1);
  hold.resolve();
  await first;
  t.is(admission.lanes.light.stats.running, 0);
});

test('admission: a queued statement whose request goes away leaves the queue and is never sent', async (t) => {
  const admission = new Admission(options({ maxConcurrent: 1 }));
  const hold = deferred();
  const first = admission.run('a', false, undefined, async () => hold.promise);
  const controller = new AbortController();
  let sent = false;
  const queued = admission.run('a', false, controller.signal, async () => {
    sent = true;
  });
  await sleep(20);
  t.is(admission.lanes.light.stats.queued, 1);
  controller.abort();
  const error = await queued.catch((caught: unknown) => caught);
  t.is(codeOf(error), requestAbortedCode);
  t.is(admission.lanes.light.stats.queued, 0);
  t.is(admission.lanes.light.stats.abandoned, 1);
  hold.resolve();
  await first;
  t.false(sent);
});

test('admission: costly shapes (rows read), failures that time out and unmeasured aggregates go to the heavy lane; a saturated heavy lane never delays light statements', async (t) => {
  const admission = new Admission(
    options({ maxConcurrent: 2 }, { maxConcurrent: 1, maxQueued: 0 }, 1_000)
  );
  t.is(admission.laneOf('new', false), 'light', 'a new shape is light');
  t.is(
    admission.laneOf('new', true),
    'heavy',
    'an unmeasured aggregate is heavy'
  );
  await admission.run('agg', true, undefined, async (_settings, report) => {
    report(10);
  });
  t.is(
    admission.laneOf('agg', true),
    'light',
    'a measured cheap aggregate is light'
  );
  await admission.run('slow', false, undefined, async (_settings, report) => {
    report(5_000);
  });
  t.is(admission.laneOf('slow', false), 'heavy', 'one costly run is enough');
  await admission
    .run(
      'timeout',
      false,
      undefined,
      async () => {
        throw new Error('over budget');
      },
      () => true
    )
    .catch(() => undefined);
  t.is(
    admission.laneOf('timeout', false),
    'heavy',
    'a timed-out shape is heavy'
  );
  const hold = deferred();
  const heavy = admission.run(
    'slow',
    false,
    undefined,
    async () => hold.promise
  );
  const refused = await admission
    .run('slow', false, undefined, async () => 'x')
    .catch((error: unknown) => error);
  t.is(codeOf(refused), overloadedCode, 'heavy lane full: refused');
  const started = Date.now();
  t.is(
    await admission.run('point', false, undefined, async () => 'light'),
    'light'
  );
  t.true(Date.now() - started < 50, 'the light lane is untouched');
  hold.resolve();
  await heavy;
  /* cheap runs bring a shape back slowly */
  for (let index = 0; index < 10; index += 1) {
    admission.record('slow', 1);
  }
  t.is(admission.laneOf('slow', false), 'light');
});

test('admission: options from the environment', (t) => {
  const parsed = admissionOptionsFromEnv({
    CHAINGRAPH_API_HEAVY_MAX_CONCURRENT: '3',
    CHAINGRAPH_API_HEAVY_MAX_THREADS: '4',
    CHAINGRAPH_API_HEAVY_ROWS: '900',
    CHAINGRAPH_API_HEAVY_WORKLOAD: 'chaingraph_heavy',
    CHAINGRAPH_API_LIGHT_MAX_QUEUED: '10',
    CHAINGRAPH_API_LIGHT_PRIORITY: '0',
  });
  t.is(parsed.heavy.maxConcurrent, 3);
  t.deepEqual(parsed.heavy.settings, {
    max_threads: 4,
    priority: 2,
    workload: 'chaingraph_heavy',
  });
  t.is(parsed.heavyRows, 900);
  t.is(parsed.light.maxQueued, 10);
  t.deepEqual(parsed.light.settings, {});
  t.false(admissionOptionsFromEnv({ CHAINGRAPH_API_ADMISSION: 'off' }).enabled);
  t.throws(() =>
    admissionOptionsFromEnv({ CHAINGRAPH_API_LIGHT_MAX_CONCURRENT: '0' })
  );
  t.throws(() =>
    admissionOptionsFromEnv({ CHAINGRAPH_API_HEAVY_WORKLOAD: 'a b' })
  );
});

test('admission: lane sizes from the ClickHouse server cores', (t) => {
  t.deepEqual(laneConcurrencyForCores(48, 8), { heavy: 6, light: 24 });
  t.deepEqual(
    {
      heavy: defaultAdmissionOptions.heavy.maxConcurrent,
      light: defaultAdmissionOptions.light.maxConcurrent,
    },
    laneConcurrencyForCores(48, 8),
    'the static defaults are the 48-core sizes'
  );
  t.deepEqual(laneConcurrencyForCores(96, 8), { heavy: 12, light: 48 });
  t.deepEqual(laneConcurrencyForCores(14, 8), { heavy: 1, light: 8 });
  t.deepEqual(laneConcurrencyForCores(4, 8), { heavy: 1, light: 8 });
  t.deepEqual(laneConcurrencyForCores(48, 0), { heavy: 1, light: 24 });
  const fromServer = admissionOptionsFromEnv({}, 32);
  t.is(fromServer.light.maxConcurrent, 16);
  t.is(fromServer.heavy.maxConcurrent, 4);
  t.deepEqual(fromServer.sizing, { cores: 32, source: 'clickhouse' });
  const envCores = admissionOptionsFromEnv(
    { CHAINGRAPH_API_CLICKHOUSE_CORES: '64' },
    32
  );
  t.is(envCores.light.maxConcurrent, 32);
  t.is(envCores.heavy.maxConcurrent, 8);
  t.is(envCores.sizing?.source, 'env');
  const threads = admissionOptionsFromEnv(
    { CHAINGRAPH_API_HEAVY_MAX_THREADS: '16' },
    48
  );
  t.is(threads.heavy.maxConcurrent, 3, 'heavy follows its max_threads');
  const overridden = admissionOptionsFromEnv(
    {
      CHAINGRAPH_API_HEAVY_MAX_CONCURRENT: '2',
      CHAINGRAPH_API_LIGHT_MAX_CONCURRENT: '5',
    },
    48
  );
  t.is(overridden.light.maxConcurrent, 5, 'explicit sizes win');
  t.is(overridden.heavy.maxConcurrent, 2);
  const unknown = admissionOptionsFromEnv({}, undefined);
  t.is(unknown.light.maxConcurrent, 24);
  t.is(unknown.sizing?.source, 'default');
  t.regex(
    admissionSummary(fromServer, 70),
    /^admission: on cores=32 \(clickhouse\) light=16 heavy=4 heavy_max_threads=8 max_concurrent_queries_for_user=70$/u
  );
});

e2e(
  '[e2e] abandoned work: a running statement of a request that goes away is cancelled on the server',
  async (t) => {
    t.timeout(60_000);
    const client = new ClickHouseClient(connectionFor('default'));
    t.teardown(async () => client.close());
    const db = new ApiDb(client);
    const controller = new AbortController();
    let queryId = '';
    db.client.kill = async function kill(id: string) {
      queryId = id;
      return ClickHouseClient.prototype.kill.call(this, id);
    };
    /* minutes of CPU on one thread: only a cancellation ends it early */
    const running = requestScope.run({ signal: controller.signal }, async () =>
      db.query(
        'slow',
        'SELECT count() AS n FROM numbers(100000000000) WHERE sipHash64(number) = 1 SETTINGS max_threads = 1',
        {}
      )
    );
    await sleep(500);
    const started = Date.now();
    controller.abort();
    const error = await running.catch((caught: unknown) => caught);
    t.true(Date.now() - started < 1_000, 'the request fails at once');
    t.is(codeOf(error), requestAbortedCode);
    t.true(queryId.startsWith(apiQueryIdPrefix));
    t.is(db.cancelledStatements, 1);
    let left = 1;
    for (let attempt = 0; attempt < 50 && left > 0; attempt += 1) {
      const [row] = await client.query<{ n: string }>(
        'SELECT toString(count()) AS n FROM system.processes WHERE query_id = {id:String}',
        { id: queryId }
      );
      left = Number(row?.n ?? 0);
      if (left > 0) await sleep(100);
    }
    t.is(left, 0, 'nothing left running on the server');
    await client.command('SYSTEM FLUSH LOGS');
    const [logged] = await client.query<{ code: number }>(
      `SELECT exception_code AS code FROM system.query_log
     WHERE query_id = {id:String} AND type != 'QueryStart' LIMIT 1`,
      { id: queryId }
    );
    t.is(Number(logged?.code), 394, 'QUERY_WAS_CANCELLED');
  }
);

e2e(
  '[e2e] abandoned work over HTTP: a client that disconnects leaves the queue; its statements are never sent',
  async (t) => {
    t.timeout(120_000);
    const { name, openStore } = await scratch(
      t,
      'admission',
      { utxo: 'off' },
      'ch1_api'
    );
    const store = await openStore();
    const { node1 } = await registerNodes(store);
    const { block0 } = threeBlockChain();
    await store.saveBlock({
      block: block0,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1)],
    });
    await store.publishWatermarks();
    /* hold every data statement until released: the single light slot stays busy */
    let gate = deferred();
    let dataStatements = 0;
    const client = new ClickHouseClient({
      ...connectionFor(name),
      faultBeforeRequest: async (request) => {
        if (request.queryId?.startsWith(apiQueryIdPrefix) !== true) return;
        dataStatements += 1;
        await gate.promise;
      },
    });
    t.teardown(async () => client.close());
    const api = createApi(client, {
      admission: options({ maxConcurrent: 1, maxQueued: 8, maxWaitMs: 30_000 }),
      responseCache: false,
    });
    const server = await listen(api);
    t.teardown(async () => server.close());
    const query = `{ blocks_by_hash(hashes: ["${block0.hash}"]) { hash } }`;
    const post = async (signal?: AbortSignal) =>
      fetch(server.url, {
        body: JSON.stringify({ query }),
        headers: { 'content-type': 'application/json' },
        method: 'POST',
        ...(signal === undefined ? {} : { signal }),
      });
    const first = post();
    const lane = api.db.admission.lanes.light.stats;
    for (let attempt = 0; attempt < 100 && lane.running < 1; attempt += 1) {
      await sleep(20);
    }
    t.is(lane.running, 1, 'the first request holds the slot');
    const abandoned = [1, 2, 3].map(() => new AbortController());
    const waiting = abandoned.map(async (controller) =>
      post(controller.signal).catch(() => undefined)
    );
    for (let attempt = 0; attempt < 100 && lane.queued < 3; attempt += 1) {
      await sleep(20);
    }
    t.is(lane.queued, 3, 'three requests wait for the slot');
    abandoned.forEach((controller) => {
      controller.abort();
    });
    await Promise.all(waiting);
    for (let attempt = 0; attempt < 100 && lane.queued > 0; attempt += 1) {
      await sleep(20);
    }
    t.is(lane.queued, 0, 'the disconnected clients left the queue');
    t.is(lane.abandoned, 3);
    const sentBefore = dataStatements;
    gate.resolve();
    gate = deferred();
    gate.resolve();
    const response = await first;
    t.is(response.status, 200);
    await sleep(200);
    t.is(
      dataStatements,
      sentBefore,
      'no statement of an abandoned request was sent'
    );
  }
);

e2e(
  '[e2e] overload over HTTP: a full queue answers 503 OVERLOADED with Retry-After at once',
  async (t) => {
    t.timeout(120_000);
    const { name, openStore } = await scratch(
      t,
      'overload',
      { utxo: 'off' },
      'ch1_api'
    );
    const store = await openStore();
    const { node1 } = await registerNodes(store);
    const { block0 } = threeBlockChain();
    await store.saveBlock({
      block: block0,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1)],
    });
    await store.publishWatermarks();
    const gate = deferred();
    const client = new ClickHouseClient({
      ...connectionFor(name),
      faultBeforeRequest: async (request) => {
        if (request.queryId?.startsWith(apiQueryIdPrefix) !== true) return;
        await gate.promise;
      },
    });
    t.teardown(async () => client.close());
    const api = createApi(client, {
      admission: options({ maxConcurrent: 1, maxQueued: 1, maxWaitMs: 30_000 }),
      responseCache: false,
    });
    const server = await listen(api);
    t.teardown(async () => server.close());
    const post = async () =>
      fetch(server.url, {
        body: JSON.stringify({
          query: `{ blocks_by_hash(hashes: ["${block0.hash}"]) { hash } }`,
        }),
        headers: { 'content-type': 'application/json' },
        method: 'POST',
      });
    const held = [post()];
    const lane = api.db.admission.lanes.light.stats;
    for (let attempt = 0; attempt < 100 && lane.running < 1; attempt += 1) {
      await sleep(20);
    }
    held.push(post());
    for (let attempt = 0; attempt < 100 && lane.queued < 1; attempt += 1) {
      await sleep(20);
    }
    const started = Date.now();
    const refused = await post();
    const elapsed = Date.now() - started;
    const body = (await refused.json()) as {
      errors?: { extensions?: { code?: string } }[];
    };
    t.is(refused.status, 503);
    t.is(refused.headers.get('retry-after'), '2');
    t.is(body.errors?.[0]?.extensions?.code, overloadedCode);
    t.true(elapsed < 500, `refused in ${elapsed} ms`);
    gate.resolve();
    const statuses = await Promise.all(
      held.map(async (response) => (await response).status)
    );
    t.deepEqual(statuses, [200, 200]);
  }
);
