/* eslint-disable @typescript-eslint/no-magic-numbers, @typescript-eslint/naming-convention, functional/no-let, functional/no-loop-statement, no-await-in-loop, functional/no-throw-statement, camelcase */
// cspell:ignore clickhouse EWMA mispredicted unticketed utilisation cgapi uncontended
/**
 * Request-level admission (fix pass 8, docs/clickhouse-port/fix-pass-8.md):
 * - a request is admitted or refused once, before any statement runs, by
 *   its predicted ClickHouse CPU and event-loop time against two token
 *   budgets with a light reserve; refused at once (OVERLOADED, 503,
 *   Retry-After); an admitted request is never refused part-way;
 * - the prediction is learned per plan key (document, nodes, variable
 *   magnitudes) from measured costs (system.query_log, loop attribution),
 *   and the budgets are reconciled with them;
 * - abandoned work (fix pass 4): a request whose client went away has its
 *   unsent statements dropped and its running statement killed.
 */
import type { ExecutionContext } from 'ava';
import test from 'ava';
import { GraphQLError, parse } from 'graphql';

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

import type {
  AdmissionOptions,
  CpuReader,
  PlanKeys,
  StatementRecord,
} from './admission.js';
import {
  Admission,
  admissionOptionsFromEnv,
  admissionSummary,
  Budget,
  defaultAdmissionOptions,
  overloadedCode,
  planKeys,
  requestAbortedCode,
} from './admission.js';
import { ApiDb, apiQueryIdPrefix, requestScope } from './db.js';
import { createApi, listen } from './server.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

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

/** Options with small, round budgets: 10 CPU-s/s and 1,000 loop-ms/s, 1 s bursts, light share 0.2. */
const options = (
  overrides: Partial<AdmissionOptions> = {}
): AdmissionOptions => ({
  ...defaultAdmissionOptions,
  clickhouse: { burstSeconds: 1, lightShare: 0.2, ratePerSecond: 10 },
  cpuSource: 'rows',
  loop: { burstSeconds: 1, lightShare: 0.2, ratePerSecond: 1_000 },
  ...overrides,
});

/** A quiet loop for the unit tests (no attribution unless a test drives it). */
const quietMeter = () => ({ activeMs: 0, elapsedMs: 100 });

const keys = (name: string): PlanKeys => ({
  document: `d:${name}`,
  plan: `p:${name}`,
  roots: `r:${name}`,
});

/** Teach the model a request's cost (as a finished, measured request would). */
const teach = (
  admission: Admission,
  name: string,
  cost: number | { cpuSeconds: number; loopMs: number }
) => {
  admission.model.observe(
    keys(name),
    typeof cost === 'number' ? { cpuSeconds: cost, loopMs: 1 } : cost
  );
};

test('budget: shares refill at their rates, a full share overflows into the other, debt is allowed and repaid', (t) => {
  const budget = new Budget(
    { burstSeconds: 1, lightShare: 0.25, ratePerSecond: 8 },
    0
  );
  t.deepEqual(budget.level, { heavy: 6, light: 2 }, 'starts full');
  t.is(budget.take('heavy', 20), 'heavy', 'admitted while positive');
  t.is(budget.level.heavy, -14, 'into debt');
  t.is(budget.take('heavy', 1), undefined, 'refused in debt');
  t.is(budget.secondsToPositive('heavy'), 14 / 6);
  /* light is full: its refill overflows into heavy, which repays at the whole rate */
  budget.refill(1_000);
  t.is(budget.level.light, 2);
  t.is(budget.level.heavy, -6, '6 + 2 overflowed');
  t.is(budget.take('light', 0.5), 'light', 'the light share is untouched');
  /* light borrows heavy tokens only when its own are spent and heavy holds the cost */
  const borrow = new Budget(
    { burstSeconds: 1, lightShare: 0.5, ratePerSecond: 2 },
    0
  );
  t.is(borrow.take('light', 1.5), 'light');
  t.is(borrow.take('light', 0.5), 'heavy', 'borrowed');
  t.is(borrow.take('light', 1), undefined, 'heavy holds 0.5 < 1');
  t.deepEqual(borrow.fill(), { heavy: 0.5, light: -0.5 });
});

test('admission: refuses at once when the budget is spent (503, Retry-After, the reason); never more than the in-flight cap of heavy requests', (t) => {
  const admission = new Admission(options(), quietMeter);
  teach(admission, 'heavy', 5);
  t.is(admission.admit(keys('heavy'), 0).cls, 'heavy');
  /* heavy share 8: 5 + 5 charged (the second while positive: debt 2), the third refused */
  admission.admit(keys('heavy'), 0);
  t.is(admission.budgets.clickhouse.level.heavy, -2);
  const started = performance.now();
  const refused = t.throws(() => admission.admit(keys('heavy'), 0));
  t.true(performance.now() - started < 50, 'refused in under 50 ms');
  t.is(codeOf(refused), overloadedCode);
  const { extensions } = refused as GraphQLError;
  t.is(extensions.reason, 'clickhouse');
  t.is(extensions.lane, 'heavy');
  t.deepEqual(extensions.http, {
    headers: { 'Retry-After': String(extensions.retryAfterSeconds) },
    status: 503,
  });
  t.is(
    extensions.retryAfterSeconds,
    1,
    'debt 2 at 8 CPU-s/s: 0.25 s, at least 1'
  );
  t.is(admission.stats.refused.clickhouse, 1);
  /* the in-flight cap, whatever the budget */
  const capped = new Admission(options({ heavyMaxInFlight: 1 }), quietMeter);
  teach(capped, 'heavy', 2);
  const first = capped.admit(keys('heavy'), 0);
  const refusedCap = t.throws(() => capped.admit(keys('heavy'), 0));
  t.is((refusedCap as GraphQLError).extensions.reason, 'in_flight');
  capped.finish(first, 0);
  t.is(capped.admit(keys('heavy'), 0).cls, 'heavy', 'a slot freed');
  t.is(capped.stats.refused.in_flight, 1);
});

test('admission: light requests keep their reserve while heavy ones are refused', (t) => {
  const admission = new Admission(options(), quietMeter);
  teach(admission, 'heavy', 20);
  teach(admission, 'light', 0.01);
  admission.admit(keys('heavy'), 0);
  t.throws(() => admission.admit(keys('heavy'), 0), {
    message: /overloaded/u,
  });
  for (let index = 0; index < 100; index += 1) {
    t.is(admission.admit(keys('light'), 0).cls, 'light');
  }
  t.is(admission.stats.admitted.light, 100);
  /* the loop budget has its own reserve: heavy loop work never spends the light share */
  const loopBound = new Admission(options(), quietMeter);
  teach(loopBound, 'loopHeavy', { cpuSeconds: 0.01, loopMs: 2_000 });
  teach(loopBound, 'light', 0.01);
  loopBound.admit(keys('loopHeavy'), 0);
  const refused = t.throws(() => loopBound.admit(keys('loopHeavy'), 0));
  t.is((refused as GraphQLError).extensions.reason, 'loop');
  t.is(loopBound.admit(keys('light'), 0).cls, 'light');
  t.deepEqual(
    loopBound.budgets.clickhouse.level,
    { heavy: 8 - 0.01, light: 2 - 0.01 },
    'a refusal by the loop gives the CPU charge back'
  );
});

test('admission: predictions from the plan key, then the document, then the root fields; a request nothing is known about is heavy', (t) => {
  const admission = new Admission(options(), quietMeter);
  const unknown = admission.predict(keys('new'));
  t.is(unknown.source, 'default');
  t.is(unknown.cls, 'heavy', 'unknown: heavy even at a light default cost');
  admission.model.observe(
    { document: 'd:x', plan: 'p:x1', roots: 'r:transactions' },
    { cpuSeconds: 4, loopMs: 100 }
  );
  t.deepEqual(
    admission.predict({ document: 'd:x', plan: 'p:x2', roots: 'r:other' }),
    { cls: 'heavy', cost: { cpuSeconds: 4, loopMs: 100 }, source: 'document' }
  );
  t.is(
    admission.predict({ document: 'd:y', plan: 'p:y', roots: 'r:transactions' })
      .source,
    'roots'
  );
  /* EWMA (0.3) of measured costs */
  admission.model.observe(
    { document: 'd:x', plan: 'p:x1', roots: 'r:transactions' },
    { cpuSeconds: 0, loopMs: 0 }
  );
  t.deepEqual(
    admission.predict({ document: 'd:x', plan: 'p:x1', roots: 'r:z' }).cost,
    { cpuSeconds: 4 * 0.7, loopMs: 100 * 0.7 }
  );
});

test('plan keys: the same document with other hashes is the same plan; a limit or list of another magnitude, or another node, is another', (t) => {
  const document = parse(
    'query Q($limit: Int, $ids: [String!]) { transactions(limit: $limit) { hash } }'
  );
  const a = planKeys(
    document,
    'Q',
    { ids: ['aa', 'bb'], limit: 10, token: 'x' },
    ['n1']
  );
  const b = planKeys(
    document,
    'Q',
    { ids: ['cc', 'dd'], limit: 12, token: 'y' },
    ['n1']
  );
  t.deepEqual(a, b);
  t.not(
    planKeys(document, 'Q', { ids: ['aa', 'bb'], limit: 10_000 }, ['n1']).plan,
    a.plan
  );
  t.not(
    planKeys(document, 'Q', { ids: Array(100).fill('a'), limit: 10 }, ['n1'])
      .plan,
    a.plan
  );
  t.not(
    planKeys(document, 'Q', { ids: ['aa', 'bb'], limit: 10 }, ['n2']).plan,
    a.plan
  );
  t.is(a.roots, 'r:query:transactions');
  t.is(
    planKeys(parse('{ transactions(limit: 1) { hash } }'), undefined, {}, [])
      .roots,
    a.roots
  );
});

test('admission: charges are reconciled with measured costs; a mispredicted light request is charged to the heavy share', (t) => {
  let busy = 0;
  const admission = new Admission(options(), () => {
    const activeMs = busy;
    busy = 0;
    return { activeMs, elapsedMs: 100 };
  });
  teach(admission, 'q', { cpuSeconds: 0.1, loopMs: 5 });
  const ticket = admission.admit(keys('q'), 0);
  t.is(ticket.cls, 'light');
  t.deepEqual(admission.budgets.clickhouse.level, { heavy: 8, light: 1.9 });
  /* 300 ms of loop work while no statement is waiting: all of it this request's */
  busy = 300;
  admission.tick();
  t.is(ticket.loopMs, 300);
  /* a statement waiting for ClickHouse is not charged loop time */
  ticket.waiting = 1;
  busy = 50;
  admission.tick();
  t.is(ticket.loopMs, 300);
  ticket.waiting = 0;
  const statement: StatementRecord = {
    queryId: 'q1',
    rows: 10_000_000,
    shape: 's',
  };
  ticket.statements.push(statement);
  admission.statementFinished(ticket, statement, 0);
  admission.finish(ticket, 0);
  /* rows × 0.3 µs = 3 CPU-s: heavy now; the light share gets its 0.1 back */
  t.is(admission.budgets.clickhouse.level.light, 2);
  t.is(admission.budgets.clickhouse.level.heavy, 5);
  t.is(admission.budgets.loop.level.light, 200, 'loop charge given back');
  t.is(admission.budgets.loop.level.heavy, 800 - 300);
  const learned = admission.predict(keys('q'));
  t.is(learned.cls, 'heavy');
  t.is(learned.cost.loopMs, 5 * 0.7 + 300 * 0.3);
  t.is(admission.stats.inFlight.light, 0);
});

test('admission: the CPU ledger replaces estimates with system.query_log measurements, also for statements outside requests; without the grant it falls back to rows', async (t) => {
  const measured = new Map<string, number>();
  const reader: CpuReader = async (ids) =>
    new Map(
      ids.flatMap((id) => (measured.has(id) ? [[id, measured.get(id)!]] : []))
    );
  const admission = new Admission(
    options({ cpuSource: 'query_log' }),
    quietMeter
  );
  admission.start(reader);
  t.teardown(() => {
    admission.stop();
  });
  teach(admission, 'q', 1);
  const ticket = admission.admit(keys('q'), 0);
  const statement: StatementRecord = {
    queryId: 'a',
    rows: 1_000_000,
    shape: 's',
  };
  ticket.statements.push(statement);
  admission.statementFinished(ticket, statement, 0);
  admission.finish(ticket, 0);
  t.is(admission.pendingStatements, 1, 'awaiting its query_log row');
  t.is(
    admission.budgets.clickhouse.level.heavy,
    8 - 1,
    'until measured the prediction stands (not rows × CPU per row)'
  );
  /* a live re-run (no ticket) is charged its estimate at once */
  const live: StatementRecord = { queryId: 'b', rows: 2_000_000, shape: 's2' };
  admission.statementFinished(undefined, live, 0);
  t.is(admission.stats.unticketed, 1);
  await admission.pollLedger(0);
  t.is(admission.pendingStatements, 2, 'not logged yet: kept');
  measured.set('a', 2_500_000);
  measured.set('b', 100_000);
  await admission.pollLedger(0);
  t.is(admission.pendingStatements, 0);
  t.is(admission.stats.ledgerResolved, 2);
  t.is(statement.cpuSeconds, 2.5);
  /* the request reconciled to 2.5 CPU-s; the live re-run (estimated 0.6: heavy) to 0.1 */
  t.true(
    Math.abs(admission.budgets.clickhouse.level.heavy - (8 - 2.5 - 0.1)) < 1e-9,
    String(admission.budgets.clickhouse.level.heavy)
  );
  t.is(admission.budgets.clickhouse.level.light, 2);
  t.is(admission.predict(keys('q')).cost.cpuSeconds, 1 * 0.7 + 2.5 * 0.3);
  /* a statement whose row never comes keeps its estimate */
  const lost: StatementRecord = { queryId: 'c', rows: 1, shape: 's' };
  admission.statementFinished(undefined, lost, 0);
  await admission.pollLedger(70_000);
  t.is(admission.stats.ledgerExpired, 1);
  /* no grant on system.query_log: the ledger stops, rows estimate the CPU */
  const denied = new Admission(options({ cpuSource: 'query_log' }), quietMeter);
  denied.start(async () => {
    throw new Error('ACCESS_DENIED');
  });
  t.teardown(() => {
    denied.stop();
  });
  denied.statementFinished(undefined, { queryId: 'x', rows: 1, shape: 's' }, 0);
  t.is(denied.cpuSource, 'query_log');
  await denied.pollLedger(0);
  t.is(denied.cpuSource, 'rows');
  t.regex(denied.stats.ledgerError ?? '', /ACCESS_DENIED/u);
});

test('admission: a heavy request admitted under contention gets the contended settings; uncontended it runs with the server threads', (t) => {
  const admission = new Admission(
    options({
      contendedSettings: { max_threads: 4 },
      heavyMaxInFlight: 8,
      settings: { heavy: {}, light: { priority: 1 } },
    }),
    quietMeter
  );
  teach(admission, 'heavy', 3);
  teach(admission, 'light', 0.01);
  t.deepEqual(admission.admit(keys('heavy'), 0).settings, {}, 'share full');
  t.deepEqual(
    admission.admit(keys('heavy'), 0).settings,
    { max_threads: 4 },
    'share 2 of 8: under half'
  );
  t.deepEqual(admission.admit(keys('light'), 0).settings, { priority: 1 });
  t.is(admission.stats.contended, 1);
  /* half the in-flight cap taken: contended whatever the budget */
  const busy = new Admission(
    options({ contendedSettings: { max_threads: 4 }, heavyMaxInFlight: 2 }),
    quietMeter
  );
  teach(busy, 'heavy', 0.6);
  t.deepEqual(busy.admit(keys('heavy'), 0).settings, {});
  t.deepEqual(busy.admit(keys('heavy'), 0).settings, { max_threads: 4 });
  t.deepEqual(
    admissionOptionsFromEnv({}, 14).contendedSettings,
    { max_threads: 4 },
    '2 × 14 cores / 7 heavy slots'
  );
  t.deepEqual(
    admissionOptionsFromEnv(
      { CHAINGRAPH_API_HEAVY_CONTENDED_MAX_THREADS: '0' },
      48
    ).contendedSettings,
    {}
  );
});

test('admission: back-to-back heavy requests on an idle server keep the server threads; leftover debt never refuses or throttles (fix-pass-9.md §2)', async (t) => {
  /* mainnet L15: ~61 CPU-s per request, 1.65 s each, query_log measurements ~2 s late, default budgets (48 cores) */
  const measured = new Map<string, number>();
  const admission = new Admission(
    { ...defaultAdmissionOptions, cpuSource: 'query_log' },
    quietMeter
  );
  admission.start(
    async (ids) =>
      new Map(
        ids.flatMap((id) => (measured.has(id) ? [[id, measured.get(id)!]] : []))
      )
  );
  t.teardown(() => {
    admission.stop();
  });
  teach(admission, 'L15', { cpuSeconds: 61, loopMs: 5 });
  /* the budgets' clock started at construction */
  let now = performance.now();
  for (let run = 0; run < 20; run += 1) {
    const ticket = admission.admit(keys('L15'), now);
    t.deepEqual(ticket.settings, {}, `run ${run}: the server's threads`);
    const statement: StatementRecord = {
      queryId: `l15-${run}`,
      rows: 100_000_000,
      shape: 'L15',
    };
    ticket.statements.push(statement);
    now += 1_650;
    admission.statementFinished(ticket, statement, now);
    admission.finish(ticket, now);
    /* the measurement of the previous run arrives while this one is being answered */
    measured.set(statement.queryId, 61_000_000);
    if (run % 2 === 1) await admission.pollLedger(now);
    now += 20;
  }
  t.is(admission.stats.contended, 0, 'never contended');
  t.is(admission.stats.refused.clickhouse, 0);
  await admission.pollLedger(now);
  /* L17 (nothing known: heavy, 0.25 CPU-s) right after: admitted at full threads */
  const l17 = admission.admit(keys('L17'), now);
  t.deepEqual(l17.settings, {});
  admission.finish(l17, now);
  /* a debt left by a heavy request larger than the burst is forgiven on an idle server */
  admission.budgets.clickhouse.level.heavy = -500;
  admission.budgets.clickhouse.level.light = -50;
  const after = admission.admit(keys('L15'), now);
  t.deepEqual(after.settings, {}, 'idle: neither refused nor throttled');
  t.is(admission.stats.refused.clickhouse, 0);
  /* while it runs the server is not idle: the debt it left refuses the next heavy request */
  t.throws(() => admission.admit(keys('L15'), now), { message: /overloaded/u });
  admission.finish(after, now);
});

test('admission: a reconciled charge is one net adjustment (giving back first was capped at the capacity)', (t) => {
  const admission = new Admission(options(), quietMeter);
  teach(admission, 'heavy', 5);
  const start = performance.now();
  const ticket = admission.admit(keys('heavy'), start);
  t.true(Math.abs(admission.budgets.clickhouse.level.heavy - 3) < 1e-6);
  /* 1 s later the share is full again; the request cost what it was charged */
  ticket.statements.push({ queryId: 'h', rows: 5 / 0.3e-6, shape: 'h' });
  admission.finish(ticket, start + 1_000);
  t.true(
    Math.abs(admission.budgets.clickhouse.level.heavy - 8) < 1e-6,
    `not charged twice: ${admission.budgets.clickhouse.level.heavy}`
  );
});

test('admission: a busy event loop refuses heavy requests whatever the budget says; light ones still run', (t) => {
  const admission = new Admission(options(), () => ({
    activeMs: 100,
    elapsedMs: 100,
  }));
  teach(admission, 'heavy', 1);
  teach(admission, 'light', 0.01);
  for (let index = 0; index < 20; index += 1) admission.tick();
  t.true(admission.loopUtilisation > 0.95);
  const refused = t.throws(() => admission.admit(keys('heavy'), 0));
  t.is((refused as GraphQLError).extensions.reason, 'loop_guard');
  t.is(admission.admit(keys('light'), 0).cls, 'light');
});

test('admission: options from the environment and the startup line', (t) => {
  const fromServer = admissionOptionsFromEnv({}, 48);
  t.is(fromServer.clickhouse.ratePerSecond, 48 * 0.9, '48 cores × 0.9');
  /* two heavy-lane threads: the light share is this loop, the heavy share the lane's two */
  const withLane = admissionOptionsFromEnv({}, 48, 1, 2);
  t.is(withLane.loop.ratePerSecond, 850 * 3);
  t.is(withLane.loop.lightShare, 1 / 3);
  t.is(
    withLane.clickhouse.lightShare,
    0.25,
    'the ClickHouse budget is unchanged'
  );
  t.is(fromServer.clickhouse.burstSeconds, 4);
  t.is(fromServer.loop.burstSeconds, 1);
  t.is(fromServer.loop.ratePerSecond, 850);
  t.is(fromServer.heavyMaxInFlight, 24);
  t.deepEqual(fromServer.settings, { heavy: {}, light: {} });
  t.is(fromServer.cpuSource, 'query_log');
  t.deepEqual(fromServer.sizing, {
    cores: 48,
    cpuTarget: 0.9,
    heavyThreads: 0,
    loopTarget: 0.85,
    processes: 1,
    source: 'clickhouse',
  });
  const shared = admissionOptionsFromEnv(
    {
      CHAINGRAPH_API_CPU_TARGET: '0.5',
      CHAINGRAPH_API_HEAVY_PRIORITY: '2',
      CHAINGRAPH_API_LIGHT_MAX_CPU_MS: '100',
      CHAINGRAPH_API_PROCESSES: '4',
    },
    48,
    2
  );
  t.is(shared.clickhouse.ratePerSecond, 6, '48 × 0.5 / 4 processes');
  t.is(shared.heavyMaxInFlight, 6);
  t.deepEqual(shared.settings.heavy, { priority: 2 });
  t.is(shared.lightMaxCpuMs, 100);
  const workers = admissionOptionsFromEnv({}, 48, 4).clickhouse;
  t.is(workers.ratePerSecond, 10.8, 'workers divide the rate');
  t.is(
    workers.ratePerSecond * workers.burstSeconds,
    48 * 0.9 * 4,
    'each process may burst like the server'
  );
  t.is(admissionOptionsFromEnv({}, undefined).sizing?.source, 'default');
  t.is(
    admissionOptionsFromEnv({ CHAINGRAPH_API_CLICKHOUSE_CORES: '64' }, 48)
      .clickhouse.ratePerSecond,
    64 * 0.9
  );
  t.false(admissionOptionsFromEnv({ CHAINGRAPH_API_ADMISSION: 'off' }).enabled);
  t.throws(() => admissionOptionsFromEnv({ CHAINGRAPH_API_LOOP_TARGET: '2' }));
  t.throws(() => admissionOptionsFromEnv({ CHAINGRAPH_API_CPU_SOURCE: 'x' }));
  t.throws(() =>
    admissionOptionsFromEnv({ CHAINGRAPH_API_HEAVY_WORKLOAD: 'a b' })
  );
  t.regex(
    admissionSummary(fromServer, 154),
    /^admission: on \(request level\) cores=48 \(clickhouse\) processes=1 cpu_budget=43\.2 CPU-s\/s burst 4 s loop_budget=850 ms\/s burst 1 s light_share=0\.25 light<=500 CPU-ms,25 loop-ms heavy_in_flight=24 heavy_contended=0\.5:max_threads 4 loop_guard=0\.95 unknown=250 CPU-ms,20 loop-ms cpu_source=query_log light_settings=server default heavy_settings=server default max_concurrent_queries_for_user=154$/u
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

/** A one-block store and an API whose data statements wait for `gate` before they are sent. */
const gatedApi = async (
  t: ExecutionContext,
  name: string,
  admission: AdmissionOptions
) => {
  const { name: database, openStore } = await scratch(
    t,
    name,
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
  const state = { gate: deferred(), held: true, sent: 0 };
  state.gate.resolve();
  state.gate = deferred();
  const client = new ClickHouseClient({
    ...connectionFor(database),
    faultBeforeRequest: async (request) => {
      if (request.queryId?.startsWith(apiQueryIdPrefix) !== true) return;
      if (state.held) await state.gate.promise;
      state.sent += 1;
    },
  });
  t.teardown(async () => client.close());
  const api = createApi(client, { admission, responseCache: false });
  t.teardown(() => {
    api.db.admission.stop();
  });
  const server = await listen(api);
  t.teardown(async () => server.close());
  /* API data statements the server saw (query_log) */
  const logged = async () => {
    await client.command('SYSTEM FLUSH LOGS');
    const [row] = await client.query<{ n: string }>(
      `SELECT toString(count()) AS n FROM system.query_log
       WHERE type = 'QueryStart' AND current_database = {db:String} AND query_id LIKE 'cgapi-%'`,
      { db: database }
    );
    return Number(row?.n ?? 0);
  };
  const post = async (query: string, signal?: AbortSignal) =>
    fetch(server.url, {
      body: JSON.stringify({ query }),
      headers: { 'content-type': 'application/json' },
      method: 'POST',
      ...(signal === undefined ? {} : { signal }),
    });
  return { api, block0, logged, post, state };
};

e2e(
  '[e2e] request-level admission over HTTP: refused at once with 503 + Retry-After before any statement is sent; an admitted request is never refused part-way',
  async (t) => {
    t.timeout(120_000);
    const { api, block0, post, state } = await gatedApi(t, 'admit', options());
    const { admission } = api.db;
    const query = `{ a: blocks_by_hash(hashes: ["${block0.hash}"]) { hash } b: blocks_by_hash(hashes: ["${block0.hash}", "${block0.hash}"]) { hash } }`;
    /* the first request (nothing known: heavy) is admitted; its statements wait */
    const first = post(query);
    for (let attempt = 0; attempt < 100 && state.sent === 0; attempt += 1) {
      if (admission.stats.inFlight.heavy > 0) break;
      await sleep(20);
    }
    t.is(admission.stats.inFlight.heavy, 1);
    /* spend the budget while it runs: the next request is refused at once, nothing sent */
    admission.budgets.clickhouse.level.heavy = -50;
    admission.budgets.clickhouse.level.light = -50;
    const sentBefore = state.sent;
    const started = Date.now();
    const refused = await post(query);
    const elapsed = Date.now() - started;
    const body = (await refused.json()) as {
      errors?: { extensions?: { code?: string; reason?: string } }[];
    };
    t.is(refused.status, 503);
    t.truthy(refused.headers.get('retry-after'));
    t.is(body.errors?.[0]?.extensions?.code, overloadedCode);
    t.is(body.errors?.[0]?.extensions?.reason, 'clickhouse');
    t.true(elapsed < 250, `refused in ${elapsed} ms`);
    t.is(state.sent, sentBefore, 'the refused request sent nothing');
    /* the admitted request's statements all run although the budget is spent */
    state.held = false;
    state.gate.resolve();
    const response = await first;
    t.is(response.status, 200);
    const result = (await response.json()) as {
      data?: { a: unknown[]; b: unknown[] };
      errors?: unknown[];
    };
    t.is(result.errors, undefined);
    t.is(result.data?.a.length, 1);
    t.true(state.sent >= 2, 'every statement of the admitted request ran');
    t.is(
      admission.stats.inFlight.heavy,
      0,
      'the ticket ended with the response'
    );
  }
);

e2e(
  '[e2e] abandoned work over HTTP: a client that disconnects before its statements are sent never sends them',
  async (t) => {
    t.timeout(120_000);
    const { api, block0, logged, post, state } = await gatedApi(
      t,
      'abandon',
      options()
    );
    const query = `{ blocks_by_hash(hashes: ["${block0.hash}"]) { hash } }`;
    const controllers = [1, 2, 3].map(() => new AbortController());
    const waiting = controllers.map(async (controller) =>
      post(query, controller.signal).catch(() => undefined)
    );
    const { admission } = api.db;
    for (
      let attempt = 0;
      attempt < 100 &&
      admission.stats.inFlight.heavy + admission.stats.inFlight.light < 3;
      attempt += 1
    ) {
      await sleep(20);
    }
    t.is(
      admission.stats.inFlight.heavy + admission.stats.inFlight.light,
      3,
      'three requests admitted, their statements held'
    );
    controllers.forEach((controller) => {
      controller.abort();
    });
    await Promise.all(waiting);
    for (
      let attempt = 0;
      attempt < 100 &&
      admission.stats.inFlight.heavy + admission.stats.inFlight.light > 0;
      attempt += 1
    ) {
      await sleep(20);
    }
    t.is(
      admission.stats.inFlight.heavy + admission.stats.inFlight.light,
      0,
      'the tickets ended with the closed responses'
    );
    state.held = false;
    state.gate.resolve();
    await sleep(300);
    t.is(await logged(), 0, 'no statement of an abandoned request was sent');
  }
);
