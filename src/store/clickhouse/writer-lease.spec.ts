/* eslint-disable @typescript-eslint/no-magic-numbers, camelcase, @typescript-eslint/naming-convention, max-params */
// cspell:ignore initialise
import test from 'ava';

import { CommitLog, counterOfSeq } from './commit-log.js';
import {
  createScratchDatabase,
  e2eClickHouseUrl,
  hashOf,
  nodeFactsVisibility,
  testSaveRowCounts,
  testSaveSteps,
  visibleOfSave,
} from './test-support.js';
import { readSnapshot, VisibilityPublisher } from './visibility.js';
import {
  defaultTtlMs,
  LeaseHeldError,
  leaseHolder,
  LeaseLostError,
  leaseTtlFromEnv,
  WriterLease,
} from './writer-lease.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const sleep = async (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

const claim = (
  epoch: bigint,
  agentId: string,
  claimedAtMs: number,
  expiresAtMs = 0
) => ({ agentId, claimedAtMs, epoch, expiresAtMs, heartbeatAtMs: claimedAtMs });

test('leaseHolder: highest epoch, then earliest server claim, then agent id', (t) => {
  t.is(leaseHolder([]), undefined);
  t.is(
    leaseHolder([claim(1n, 'a', 5), claim(2n, 'b', 9), claim(2n, 'c', 7)])
      ?.agentId,
    'c'
  );
  t.is(leaseHolder([claim(3n, 'z', 7), claim(3n, 'y', 7)])?.agentId, 'y');
  t.is(leaseHolder([claim(3n, 'z', 7), claim(4n, 'y', 99)])?.agentId, 'y');
});

test('WriterLease: the local deadline fences the holder without any I/O', async (t) => {
  // eslint-disable-next-line functional/no-let
  let now = 0;
  const client = {
    command: async () => undefined,
    insertSelect: async () => undefined,
    query: async () => [] as never[],
  };
  const lease = new WriterLease(client, {
    agentId: 'me',
    monotonicNow: () => now,
    safetyMarginMs: 100,
    settleMs: 0,
    ttlMs: 1000,
  });
  t.throws(
    () => {
      lease.assertHeld();
    },
    { instanceOf: LeaseLostError }
  );
  t.throws(() => lease.epoch, { instanceOf: LeaseLostError });
  // with no claims visible on re-read the claimant cannot confirm: it gives up
  await t.throwsAsync(lease.acquire(), { instanceOf: LeaseLostError });
  // a store that echoes our claim back
  const echo = new WriterLease(
    {
      command: async () => undefined,
      insertSelect: async () => undefined,
      query: async () =>
        [
          {
            agent_id: 'me',
            claimed_ms: '10',
            epoch: '1',
            expires_ms: '1010',
            heartbeat_ms: '10',
            server_now: '10',
          },
        ] as never[],
    },
    {
      agentId: 'me',
      monotonicNow: () => now,
      safetyMarginMs: 100,
      settleMs: 0,
      ttlMs: 1000,
    }
  );
  /*
   * the existing claim is ours (same agent id), so acquiring takes epoch 2; the
   * echo still shows epoch 1 as the holder, so confirmation fails
   */
  await t.throwsAsync(echo.acquire(), { instanceOf: LeaseLostError });
  now = 0;
});

/** A client whose claims are whatever `claims` holds (server time = `serverNow`). */
const fakeLeaseStore = () => {
  const state = {
    claims: [] as { epoch: bigint; agent: string; at: number }[],
    failQueries: false,
    serverNow: 1000,
  };
  const client = {
    command: async (_sql: string, params?: { [key: string]: unknown }) => {
      const epoch = BigInt(String(params?.epoch));
      const agent = String(params?.agent);
      if (!state.claims.some((c) => c.epoch === epoch && c.agent === agent)) {
        state.claims.push({ agent, at: state.serverNow, epoch });
      }
      return undefined;
    },
    insertSelect: async () => undefined,
    query: async (sql: string) => {
      if (state.failQueries) {
        // eslint-disable-next-line functional/no-throw-statement
        throw new Error('ECONNRESET (simulated)');
      }
      if (!sql.includes('FROM writer_lease')) {
        return [{ server_now: String(state.serverNow) }] as never[];
      }
      return state.claims.map((c) => ({
        agent_id: c.agent,
        claimed_ms: String(c.at),
        epoch: c.epoch.toString(),
        expires_ms: String(state.serverNow + 1_000_000),
        heartbeat_ms: String(c.at),
        server_now: String(state.serverNow),
      })) as never[];
    },
  };
  return { client, state };
};

test('leaseTtlFromEnv: default 120 s, env override, invalid values refused', (t) => {
  t.is(defaultTtlMs, 120_000);
  t.is(leaseTtlFromEnv({}), undefined);
  t.is(
    leaseTtlFromEnv({ CHAINGRAPH_CLICKHOUSE_LEASE_TTL_MS: '45000' }),
    45_000
  );
  t.throws(
    () => leaseTtlFromEnv({ CHAINGRAPH_CLICKHOUSE_LEASE_TTL_MS: '5.5' }),
    {
      instanceOf: RangeError,
    }
  );
  t.throws(
    () => leaseTtlFromEnv({ CHAINGRAPH_CLICKHOUSE_LEASE_TTL_MS: '10' }),
    {
      instanceOf: RangeError,
    }
  );
  const lease = new WriterLease(fakeLeaseStore().client, { settleMs: 0 });
  t.is(lease.ttlMs, leaseTtlFromEnv() ?? defaultTtlMs);
});

test('WriterLease: a stalled heartbeat reports lost once, never renews late; reacquire takes a new epoch', async (t) => {
  // eslint-disable-next-line functional/no-let
  let now = 0;
  const { client, state } = fakeLeaseStore();
  const lease = new WriterLease(client, {
    agentId: 'me',
    monotonicNow: () => now,
    renewIntervalMs: 20_000,
    safetyMarginMs: 20_000,
    settleMs: 0,
    ttlMs: 120_000,
  });
  t.is(await lease.acquire(), 1n);
  const losses: LeaseLostError[] = [];
  lease.startHeartbeat((error) => {
    losses.push(error);
  });
  // an on-time tick renews
  now = 20_000;
  t.is(await lease.tick(), undefined);
  // the loop stalls 85 s: the deadline (renewal start 20 s + 120 s - 20 s = 120 s) has passed
  now = 125_000;
  const claimsBefore = state.claims.length;
  const lost = await lease.tick();
  t.is(lost?.reason, 'stalled');
  t.is(lost?.epoch, 1n);
  t.is(state.claims.length, claimsBefore, 'no late heartbeat row was written');
  t.is(losses.length, 1);
  t.is(losses[0], lost);
  t.false(lease.isHeld);
  t.throws(
    () => {
      lease.assertHeld();
    },
    { instanceOf: LeaseLostError }
  );
  // the heartbeat stopped itself: onLost is not called again
  t.is((await lease.tick())?.reason, 'not-held');
  t.is(losses.length, 1);
  // recovery: a new epoch, never the old one
  t.is(await lease.reacquire(), 2n);
  t.true(lease.isHeld);
  t.is(lease.epoch, 2n);
  lease.stopHeartbeat();
});

test('WriterLease: renewal I/O errors are retried until the deadline, then lost', async (t) => {
  // eslint-disable-next-line functional/no-let
  let now = 0;
  const { client, state } = fakeLeaseStore();
  const lease = new WriterLease(client, {
    agentId: 'me',
    monotonicNow: () => now,
    safetyMarginMs: 20_000,
    settleMs: 0,
    ttlMs: 120_000,
  });
  await lease.acquire();
  const losses: LeaseLostError[] = [];
  lease.startHeartbeat((error) => {
    losses.push(error);
  });
  state.failQueries = true;
  now = 20_000;
  t.is(await lease.tick(), undefined);
  now = 60_000;
  t.is(await lease.tick(), undefined);
  t.true(lease.isHeld);
  // the failing renewal crosses the deadline (100 s)
  now = 99_000;
  const original = client.query;
  client.query = async (sql: string) => {
    now = 101_000;
    return original(sql);
  };
  const lost = await lease.tick();
  t.is(lost?.reason, 'deadline');
  t.is(losses.length, 1);
  t.false(lease.isHeld);
});

test('WriterLease: a takeover seen by a renewal is reported as taken-over', async (t) => {
  // eslint-disable-next-line functional/no-let
  let now = 0;
  const { client, state } = fakeLeaseStore();
  const lease = new WriterLease(client, {
    agentId: 'me',
    monotonicNow: () => now,
    settleMs: 0,
    ttlMs: 120_000,
  });
  await lease.acquire();
  const losses: LeaseLostError[] = [];
  lease.startHeartbeat((error) => {
    losses.push(error);
  });
  state.claims.push({ agent: 'other', at: 2000, epoch: 2n });
  now = 20_000;
  t.is((await lease.tick())?.reason, 'taken-over');
  t.is(losses.length, 1);
  // the other agent's claim is unexpired: reacquiring is refused
  await t.throwsAsync(lease.reacquire(), { instanceOf: LeaseHeldError });
});

e2e(
  '[e2e] WriterLease: stall -> lost -> reacquire -> the old epoch is fenced; a racing claimant leaves one holder',
  async (t) => {
    const scratch = await createScratchDatabase('lease_reacquire');
    t.teardown(scratch.drop);
    // eslint-disable-next-line functional/no-let
    let skewMs = 0;
    const monotonicNow = () =>
      Number(process.hrtime.bigint() / 1_000_000n) + skewMs;
    const options = { safetyMarginMs: 300, settleMs: 200, ttlMs: 2000 };
    const lease = new WriterLease(scratch.client, {
      ...options,
      agentId: 'agent',
      monotonicNow,
    });
    t.is(await lease.acquire(), 1n);
    const log = new CommitLog(scratch.client, lease);
    await log.init();
    const publisher = new VisibilityPublisher(scratch.client, log);
    await publisher.init();
    publisher.registerNode(1);
    const save = {
      category: hashOf(0xfeed),
      nodeId: 1,
      outputs: 2,
      transactionId: 1,
    };
    const commit = await log.beginCommit({
      kind: 'mempool_batch',
      nodeScope: [1],
    });
    // eslint-disable-next-line functional/no-loop-statement
    for (const step of testSaveSteps(scratch.client, commit, save)) {
      // eslint-disable-next-line no-await-in-loop
      await step();
    }
    await log.markCommitted(commit.seq, testSaveRowCounts(save));
    await publisher.publishWatermark();
    const losses: LeaseLostError[] = [];
    lease.startHeartbeat((error) => {
      losses.push(error);
    });
    // simulate a stall: the clock jumps past the local deadline before the next tick
    skewMs = options.ttlMs;
    const lost = await lease.tick();
    t.is(lost?.reason, 'stalled');
    t.is(losses.length, 1);
    await t.throwsAsync(
      log.beginCommit({ kind: 'mempool_batch', nodeScope: [1] }),
      { instanceOf: LeaseLostError }
    );
    // wait out the real ttl so a racing claimant may also try
    await sleep(options.ttlMs + 100);
    skewMs = 0;
    const rival = new WriterLease(scratch.newClient(), {
      ...options,
      agentId: 'rival',
    });
    const [mine, theirs] = await Promise.allSettled([
      lease.reacquire(),
      rival.acquire(),
    ]);
    const winners = [mine, theirs].filter((r) => r.status === 'fulfilled');
    t.is(winners.length, 1, JSON.stringify([mine.status, theirs.status]));
    const holder = mine.status === 'fulfilled' ? lease : rival;
    t.true(holder.epoch >= 2n);
    // recovery under the new epoch: a new CommitLog fences every older epoch
    const recovered = new CommitLog(scratch.client, holder);
    const recovery = await recovered.init();
    t.true(
      recovery.fences.some(
        (fence) => fence.epoch === 1n && fence.maxValidSeq === commit.seq
      )
    );
    const snapshot = await readSnapshot(scratch.client, 1);
    t.is(snapshot.fence[0], counterOfSeq(commit.seq));
    // the committed save of the old epoch is still visible; nothing of it is torn
    t.is(
      nodeFactsVisibility(await visibleOfSave(scratch.client, save), save),
      'all'
    );
    await lease.release();
    await rival.release();
  }
);

e2e(
  '[e2e] WriterLease: acquire, refuse a second writer, renew, release',
  async (t) => {
    const scratch = await createScratchDatabase('lease');
    t.teardown(scratch.drop);
    const options = { safetyMarginMs: 200, settleMs: 150, ttlMs: 2000 };
    const first = new WriterLease(scratch.client, { ...options, agentId: 'a' });
    const second = new WriterLease(scratch.newClient(), {
      ...options,
      agentId: 'b',
    });
    t.is(await first.acquire(), 1n);
    t.is(first.epoch, 1n);
    t.notThrows(() => {
      first.assertHeld();
    });
    await t.throwsAsync(second.acquire(), { instanceOf: LeaseHeldError });
    await first.renew();
    t.true(first.isHeld);
    await first.release();
    t.false(first.isHeld);
    // released: the next writer need not wait for the ttl
    t.is(await second.acquire(), 2n);
    await t.throwsAsync(first.renew(), { instanceOf: LeaseLostError });
    await second.release();
  }
);

e2e(
  '[e2e] WriterLease: a stale holder is taken over after expiry and fenced',
  async (t) => {
    const scratch = await createScratchDatabase('lease_fence');
    t.teardown(scratch.drop);
    const staleClient = scratch.newClient();
    const options = { safetyMarginMs: 300, settleMs: 150, ttlMs: 1500 };
    const stale = new WriterLease(staleClient, {
      ...options,
      agentId: 'stale',
    });
    await stale.acquire();
    const staleLog = new CommitLog(staleClient, stale);
    await staleLog.init();
    const stalePublisher = new VisibilityPublisher(staleClient, staleLog);
    await stalePublisher.init();
    stalePublisher.registerNode(1);
    const category = hashOf(0xbeef);

    // a commit made while the lease is valid stays visible
    const goodSave = { category, nodeId: 1, outputs: 2, transactionId: 1 };
    const good = await staleLog.beginCommit({
      kind: 'mempool_batch',
      nodeScope: [1],
    });
    // eslint-disable-next-line functional/no-loop-statement
    for (const step of testSaveSteps(staleClient, good, goodSave)) {
      // eslint-disable-next-line no-await-in-loop
      await step();
    }
    await staleLog.markCommitted(good.seq, testSaveRowCounts(goodSave));
    await stalePublisher.publishWatermark();

    // the stale writer pauses past its ttl (no heartbeat)
    await sleep(options.ttlMs + 100);
    t.throws(
      () => {
        stale.assertHeld();
      },
      { instanceOf: LeaseLostError }
    );
    await t.throwsAsync(
      staleLog.beginCommit({ kind: 'mempool_batch', nodeScope: [1] }),
      { instanceOf: LeaseLostError }
    );

    const fresh = new WriterLease(scratch.client, {
      ...options,
      agentId: 'fresh',
    });
    t.is(await fresh.acquire(), 2n);
    const freshLog = new CommitLog(scratch.client, fresh);
    const recovery = await freshLog.init();
    t.deepEqual(recovery.fences, [{ epoch: 1n, maxValidSeq: good.seq }]);
    const freshPublisher = new VisibilityPublisher(scratch.client, freshLog);
    await freshPublisher.init();
    await t.throwsAsync(stale.renew(), { instanceOf: LeaseLostError });

    /*
     * a stale writer that ignores its lease entirely: it writes a full commit in
     * its own epoch (seq above the fence) and publishes a watermark covering it
     */
    const staleSave = { category, nodeId: 1, outputs: 3, transactionId: 2 };
    const rogue = new CommitLog(staleClient, {
      assertHeld: () => undefined,
      epoch: 1n,
    });
    const rogueRecovery = await rogue
      .recoverIncomplete(1n)
      .catch((error: unknown) => error);
    t.true(
      rogueRecovery instanceof Error,
      'a stale epoch cannot even initialise'
    );
    // so it bypasses init and writes raw rows with the next seq of its epoch
    const rogueSeq = good.seq + 1n;
    const rogueCommit = {
      epoch: 1n,
      kind: 'mempool_batch' as const,
      nodeScope: [1],
      seq: rogueSeq,
      token: (table: string, chunk: number | string = 0) =>
        `${rogueSeq}:${table}:${chunk}`,
    };
    await staleClient.command(
      `INSERT INTO commit_log (commit_seq, state, node_scope, kind, block_hash, row_counts, writer_epoch, started_at)
     SELECT {seq:UInt64}, 'intent', [1], 'mempool_batch', toFixedString('', 32), map(), 1, now64(3)`,
      { seq: rogueSeq }
    );
    // eslint-disable-next-line functional/no-loop-statement
    for (const step of testSaveSteps(staleClient, rogueCommit, staleSave)) {
      // eslint-disable-next-line no-await-in-loop
      await step();
    }
    await staleClient.command(
      `INSERT INTO commit_log (commit_seq, state, node_scope, kind, block_hash, row_counts, writer_epoch, started_at)
     SELECT {seq:UInt64}, 'committed', [1], 'mempool_batch', toFixedString('', 32), map(), 1, now64(3)`,
      { seq: rogueSeq }
    );
    await staleClient.command(
      'INSERT INTO visibility (node_internal_id, visible_seq, updated_at) SELECT 1, {seq:UInt64}, now64(3)',
      { seq: rogueSeq }
    );

    // the fresh writer commits normally
    const freshSave = { category, nodeId: 1, outputs: 1, transactionId: 3 };
    const freshCommit = await freshLog.beginCommit({
      kind: 'mempool_batch',
      nodeScope: [1],
    });
    // eslint-disable-next-line functional/no-loop-statement
    for (const step of testSaveSteps(scratch.client, freshCommit, freshSave)) {
      // eslint-disable-next-line no-await-in-loop
      await step();
    }
    await freshLog.markCommitted(freshCommit.seq, testSaveRowCounts(freshSave));
    await freshPublisher.publishWatermark();

    t.is(
      nodeFactsVisibility(
        await visibleOfSave(scratch.client, goodSave),
        goodSave
      ),
      'all'
    );
    const rogueSeen = await visibleOfSave(scratch.client, staleSave);
    t.deepEqual(rogueSeen, {
      output: 0,
      tx_acceptance: 0,
      utxo: 0,
      utxo_by_script: 0,
    });
    t.is(
      nodeFactsVisibility(
        await visibleOfSave(scratch.client, freshSave),
        freshSave
      ),
      'all'
    );
    await fresh.release();
    await staleClient.close();
  }
);

e2e(
  '[e2e] WriterLease: of two simultaneous claimants exactly one wins',
  async (t) => {
    const scratch = await createScratchDatabase('lease_race');
    t.teardown(scratch.drop);
    const options = { safetyMarginMs: 200, settleMs: 300, ttlMs: 3000 };
    const leases = ['x', 'y', 'z'].map(
      (agentId) => new WriterLease(scratch.newClient(), { ...options, agentId })
    );
    const results = await Promise.allSettled(
      leases.map(async (lease) => lease.acquire())
    );
    const winners = results.filter((result) => result.status === 'fulfilled');
    t.is(winners.length, 1, JSON.stringify(results.map((r) => r.status)));
    results
      .filter(
        (result): result is PromiseRejectedResult =>
          result.status === 'rejected'
      )
      .forEach((result) => {
        t.true(
          result.reason instanceof LeaseLostError ||
            result.reason instanceof LeaseHeldError
        );
      });
    await Promise.all(leases.map(async (lease) => lease.release()));
  }
);
