/* eslint-disable @typescript-eslint/no-magic-numbers, camelcase, @typescript-eslint/naming-convention, complexity */
// cspell:ignore randomised seqs unhex
import test from 'ava';

import type { ClickHouseClient } from './client.js';
import type { CommitLease } from './commit-log.js';
import { CommitLog, seqForEpoch } from './commit-log.js';
import type { ScratchDatabase, TestSave } from './test-support.js';
import {
  createScratchDatabase,
  e2eClickHouseUrl,
  hashOf,
  nodeFactsVisibility,
  testSaveRowCounts,
  testSaveSteps,
  visibleOfSave,
} from './test-support.js';
import {
  agnosticViewParams,
  computeWatermarks,
  nodeViewParams,
  readSnapshot,
  readWatermark,
  VisibilityPublisher,
} from './visibility.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const lease = (epoch: bigint): CommitLease => ({
  assertHeld: () => undefined,
  epoch,
});

test('computeWatermarks: a node waits only for its own open commits', (t) => {
  const watermarks = computeWatermarks({
    lastAllocatedSeq: 20n,
    nodes: [1, 2, 3],
    openCommits: [
      { nodeScope: [1], seq: 12n },
      { nodeScope: [1, 2], seq: 15n },
      { nodeScope: [], seq: 11n },
    ],
  });
  t.deepEqual(
    [...watermarks].sort(([a], [b]) => a - b),
    [
      [0, 10n],
      [1, 11n],
      [2, 14n],
      [3, 20n],
    ]
  );
});

test('computeWatermarks: no open commits means everything allocated is visible', (t) => {
  t.deepEqual(
    [
      ...computeWatermarks({
        lastAllocatedSeq: 7n,
        nodes: [0, 5],
        openCommits: [],
      }),
    ],
    [
      [0, 7n],
      [5, 7n],
    ]
  );
});

test('computeWatermarks: watermarks never pass an open commit (randomised)', (t) => {
  // eslint-disable-next-line functional/no-let
  let state = 12345;
  const random = () => {
    state = (state * 1103515245 + 12345) % 2147483648;
    return state / 2147483648;
  };
  Array.from({ length: 200 }).forEach(() => {
    const last = BigInt(1 + Math.floor(random() * 50));
    const openCommits = Array.from(
      { length: Math.floor(random() * 6) },
      () => ({
        nodeScope: [1, 2, 3].filter(() => random() < 0.5),
        seq: 1n + BigInt(Math.floor(random() * Number(last))),
      })
    );
    const watermarks = computeWatermarks({
      lastAllocatedSeq: last,
      nodes: [1, 2, 3],
      openCommits,
    });
    openCommits.forEach(({ nodeScope, seq }) => {
      t.true(watermarks.get(0)! < seq);
      nodeScope.forEach((node) => {
        t.true(watermarks.get(node)! < seq);
      });
    });
    watermarks.forEach((visible) => {
      t.true(visible <= last);
    });
  });
});

test('VisibilityPublisher: writes only advanced watermarks', async (t) => {
  const commands: { [key: string]: unknown }[] = [];
  const client = {
    command: async (_sql: string, params?: { [key: string]: unknown }) => {
      commands.push(params ?? {});
    },
    query: async () => [{ node: 1, visible: '3' }] as never[],
  };
  const source = {
    lastAllocatedSeq: 5n,
    onTerminal: () => undefined,
    openCommits: () => [{ nodeScope: [2], seq: 4n }],
  };
  const publisher = new VisibilityPublisher(client, source);
  await publisher.init();
  publisher.registerNode(3);
  const first = await publisher.publishWatermark();
  t.deepEqual(first.get(1), 5n);
  t.deepEqual(first.get(2), 3n);
  t.deepEqual(commands, [{ nodes: [0, 1, 2, 3], seqs: [3n, 5n, 3n, 5n] }]);
  await publisher.publishWatermark();
  t.is(commands.length, 1);
  t.is(publisher.publishedWatermark(1), 5n);
});

/** A writer: commit log + publisher on one client, with a fixed epoch. */
const writer = async (client: ClickHouseClient, epoch: bigint) => {
  const log = new CommitLog(client, lease(epoch));
  const recovery = await log.init();
  const publisher = new VisibilityPublisher(client, log);
  await publisher.init();
  publisher.registerNode(1);
  publisher.registerNode(2);
  await publisher.publishWatermark();
  return { log, publisher, recovery };
};

const runSave = async (
  client: ClickHouseClient,
  log: CommitLog,
  save: TestSave
) => {
  const commit = await log.beginCommit({
    kind: 'mempool_batch',
    nodeScope: [save.nodeId],
  });
  // eslint-disable-next-line functional/no-loop-statement
  for (const step of testSaveSteps(client, commit, save)) {
    // eslint-disable-next-line no-await-in-loop
    await step();
  }
  return commit;
};

const withScratch = async (
  label: string,
  t: { teardown: (fn: () => Promise<void>) => void }
): Promise<ScratchDatabase> => {
  const scratch = await createScratchDatabase(label);
  t.teardown(scratch.drop);
  return scratch;
};

const category = hashOf(0xcafe);

e2e(
  '[e2e] watermark advances only when all earlier commits for the node are terminal',
  async (t) => {
    const { client } = await withScratch('watermark', t);
    const { log, publisher } = await writer(client, 1n);
    const slow = await log.beginCommit({ kind: 'block', nodeScope: [1] });
    const fastSameNode = await log.beginCommit({
      kind: 'block',
      nodeScope: [1],
    });
    const otherNode = await log.beginCommit({ kind: 'block', nodeScope: [2] });
    await log.markCommitted(fastSameNode.seq, {});
    await log.markCommitted(otherNode.seq, {});
    await publisher.publishWatermark();
    t.is(await readWatermark(client, 1), slow.seq - 1n);
    t.is(await readWatermark(client, 2), otherNode.seq);
    t.is(await readWatermark(client, 0), slow.seq - 1n);
    await log.markAborted(slow.seq, 'test');
    await publisher.publishWatermark();
    t.is(await readWatermark(client, 1), otherNode.seq);
    t.is(await readWatermark(client, 0), otherNode.seq);
    // a node with no commits follows the highest allocated seq
    t.is(await readWatermark(client, 3), 0n);
    publisher.registerNode(3);
    await publisher.publishWatermark();
    t.is(await readWatermark(client, 3), otherNode.seq);
  }
);

e2e(
  '[e2e] aborted rows stay invisible through the views after the watermark passes them',
  async (t) => {
    const { client } = await withScratch('aborted', t);
    const { log, publisher } = await writer(client, 1n);
    const abortedSave = { category, nodeId: 1, outputs: 2, transactionId: 10 };
    const keptSave = { category, nodeId: 1, outputs: 3, transactionId: 11 };
    const aborted = await runSave(client, log, abortedSave);
    await log.markAborted(aborted.seq, 'test');
    const kept = await runSave(client, log, keptSave);
    await log.markCommitted(kept.seq, testSaveRowCounts(keptSave));
    await publisher.publishWatermark();
    t.true((await readWatermark(client, 1)) >= kept.seq);
    t.deepEqual(await visibleOfSave(client, abortedSave), {
      output: 0,
      tx_acceptance: 0,
      utxo: 0,
      utxo_by_script: 0,
    });
    t.deepEqual(await visibleOfSave(client, keptSave), {
      output: 3,
      tx_acceptance: 1,
      utxo: 3,
      utxo_by_script: 3,
    });
    // the base table still has the aborted rows (GC is a later mutation)
    const raw = await client.query<{ c: string }>(
      'SELECT count() AS c FROM utxo WHERE commit_seq = {seq:UInt64}',
      { seq: aborted.seq }
    );
    t.is(raw[0]!.c, '2');
  }
);

e2e(
  '[e2e] torn reads: a pinned watermark shows a two-table save all-or-none',
  async (t) => {
    const { client } = await withScratch('torn', t);
    const { log, publisher } = await writer(client, 1n);
    const save = { category, nodeId: 1, outputs: 2, transactionId: 20 };
    const pinnedRead = async (
      snapshot: Awaited<ReturnType<typeof readSnapshot>>
    ) => {
      const node = nodeViewParams(snapshot);
      const agnostic = agnosticViewParams(snapshot);
      // one query joining two node views and a node-agnostic view, one watermark
      const rows = await client.query<{
        acceptance: string;
        utxo: string;
        outputs: string;
      }>(
        `SELECT
         (SELECT count() FROM tx_acceptance_at(node = {node:UInt32}, visible = {visible:UInt64})
           WHERE transaction_internal_id = {tx:UInt64}) AS acceptance,
         (SELECT count() FROM utxo_at(node = {node:UInt32}, visible = {visible:UInt64}) AS u
           INNER JOIN tx_acceptance_at(node = {node:UInt32}, visible = {visible:UInt64}) AS a
           ON u.transaction_hash = a.transaction_hash
           WHERE u.transaction_internal_id = {tx:UInt64}) AS utxo,
         (SELECT count() FROM output_at(visible0 = {visible0:UInt64}, tail = {tail:Array(UInt64)})
           WHERE transaction_internal_id = {tx:UInt64}) AS outputs`,
        { ...node, ...agnostic, tx: save.transactionId }
      );
      return rows[0]!;
    };
    const none = { acceptance: '0', outputs: '0', utxo: '0' };
    const all = { acceptance: '1', outputs: '2', utxo: '2' };

    const before = await readSnapshot(client, 1);
    const commit = await log.beginCommit({
      kind: 'mempool_batch',
      nodeScope: [1],
    });
    const steps = testSaveSteps(client, commit, save);
    await steps[0]!();
    await steps[1]!();
    // between the data inserts and `committed`: nothing, from old and fresh snapshots
    t.deepEqual(await pinnedRead(before), none);
    t.deepEqual(await pinnedRead(await readSnapshot(client, 1)), none);
    await steps[2]!();
    t.deepEqual(await pinnedRead(await readSnapshot(client, 1)), none);
    await log.markCommitted(commit.seq, testSaveRowCounts(save));
    // committed but not published: node facts hidden; the node-agnostic rows may show (committed tail)
    const committedOnly = await pinnedRead(await readSnapshot(client, 1));
    t.is(committedOnly.acceptance, '0');
    t.is(committedOnly.utxo, '0');
    await publisher.publishWatermark();
    const after = await readSnapshot(client, 1);
    t.true(after.visible >= commit.seq);
    t.deepEqual(await pinnedRead(after), all);
    // the old snapshot stays pinned: still nothing (a forged larger W is clamped, never needed)
    t.deepEqual(await pinnedRead(before), none);
    t.deepEqual(await pinnedRead({ ...before, visible: 2n ** 64n - 1n }), {
      ...none,
      acceptance: '1',
      utxo: '2',
    });
  }
);

e2e(
  '[e2e] crash injection: a reader of node n sees all of a save or none, before and after recovery',
  async (t) => {
    const { client, newClient } = await withScratch('crash', t);
    // protocol steps: 0 intent, 1-3 data steps, 4 committed, 5 publish
    const stepCount = 6;
    // eslint-disable-next-line functional/no-loop-statement, functional/no-let
    for (let crashAfter = -1; crashAfter < stepCount; crashAfter += 1) {
      // epochs only grow: round r writes with 2r+3 and restarts with 2r+4
      const epoch = BigInt(2 * crashAfter + 5);
      const writerClient = newClient();
      // eslint-disable-next-line no-await-in-loop
      const { log, publisher } = await writer(writerClient, epoch);
      const save = {
        category,
        nodeId: 1,
        outputs: 2,
        transactionId: 100 + crashAfter + 1,
      };
      const run = async () => {
        if (crashAfter < 0) {
          return undefined;
        }
        const commit = await log.beginCommit({
          kind: 'mempool_batch',
          nodeScope: [1],
        });
        const steps = testSaveSteps(writerClient, commit, save);
        // eslint-disable-next-line functional/no-loop-statement, functional/no-let
        for (let step = 1; step <= 3 && step <= crashAfter; step += 1) {
          // eslint-disable-next-line no-await-in-loop
          await steps[step - 1]!();
        }
        if (crashAfter >= 4) {
          await log.markCommitted(commit.seq, testSaveRowCounts(save));
        }
        if (crashAfter >= 5) {
          await publisher.publishWatermark();
        }
        return commit;
      };
      // eslint-disable-next-line no-await-in-loop
      const commit = await run();
      // the writer dies here; a reader looks before any recovery
      // eslint-disable-next-line no-await-in-loop
      const seenBefore = await visibleOfSave(client, save);
      t.is(
        nodeFactsVisibility(seenBefore, save),
        crashAfter >= 5 ? 'all' : 'none',
        `crash after step ${crashAfter}, before recovery`
      );
      // eslint-disable-next-line no-await-in-loop
      await writerClient.close();
      // restart: a new epoch recovers and publishes
      // eslint-disable-next-line no-await-in-loop
      const restarted = await writer(client, epoch + 1n);
      // eslint-disable-next-line no-await-in-loop
      const seenAfter = await visibleOfSave(client, save);
      const expectedAfter = crashAfter >= 4 ? 'all' : 'none';
      t.is(
        nodeFactsVisibility(seenAfter, save),
        expectedAfter,
        `crash after step ${crashAfter}, after recovery`
      );
      t.is(
        seenAfter.output,
        expectedAfter === 'all' ? save.outputs : 0,
        `crash after step ${crashAfter}: node-agnostic rows follow the commit`
      );
      if (commit !== undefined) {
        t.is(
          restarted.recovery.aborted.some(
            (record) => record.seq === commit.seq
          ),
          crashAfter < 4,
          `crash after step ${crashAfter}: recovery aborts exactly the unfinished commit`
        );
      }
    }
  }
);

e2e(
  '[e2e] pinned views keep primary-key and projection use (EXPLAIN)',
  async (t) => {
    const { client } = await withScratch('explain', t);
    const { log, publisher } = await writer(client, 1n);
    const commit = await log.beginCommit({ kind: 'block', nodeScope: [1] });
    await client.command(
      `INSERT INTO output (transaction_hash, output_index, transaction_internal_id, value_satoshis,
       locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability,
       nonfungible_token_commitment, commit_seq)
     SELECT toFixedString(unhex(leftPad(hex(number), 64, '0')), 32), 0, number, 1,
       concat('script', toString(number)), toFixedString(unhex(repeat('00', 32)), 32), NULL, NULL, NULL,
       {seq:UInt64}
     FROM numbers(100000)`,
      { seq: commit.seq },
      { insert_deduplication_token: commit.token('output') }
    );
    await client.command(
      `INSERT INTO utxo (node_internal_id, token_category, transaction_hash, output_index,
       transaction_internal_id, created_height, value_satoshis, locking_bytecode, fungible_token_amount,
       nonfungible_token_capability, nonfungible_token_commitment, sign, version, commit_seq)
     SELECT 1 + number % 2, toFixedString(unhex(leftPad(hex(number % 500), 64, '0')), 32),
       toFixedString(unhex(leftPad(hex(number), 64, '0')), 32), 0, number, 0, 1, 'abc', NULL, NULL, NULL,
       1, 1, {seq:UInt64}
     FROM numbers(100000)`,
      { seq: commit.seq },
      { insert_deduplication_token: commit.token('utxo') }
    );
    await log.markCommitted(commit.seq, { output: 100_000, utxo: 100_000 });
    await publisher.publishWatermark();
    const snapshot = await readSnapshot(client, 1);
    const explain = async (sql: string, params: { [key: string]: unknown }) =>
      (
        await client.query<{ explain: string }>(
          `EXPLAIN projections = 1, indexes = 1 ${sql}`,
          params
        )
      )
        .map((row) => row.explain)
        .join('\n');

    // node 1 holds the even numbers; category 8 has 200 of them
    const category8 = `${'0'.repeat(63)}8`;
    const utxoPlan = await explain(
      'SELECT * FROM utxo_at(node = {node:UInt32}, visible = {visible:UInt64}) WHERE token_category = unhex({category:String})',
      { ...nodeViewParams(snapshot), category: category8 }
    );
    t.regex(utxoPlan, /token_category/u);
    const utxoGranules = /Granules: (?<read>\d+)\/(?<total>\d+)/u.exec(
      utxoPlan.slice(utxoPlan.indexOf('PrimaryKey'))
    );
    const granulesRead = utxoGranules?.groups?.read ?? '';
    const granulesTotal = utxoGranules?.groups?.total ?? '';
    t.true(granulesRead !== '' && Number(granulesRead) <= 2, utxoPlan);
    const outputPlan = await explain(
      'SELECT * FROM output_at(visible0 = {visible0:UInt64}, tail = {tail:Array(UInt64)}) WHERE locking_bytecode_prefix = {prefix:String}',
      { ...agnosticViewParams(snapshot), prefix: 'script1234' }
    );
    t.regex(outputPlan, /ReadFromMergeTree \(p_script\)/u);
    // and the answers are right, within a row budget only an index allows
    const rows = await client.query<{ c: string }>(
      `SELECT
       (SELECT count() FROM utxo_at(node = {node:UInt32}, visible = {visible:UInt64})
         WHERE token_category = unhex({category:String})) AS c`,
      { ...nodeViewParams(snapshot), category: category8 },
      { max_rows_to_read: '20000' }
    );
    t.is(rows[0]!.c, '200');
    t.log(
      `utxo_at PK granules ${granulesRead}/${granulesTotal}; output_at uses p_script`
    );
    t.is(snapshot.visible, seqForEpoch(1n, 1n));
  }
);
