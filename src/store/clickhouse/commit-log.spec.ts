/* eslint-disable @typescript-eslint/no-magic-numbers, no-bitwise */
// cspell:ignore dedup seqs
import { readFileSync } from 'node:fs';

import test from 'ava';

import type { CommitLease, CommitLogClient } from './commit-log.js';
import {
  CommitDependencyError,
  commitKinds,
  CommitLog,
  CommitStateError,
  counterOfSeq,
  dedupToken,
  epochOfSeq,
  lastSeqOfEpoch,
  seqForEpoch,
} from './commit-log.js';
import { createScratchDatabase, e2eClickHouseUrl } from './test-support.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const fixedLease = (epoch: bigint, held = { value: true }): CommitLease => ({
  assertHeld: () => {
    if (!held.value) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error('lease lost');
    }
  },
  epoch,
});

interface RecordedInsert {
  sql: string;
  params: { [key: string]: unknown };
  token: string;
}

const stubClient = () => {
  const inserts: RecordedInsert[] = [];
  const client: CommitLogClient = {
    insertSelect: async (sql, params, options) => {
      inserts.push({ params, sql, token: options.deduplicationToken });
    },
    query: async () => [] as never[],
  };
  return { client, inserts };
};

test('seq layout: epoch in the high bits, counter >= 1', (t) => {
  const seq = seqForEpoch(3n, 5n);
  t.is(seq, (3n << 40n) | 5n);
  t.is(epochOfSeq(seq), 3n);
  t.is(counterOfSeq(seq), 5n);
  t.true(seqForEpoch(4n, 1n) > lastSeqOfEpoch(3n));
  t.is(lastSeqOfEpoch(3n) + 1n, 4n << 40n);
  t.throws(() => seqForEpoch(0n, 1n));
  t.throws(() => seqForEpoch(1n, 0n));
  t.throws(() => seqForEpoch(1n, 1n << 40n));
});

test('commitKinds: equals the commit_log.kind enum of the CREATE and the ALTER', (t) => {
  const ddl = readFileSync(
    new URL(
      '../../../src/store/clickhouse/ddl/040_bookkeeping.sql',
      import.meta.url
    ),
    'utf8'
  );
  const byText = (left: string, right: string) => left.localeCompare(right);
  const enums = [...ddl.matchAll(/\bkind\s+Enum8\((?<values>[^)]*)\)/gu)].map(
    (match) =>
      [
        ...(match.groups?.values ?? '').matchAll(
          /'(?<name>\w+)'\s*=\s*(?<value>\d+)/gu
        ),
      ].map(
        (entry) => `${entry.groups?.name ?? ''}=${entry.groups?.value ?? ''}`
      )
  );
  t.is(enums.length, 2);
  t.deepEqual(enums[1], enums[0]);
  t.deepEqual(
    (enums[0] ?? []).map((entry) => entry.split('=')[0] ?? '').sort(byText),
    [...commitKinds].sort(byText)
  );
  t.true(enums[0]?.includes('backfill=9'));
  t.true(enums[0]?.includes('block=1'));
});

test('dedupToken: seq:table:chunk', (t) => {
  t.is(dedupToken(42n, 'utxo'), '42:utxo:0');
  t.is(dedupToken(42n, 'utxo', 3), '42:utxo:3');
  t.is(dedupToken(42n, 'input', 'f0'), '42:input:f0');
});

test('CommitLog: begin, commit and abort track state in memory', async (t) => {
  const { client, inserts } = stubClient();
  const log = new CommitLog(client, fixedLease(2n), () => 1_000);
  await t.throwsAsync(log.beginCommit({ kind: 'block', nodeScope: [1] }), {
    instanceOf: CommitStateError,
  });
  const recovery = await log.init();
  t.deepEqual(recovery, {
    aborted: [],
    fences: [{ epoch: 1n, maxValidSeq: 1n << 40n }],
    lastSeq: 0n,
  });
  const terminal: bigint[] = [];
  log.onTerminal((seq) => terminal.push(seq));

  const first = await log.beginCommit({ kind: 'block', nodeScope: [1, 2] });
  const second = await log.beginCommit({
    kind: 'mempool_batch',
    nodeScope: [1],
  });
  t.is(first.seq, seqForEpoch(2n, 1n));
  t.is(second.seq, seqForEpoch(2n, 2n));
  t.is(first.token('output', 1), `${first.seq}:output:1`);
  t.is(log.lastAllocatedSeq, second.seq);
  t.deepEqual(
    log.openCommits().map((commit) => commit.seq),
    [first.seq, second.seq]
  );
  t.is(log.stateOf(first.seq), 'intent');

  await log.markIncomplete(second.seq);
  t.is(log.stateOf(second.seq), 'incomplete');
  await log.markCommitted(first.seq, { output: 3, utxo: 2n });
  await log.markAborted(second.seq, 'test');
  t.is(log.stateOf(first.seq), 'committed');
  t.is(log.stateOf(second.seq), 'aborted');
  t.deepEqual(log.openCommits(), []);
  t.deepEqual(terminal, [first.seq, second.seq]);

  const tokens = inserts.map((insert) => insert.token);
  t.deepEqual(tokens, [
    '2:epoch_fence:0',
    `${first.seq}:commit_log:intent`,
    `${second.seq}:commit_log:intent`,
    `${second.seq}:commit_log:incomplete`,
    `${first.seq}:commit_log:committed`,
    // the void row is written before commit_log's aborted row
    `${second.seq}:commit_void:0`,
    `${second.seq}:commit_log:aborted`,
  ]);
  const committedRow = inserts[4]!.params;
  t.deepEqual(committedRow.countKeys, ['output', 'utxo']);
  t.deepEqual(committedRow.countValues, [3n, 2n]);
  t.deepEqual(committedRow.scope, [1, 2]);
  await t.throwsAsync(log.markCommitted(first.seq, {}), {
    instanceOf: CommitStateError,
  });
});

test('CommitLog: markCommitted refuses until dependencies are committed', async (t) => {
  const { client } = stubClient();
  const log = new CommitLog(client, fixedLease(1n));
  await log.init();
  const mempool = await log.beginCommit({
    kind: 'mempool_batch',
    nodeScope: [2],
  });
  const block = await log.beginCommit({
    dependsOn: [mempool.seq],
    kind: 'block',
    nodeScope: [1],
  });
  await t.throwsAsync(log.markCommitted(block.seq, {}), {
    instanceOf: CommitDependencyError,
    message: /intent/u,
  });
  await log.markCommitted(mempool.seq, {});
  await log.markCommitted(block.seq, {});
  t.is(log.stateOf(block.seq), 'committed');

  const other = await log.beginCommit({ kind: 'mempool_batch', nodeScope: [] });
  const dependent = await log.beginCommit({
    dependsOn: [other.seq],
    kind: 'block',
    nodeScope: [1],
  });
  await log.markAborted(other.seq, 'crash');
  await t.throwsAsync(log.markCommitted(dependent.seq, {}), {
    instanceOf: CommitDependencyError,
    message: /aborted/u,
  });
});

test('CommitLog: a lost lease stops new commits and commit marks', async (t) => {
  const { client } = stubClient();
  const held = { value: true };
  const log = new CommitLog(client, fixedLease(1n, held));
  await log.init();
  const commit = await log.beginCommit({ kind: 'block', nodeScope: [1] });
  held.value = false;
  await t.throwsAsync(log.markCommitted(commit.seq, {}), {
    message: /lease lost/u,
  });
  await t.throwsAsync(log.beginCommit({ kind: 'block', nodeScope: [1] }), {
    message: /lease lost/u,
  });
  // aborting is always allowed (it only hides rows)
  await log.markAborted(commit.seq, 'lease lost');
  t.is(log.stateOf(commit.seq), 'aborted');
});

test('CommitLog: staleIncomplete reports incomplete commits past their lifetime', async (t) => {
  const { client } = stubClient();
  // eslint-disable-next-line functional/no-let
  let now = 0;
  const log = new CommitLog(client, fixedLease(1n), () => now);
  await log.init();
  const intent = await log.beginCommit({ kind: 'block', nodeScope: [1] });
  const incomplete = await log.beginCommit({ kind: 'block', nodeScope: [1] });
  await log.markIncomplete(incomplete.seq);
  now = 59_000;
  t.deepEqual(log.staleIncomplete(60_000), []);
  now = 61_000;
  t.deepEqual(log.staleIncomplete(60_000), [incomplete.seq]);
  t.is(log.stateOf(intent.seq), 'intent');
});

e2e(
  '[e2e] CommitLog: commit, abort, recover and list against ClickHouse',
  async (t) => {
    const scratch = await createScratchDatabase('commit_log');
    t.teardown(scratch.drop);
    const { client } = scratch;

    const first = new CommitLog(client, fixedLease(1n));
    t.deepEqual((await first.init()).lastSeq, 0n);
    const committed = await first.beginCommit({
      blockHashHex: 'ab'.repeat(32),
      kind: 'block',
      nodeScope: [1],
    });
    await first.markCommitted(committed.seq, { block: 1, output: 4 });
    const aborted = await first.beginCommit({ kind: 'reorg', nodeScope: [1] });
    await first.markAborted(aborted.seq, 'test abort');
    const leftIntent = await first.beginCommit({
      kind: 'mempool_batch',
      nodeScope: [2],
    });
    const leftIncomplete = await first.beginCommit({
      kind: 'block',
      nodeScope: [1, 2],
    });
    await first.markIncomplete(leftIncomplete.seq);
    // the process "dies" here: no more writes from `first`

    const second = new CommitLog(client, fixedLease(2n));
    const recovery = await second.init();
    t.deepEqual(
      recovery.aborted.map((record) => [record.seq, record.state, record.kind]),
      [
        [leftIntent.seq, 'intent', 'mempool_batch'],
        [leftIncomplete.seq, 'incomplete', 'block'],
      ]
    );
    t.deepEqual(recovery.fences, [
      { epoch: 1n, maxValidSeq: leftIncomplete.seq },
    ]);
    t.is(recovery.lastSeq, leftIncomplete.seq);

    const all = await second.listCommits();
    t.deepEqual(
      all.map((record) => [record.seq, record.state]),
      [
        [committed.seq, 'committed'],
        [aborted.seq, 'aborted'],
        [leftIntent.seq, 'aborted'],
        [leftIncomplete.seq, 'aborted'],
      ]
    );
    const [committedRecord] = all;
    t.is(committedRecord!.blockHashHex, 'ab'.repeat(32));
    t.deepEqual(committedRecord!.rowCounts, { block: 1n, output: 4n });
    t.deepEqual(committedRecord!.nodeScope, [1]);
    t.is(committedRecord!.writerEpoch, 1n);
    t.not(committedRecord!.finishedAt, null);
    t.regex(
      all[2]!.abortReason,
      /recovered at startup by epoch 2 \(was intent\)/u
    );
    t.deepEqual(
      (await second.listCommits({ states: ['aborted'] })).map((r) => r.seq),
      [aborted.seq, leftIntent.seq, leftIncomplete.seq]
    );
    t.deepEqual(
      (
        await second.listCommits({
          fromSeq: aborted.seq,
          limit: 1,
          toSeq: leftIntent.seq,
        })
      ).map((r) => r.seq),
      [aborted.seq]
    );
    const voided = await client.query<{ seq: string }>(
      'SELECT toString(commit_seq) AS seq FROM commit_void ORDER BY commit_seq'
    );
    t.deepEqual(
      voided.map((row) => BigInt(row.seq)),
      [aborted.seq, leftIntent.seq, leftIncomplete.seq]
    );

    // new seqs are in the new epoch, above every earlier seq
    const next = await second.beginCommit({ kind: 'block', nodeScope: [1] });
    t.is(next.seq, seqForEpoch(2n, 1n));
    t.true(next.seq > leftIncomplete.seq);

    // a third start fences epoch 2 (dense) and recovers `next`
    const third = new CommitLog(client, fixedLease(4n));
    const thirdRecovery = await third.init();
    t.deepEqual(
      thirdRecovery.aborted.map((record) => record.seq),
      [next.seq]
    );
    t.deepEqual(thirdRecovery.fences, [
      { epoch: 2n, maxValidSeq: next.seq },
      { epoch: 3n, maxValidSeq: 3n << 40n },
    ]);

    // starting with an epoch at or below one already used is refused
    await t.throwsAsync(new CommitLog(client, fixedLease(2n)).init(), {
      instanceOf: CommitStateError,
    });
  }
);
