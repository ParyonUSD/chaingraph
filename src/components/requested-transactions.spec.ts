/* eslint-disable @typescript-eslint/no-magic-numbers, max-params */
import test from 'ava';

import type { ChaingraphTransaction } from '../types/chaingraph.js';

import { RequestedTransactions } from './requested-transactions.js';

const tx = (hash: string, spends: string[]): ChaingraphTransaction => ({
  hash,
  inputs: spends.map((outpointTransactionHash) => ({
    outpointIndex: 0,
    outpointTransactionHash,
    sequenceNumber: 0,
    unlockingBytecode: '',
  })),
  isCoinbase: false,
  locktime: 0,
  outputs: [],
  sizeBytes: 60,
  version: 2,
});

/**
 * Simulate the agent: each received transaction waits for its requested
 * parents, is "saved" (recorded) and settled.
 */
const receive = async (
  requested: RequestedTransactions,
  saved: string[],
  node: string,
  transaction: ChaingraphTransaction
) => {
  await requested.awaitParents(node, transaction);
  saved.push(transaction.hash);
  requested.settle(node, transaction.hash);
};

test('a chain announced and received child first is saved parents first', async (t) => {
  const requested = new RequestedTransactions(10_000);
  const a = tx('aa', ['00']);
  const b = tx('bb', ['aa']);
  const c = tx('cc', ['bb', 'aa']);
  ['cc', 'bb', 'aa'].forEach((hash) => {
    t.true(requested.request('node1', hash));
  });
  t.false(requested.request('node1', 'cc'), 'one request per node and hash');
  const saved: string[] = [];
  await Promise.all([
    receive(requested, saved, 'node1', c),
    receive(requested, saved, 'node1', b),
    receive(requested, saved, 'node1', a),
  ]);
  t.deepEqual(saved, ['aa', 'bb', 'cc']);
  t.is(requested.pendingCount('node1'), 0);
});

test('requests are per node: another node’s pending parent does not hold a transaction', async (t) => {
  const requested = new RequestedTransactions(10_000);
  requested.request('node2', 'aa');
  const saved: string[] = [];
  await receive(requested, saved, 'node1', tx('bb', ['aa']));
  t.deepEqual(saved, ['bb']);
  requested.settleNode('node2');
  t.is(requested.pendingCount('node2'), 0);
});

test('a parent that never comes stops holding its child after the timeout, on notfound or a disconnect', async (t) => {
  const timedOut = new RequestedTransactions(50);
  timedOut.request('node1', 'aa');
  const started = Date.now();
  // the request timers are unref'd: keep the test process alive meanwhile
  const keepAlive = setTimeout(() => undefined, 1_000);
  t.deepEqual(await timedOut.awaitParents('node1', tx('bb', ['aa'])), ['aa']);
  clearTimeout(keepAlive);
  t.true(Date.now() - started >= 40);

  const settled = new RequestedTransactions(10_000);
  settled.request('node1', 'aa');
  settled.request('node1', 'dd');
  const waiting = settled.awaitParents('node1', tx('bb', ['aa', 'dd']));
  settled.settle('node1', 'aa');
  settled.settleNode('node1');
  t.deepEqual(await waiting, ['aa', 'dd']);
});
