/* eslint-disable @typescript-eslint/no-magic-numbers */
// cspell:ignore clickhouse
import test from 'ava';

import {
  awaitDependencies,
  DependencyFailedError,
  NodeRegistry,
  OperationRegistry,
  waitForPredecessorRows,
} from './node-state.js';

test('NodeRegistry: name ↔ id', (t) => {
  const registry = new NodeRegistry();
  registry.set('b', 2);
  registry.set('a', 1);
  t.is(registry.idOf('a'), 1);
  t.is(registry.nameOf(2), 'b');
  t.deepEqual(registry.ids, [1, 2]);
});

test('OperationRegistry: predecessors are earlier live operations sharing a node', (t) => {
  const registry = new OperationRegistry();
  const a = registry.begin('block', [1, 2]);
  const b = registry.begin('block', [3]);
  const c = registry.begin('reorg', [2]);
  const d = registry.begin('header_accept', [3, 1]);
  t.deepEqual(a.predecessors, []);
  t.deepEqual(b.predecessors, []);
  t.deepEqual(c.predecessors, [a]);
  t.deepEqual(d.predecessors, [a, b]);
  t.deepEqual(d.nodes, [1, 3]);
  t.is(registry.activeCount, 4);
});

test('OperationRegistry: rows-written waits, dependency failures and drain', async (t) => {
  const registry = new OperationRegistry();
  const a = registry.begin('block', [1]);
  const b = registry.begin('reorg', [1]);
  const order: string[] = [];
  const waiting = waitForPredecessorRows(b).then(() =>
    order.push('b may read')
  );
  await Promise.resolve();
  order.push('a writes');
  a.seq = 5n;
  a.markRowsWritten();
  await waiting;
  t.deepEqual(order, ['a writes', 'b may read']);
  a.markCommitted();
  t.deepEqual(await awaitDependencies([a]), [5n]);
  b.markFailed(new Error('boom'));
  await t.throwsAsync(awaitDependencies([a, b]), {
    instanceOf: DependencyFailedError,
  });
  const drained = registry.drain();
  registry.end(a);
  registry.end(b);
  await drained;
  t.is(registry.activeCount, 0);
});
