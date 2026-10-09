/* eslint-disable @typescript-eslint/no-magic-numbers */
// cspell:ignore clickhouse
import test from 'ava';

import {
  AbandonedError,
  AbandonSignal,
  awaitDependencies,
  DependencyFailedError,
  InFlightLimiter,
  NodeRegistry,
  OperationRegistry,
  SaveSlot,
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

const tick = async () =>
  new Promise((resolve) => {
    setImmediate(resolve);
  });

test('InFlightLimiter: cap held, FIFO for new tickets, an older ticket goes first', async (t) => {
  t.throws(() => new InFlightLimiter(0), { instanceOf: RangeError });
  const limiter = new InFlightLimiter(2);
  const granted: number[] = [];
  const take = async (ticket: number) =>
    limiter.acquire(ticket).then(() => granted.push(ticket));
  await take(1);
  await take(2);
  const queued = [take(4), take(5), take(3)];
  await tick();
  t.deepEqual(granted, [1, 2]);
  t.is(limiter.active, 2);
  t.is(limiter.waiting, 3);
  limiter.release();
  await tick();
  t.deepEqual(
    granted,
    [1, 2, 3],
    'the oldest ticket (a resumed operation) first'
  );
  limiter.release();
  limiter.release();
  await Promise.all(queued);
  t.deepEqual(granted, [1, 2, 3, 4, 5]);
  t.is(limiter.active, 2);
  t.is(limiter.waiting, 0);
  limiter.release();
  limiter.release();
  t.is(limiter.active, 0);
});

test('InFlightLimiter: abandon leaves the queue; a held slot is not leaked', async (t) => {
  const limiter = new InFlightLimiter(1);
  const abandon = new AbandonSignal();
  await limiter.acquire(1, abandon);
  const waiting = limiter.acquire(2, abandon);
  await tick();
  t.is(limiter.waiting, 1);
  abandon.abandon('test');
  await t.throwsAsync(waiting, { instanceOf: AbandonedError });
  t.is(limiter.waiting, 0);
  t.is(limiter.active, 1);
  limiter.release();
  t.is(limiter.active, 0);
  await t.throwsAsync(limiter.acquire(3, abandon), {
    instanceOf: AbandonedError,
  });
  t.is(limiter.active, 0);
});

test('SaveSlot: a holder waiting on a queued operation gives its slot up (cap 1: no deadlock)', async (t) => {
  const limiter = new InFlightLimiter(1);
  const registry = new OperationRegistry();
  const child = registry.begin('block', [1]);
  const parent = registry.begin('block', [1]);
  child.slot = new SaveSlot(limiter, child.id);
  parent.slot = new SaveSlot(limiter, parent.id);
  await child.slot.acquire();
  const parentDone = parent.slot.acquire().then(() => {
    parent.markCommitted();
    parent.slot?.release();
  });
  await tick();
  t.is(limiter.waiting, 1, 'the parent is queued behind the child');
  // the child waits for the parent's commit: without giving up its slot this never resolves
  await child.whileWaiting(parent.committed);
  t.true(child.slot.isHeld, 'the child holds its slot again after the wait');
  await parentDone;
  child.slot.release();
  t.is(limiter.active, 0);
  t.is(limiter.waiting, 0);
});

test('SaveSlot: no slot (unbounded store) leaves waits unchanged', async (t) => {
  const registry = new OperationRegistry();
  const a = registry.begin('block', [1]);
  const b = registry.begin('block', [1]);
  const waiting = waitForPredecessorRows(b);
  a.markRowsWritten();
  await waiting;
  t.is(b.slot, undefined);
  t.is(await b.whileWaiting(Promise.resolve(7)), 7);
});
