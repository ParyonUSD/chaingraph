/* eslint-disable @typescript-eslint/no-magic-numbers */
// cspell:ignore clickhouse
import { setFlagsFromString } from 'node:v8';
import { runInNewContext } from 'node:vm';

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

const tick = async () =>
  new Promise((resolve) => {
    setImmediate(resolve);
  });

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
  t.false(registry.hasUnsettledEarlier(a));
  t.false(registry.hasUnsettledEarlier(b));
  t.true(registry.hasUnsettledEarlier(c));
  t.true(registry.hasUnsettledEarlier(d));
  // one barrier dependency stands for all of them; it never yields a dependsOn seq
  t.is(c.predecessors.length, 1);
  t.is(c.predecessors[0]!.state, 'done');
  t.deepEqual(d.nodes, [1, 3]);
  t.is(registry.activeCount, 4);
});

const settledState = async (promise: Promise<unknown>) => {
  const state = { value: 'pending' };
  promise.then(
    () => {
      state.value = 'resolved';
    },
    () => {
      state.value = 'rejected';
    }
  );
  await tick();
  return state.value;
};

test('OperationRegistry: an operation waits for every earlier operation of its nodes, not only the last (WP6b item 3)', async (t) => {
  const registry = new OperationRegistry();
  const q = registry.begin('block', [1]);
  const p = registry.begin('block', [1]);
  const other = registry.begin('block', [2]);
  const r = registry.begin('header_accept', [1, 2]);
  // a bulk-mode save (p) can write and commit before an earlier one (q)
  p.markCommitted();
  other.markCommitted();
  registry.end(p);
  registry.end(other);
  const rows = waitForPredecessorRows(r);
  const settled = awaitDependencies(r.predecessors);
  t.is(await settledState(rows), 'pending', 'q has not written its rows');
  q.markRowsWritten();
  t.is(await settledState(rows), 'resolved');
  t.is(await settledState(settled), 'pending', 'q has not committed');
  q.markCommitted();
  t.is(await settledState(settled), 'resolved');
  // no predecessor left: nothing to wait for
  t.deepEqual(r.predecessors, []);
});

test('OperationRegistry: a failure fails the operations that had it as a predecessor, not later ones', async (t) => {
  const registry = new OperationRegistry();
  const a = registry.begin('block', [1]);
  const b = registry.begin('reorg', [1]);
  const unrelated = registry.begin('reorg', [2]);
  const bSettled = awaitDependencies(b.predecessors);
  a.markFailed(new Error('boom'));
  registry.end(a);
  await t.throwsAsync(bSettled, { instanceOf: DependencyFailedError });
  t.not(b.poisoned, undefined);
  t.is(unrelated.poisoned, undefined);
  // registered after the failure: a was not live, so it is not a predecessor
  const c = registry.begin('reorg', [1]);
  b.markCommitted();
  t.deepEqual(await awaitDependencies(c.predecessors), []);
  t.is(c.poisoned, undefined);
});

test('OperationRegistry: an operation ended without settling does not block later ones', async (t) => {
  const registry = new OperationRegistry();
  const a = registry.begin('block', [1]);
  const b = registry.begin('block', [1]);
  registry.end(a);
  t.is(await settledState(waitForPredecessorRows(b)), 'resolved');
  t.deepEqual(b.predecessors, []);
});

test('OperationRegistry: operationOfSeq is an index of live, unfinished operations', (t) => {
  const registry = new OperationRegistry();
  const a = registry.begin('block', [1]);
  a.seq = 7n;
  t.is(registry.operationOfSeq(7n), a);
  a.markCommitted();
  t.is(registry.operationOfSeq(7n), undefined);
  const b = registry.begin('block', [1]);
  b.seq = 8n;
  registry.end(b);
  t.is(registry.operationOfSeq(8n), undefined);
});

const heapUsedAfterGc = () => {
  const collect = runInNewContext('gc') as () => void;
  collect();
  collect();
  return process.memoryUsage().heapUsed;
};

test('OperationRegistry: 10k queued operations on one node take O(n) memory and waits (WP6b item 3)', async (t) => {
  setFlagsFromString('--expose-gc');
  const measure = (count: number) => {
    const registry = new OperationRegistry();
    const before = heapUsedAfterGc();
    const operations = Array.from({ length: count }, () =>
      registry.begin('block', [1, 2])
    );
    // every queued operation waits for its predecessors' rows
    const waits = operations.map(async (operation) =>
      waitForPredecessorRows(operation)
    );
    const used = heapUsedAfterGc() - before;
    return { operations, registry, used, waits };
  };
  const small = measure(10_000);
  const large = measure(20_000);
  t.log(
    `heap: 10k ops ${(small.used / 1e6).toFixed(1)} MB, 20k ops ${(
      large.used / 1e6
    ).toFixed(1)} MB`
  );
  /*
   * the old registry copied every live operation sharing a node:
   * 10k ops = 50M references (> 400 MB)
   */
  t.true(small.used < 100e6, `10k operations used ${small.used} bytes`);
  t.true(
    large.used < small.used * 2.5,
    'doubling the operations at most ~doubles the memory'
  );
  // order: completing them in order releases each waiter
  await Promise.all(
    [small, large].map(async ({ operations, waits }) => {
      operations.forEach((operation) => {
        operation.markCommitted();
      });
      await Promise.all(waits);
    })
  );
  t.pass();
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
