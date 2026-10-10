/* eslint-disable @typescript-eslint/no-magic-numbers, functional/no-loop-statement, functional/no-let, line-comment-position, complexity */
import test from 'ava';

import { RecentCache } from './recent-cache.js';
import { OutputRegistry } from './utxo.js';

test('RecentCache keeps the newest `capacity` entries; set refreshes a key', (t) => {
  const cache = new RecentCache<string, number>(3);
  ['a', 'b', 'c'].forEach((key, index) => {
    cache.set(key, index);
  });
  cache.set('a', 10); // a is now the newest
  cache.set('d', 3); // evicts b
  t.false(cache.has('b'));
  t.deepEqual(
    ['a', 'c', 'd'].map((key) => cache.get(key)),
    [10, 2, 3]
  );
  cache.set('e', 4); // evicts c
  cache.set('f', 5); // evicts a
  t.deepEqual(
    ['a', 'c', 'd', 'e', 'f'].map((key) => cache.has(key)),
    [false, false, true, true, true]
  );
  t.is(cache.size, 3);
});

test('RecentCache survives being emptied and capacity 0', (t) => {
  const cache = new RecentCache<string, number>(1);
  cache.set('a', 1);
  cache.delete('a');
  cache.set('b', 2);
  cache.set('c', 3);
  t.deepEqual([cache.has('b'), cache.get('c'), cache.size], [false, 3, 1]);
  const none = new RecentCache<string, number>(0);
  none.set('a', 1);
  none.set('b', 2);
  t.is(none.size, 0);
});

test('RecentCache matches a reference FIFO under random refreshes', (t) => {
  const capacity = 50;
  const cache = new RecentCache<number, number>(capacity);
  const reference: number[] = [];
  let seed = 7;
  const random = () => {
    seed = (seed * 1103515245 + 12345) % 2147483648;
    return seed;
  };
  for (let step = 0; step < 20_000; step += 1) {
    const key = random() % 200;
    if (random() % 10 === 0) {
      cache.delete(key);
      const index = reference.indexOf(key);
      if (index !== -1) reference.splice(index, 1);
    } else {
      cache.set(key, step);
      const index = reference.indexOf(key);
      if (index !== -1) reference.splice(index, 1);
      reference.push(key);
      if (reference.length > capacity) reference.shift();
    }
  }
  t.is(cache.size, reference.length);
  t.true(reference.every((key) => cache.has(key)));
});

/*
 * G1 lab stall (g1-fix-pass-2.md §1): evicting with `map.keys().next()` is
 * quadratic once the cache is full (300k evictions from a 500k cache took
 * ~30 s of one thread). Linear eviction does them in well under a second.
 */
test('evicting 300k outputs from a full 500k OutputRegistry cache is linear', (t) => {
  const registry = new OutputRegistry<object>(500_000);
  const internalId = Promise.resolve(1n);
  const output = { lockingBytecode: '51', valueSatoshis: 1n };
  const commitOne = (count: number, prefix: string) => {
    const owner = {};
    registry.register(
      owner,
      Array.from({ length: count / 2 }, (_, index) => ({
        hash: `${prefix}${index.toString(16).padStart(60, '0')}`,
        internalId,
        outputs: [output, output],
      }))
    );
    registry.release(owner, true);
  };
  commitOne(500_000, 'aaaa');
  const started = performance.now();
  for (let batch = 0; batch < 6; batch += 1) {
    commitOne(50_000, `b${batch}00`);
  }
  const elapsedMs = performance.now() - started;
  t.is(registry.recentCount, 500_000);
  t.true(elapsedMs < 3_000, `took ${Math.round(elapsedMs)} ms`);
});
