/* eslint-disable @typescript-eslint/no-magic-numbers, camelcase, @typescript-eslint/naming-convention */
import test from 'ava';

import type { IdKind, ReservationStore } from './id-allocator.js';
import {
  ClickHouseReservationStore,
  IdAllocator,
  segmentIds,
  segmentsLength,
} from './id-allocator.js';
import { createScratchDatabase, e2eClickHouseUrl } from './test-support.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

/** An in-memory store that records reservations and can fail on demand. */
const memoryStore = (initial: [IdKind, bigint, bigint][] = []) => {
  const reservations = [...initial];
  const log: string[] = [];
  const control = { failNext: false };
  const store: ReservationStore = {
    highestReservedEnd: async (kind) =>
      reservations
        .filter(([reservedKind]) => reservedKind === kind)
        .reduce((highest, [, , end]) => (end > highest ? end : highest), 1n),
    reserve: async (kind, start, end) => {
      await new Promise((resolve) => {
        setTimeout(resolve, 1);
      });
      if (control.failNext) {
        control.failNext = false;
        // eslint-disable-next-line functional/no-throw-statement
        throw new Error('store unavailable');
      }
      reservations.push([kind, start, end]);
      log.push(`reserved ${kind} [${start}, ${end})`);
    },
  };
  return { control, log, reservations, store };
};

test('IdAllocator: ids start at 1 and are reserved before use', async (t) => {
  const { log, store } = memoryStore();
  const allocator = new IdAllocator(store, {
    rangeSizes: { transaction: 10n },
  });
  const first = await allocator.allocate('transaction', 3);
  t.deepEqual(first, [{ end: 4n, start: 1n }]);
  t.deepEqual(log, ['reserved transaction [1, 11)']);
  t.is(await allocator.allocateOne('transaction'), 4n);
  t.deepEqual(log.length, 1);
});

test('IdAllocator: allocations spanning ranges reserve as needed, in order', async (t) => {
  const { log, store } = memoryStore();
  const allocator = new IdAllocator(store, { rangeSizes: { block: 10n } });
  await allocator.allocate('block', 8);
  const spanning = await allocator.allocate('block', 5);
  t.deepEqual(spanning, [
    { end: 11n, start: 9n },
    { end: 14n, start: 11n },
  ]);
  t.deepEqual(segmentIds(spanning), [9n, 10n, 11n, 12n, 13n]);
  // the rest of the current range, then one range big enough for the remainder
  const large = await allocator.allocate('block', 25);
  t.is(segmentsLength(large), 25n);
  t.deepEqual(large, [
    { end: 21n, start: 14n },
    { end: 39n, start: 21n },
  ]);
  t.deepEqual(log, [
    'reserved block [1, 11)',
    'reserved block [11, 21)',
    'reserved block [21, 39)',
  ]);
  t.deepEqual(await allocator.allocate('block', 0), []);
  await t.throwsAsync(allocator.allocate('block', -1));
});

test('IdAllocator: concurrent allocations are disjoint and increasing', async (t) => {
  const { store } = memoryStore();
  const allocator = new IdAllocator(store, { rangeSizes: { transaction: 7n } });
  const results = await Promise.all(
    Array.from({ length: 20 }, async (_, index) =>
      allocator.allocate('transaction', (index % 4) + 1)
    )
  );
  const all = results.flatMap(segmentIds);
  t.is(new Set(all).size, all.length);
  t.deepEqual(
    all,
    [...all].sort((a, b) => (a < b ? -1 : 1))
  );
  t.is(all[0], 1n);
  t.is(all[all.length - 1], BigInt(all.length));
});

test('IdAllocator: kinds are independent', async (t) => {
  const { store } = memoryStore();
  const allocator = new IdAllocator(store);
  t.is(await allocator.allocateOne('transaction'), 1n);
  t.is(await allocator.allocateOne('block'), 1n);
  t.is(await allocator.allocateOne('node'), 1n);
  t.is(await allocator.allocateOne('transaction'), 2n);
});

test('IdAllocator: a restart resumes above the highest reservation (a crash loses at most one range)', async (t) => {
  const { reservations, store } = memoryStore();
  const first = new IdAllocator(store, { rangeSizes: { transaction: 100n } });
  await first.allocate('transaction', 30);
  await first.allocate('transaction', 80);
  // crash: ids 111..200 of the second range are never used
  const second = new IdAllocator(store, { rangeSizes: { transaction: 100n } });
  t.is(await second.allocateOne('transaction'), 201n);
  t.deepEqual(
    reservations.map(([, start, end]) => [start, end]),
    [
      [1n, 101n],
      [101n, 201n],
      [201n, 301n],
    ]
  );
});

test('IdAllocator: a failed reservation hands out nothing and later calls recover', async (t) => {
  const { control, store } = memoryStore();
  const allocator = new IdAllocator(store, { rangeSizes: { block: 5n } });
  control.failNext = true;
  await t.throwsAsync(allocator.allocate('block', 2), {
    message: /store unavailable/u,
  });
  t.deepEqual(await allocator.allocate('block', 2), [{ end: 3n, start: 1n }]);
});

test('IdAllocator: a lost lease stops new reservations', async (t) => {
  const { store } = memoryStore();
  const held = { value: true };
  const allocator = new IdAllocator(store, {
    assertHeld: () => {
      if (!held.value) {
        // eslint-disable-next-line functional/no-throw-statement
        throw new Error('lease lost');
      }
    },
    rangeSizes: { block: 2n },
  });
  await allocator.allocate('block', 2);
  held.value = false;
  await t.throwsAsync(allocator.allocate('block', 1), {
    message: /lease lost/u,
  });
});

e2e(
  '[e2e] ClickHouseReservationStore: durable ranges, resume after restart',
  async (t) => {
    const scratch = await createScratchDatabase('ids');
    t.teardown(scratch.drop);
    const { client } = scratch;
    const store = new ClickHouseReservationStore(client, () => 3n);
    t.is(await store.highestReservedEnd('transaction'), 1n);
    const first = new IdAllocator(store, {
      rangeSizes: { transaction: 1000n },
    });
    t.deepEqual(await first.allocate('transaction', 600), [
      { end: 601n, start: 1n },
    ]);
    t.deepEqual(await first.allocate('transaction', 900), [
      { end: 1001n, start: 601n },
      { end: 1501n, start: 1001n },
    ]);
    const rows = await client.query<{
      id_kind: string;
      range_start: string;
      range_end: string;
      writer_epoch: string;
    }>(
      'SELECT id_kind, range_start, range_end, writer_epoch FROM id_reservation ORDER BY range_start'
    );
    t.deepEqual(rows, [
      {
        id_kind: 'transaction',
        range_end: '1001',
        range_start: '1',
        writer_epoch: '3',
      },
      {
        id_kind: 'transaction',
        range_end: '2001',
        range_start: '1001',
        writer_epoch: '3',
      },
    ]);
    // a retried reservation (same token) is idempotent
    await store.reserve('transaction', 1001n, 2001n);
    t.is((await client.query('SELECT * FROM id_reservation')).length, 2);
    const restarted = new IdAllocator(
      new ClickHouseReservationStore(client, () => 4n),
      { rangeSizes: { transaction: 1000n } }
    );
    t.is(await restarted.allocateOne('transaction'), 2001n);
    t.is(await restarted.allocateOne('block'), 1n);
  }
);
