/* eslint-disable @typescript-eslint/no-magic-numbers */
import test from 'ava';

import type { MempoolCleanupVector } from './mempool-cleanup.vectors.js';
import {
  afterExpectedSweep,
  mempoolCleanupVectors,
  planVector,
  randomVector,
  seededRandom,
  timestampToDate,
  vectorNodeInclusions,
  vectorNodeMempool,
  vectorTxHash,
} from './mempool-cleanup.vectors.js';
import type { NodeMempool, Outpoint, TxKey } from './mempool-graph.js';
import {
  indexMempool,
  outpoint,
  planAcceptedBlockCleanup,
  planMempoolExpiry,
  planMempoolReplacement,
} from './mempool-graph.js';

const randomSeeds = Array.from({ length: 200 }, (_, i) => i + 1);

mempoolCleanupVectors.forEach((vector) => {
  test(`mempool-graph: vector ${vector.name}: ${vector.description}`, (t) => {
    t.deepEqual(planVector(vector), vector.expected);
    t.deepEqual(
      planVector(afterExpectedSweep(vector)).archived,
      [],
      'a second sweep archives nothing'
    );
  });
});

const forNode = <Row extends { node: number }>(
  rows: readonly Row[],
  node: number
) => rows.filter((row) => row.node === node);

test('mempool-graph: one node never affects another node (random vectors)', (t) => {
  randomSeeds.forEach((seed) => {
    const vector = randomVector(seed);
    const otherNodeRows = randomVector(seed + 10_000);
    const replaceNode2 = <Row extends { node: number }>(
      own: readonly Row[],
      other: readonly Row[]
    ) => [
      ...own.filter((row) => row.node !== 2),
      ...other.filter((row) => row.node === 2),
    ];
    const swapped: MempoolCleanupVector = {
      ...vector,
      nodeBlocks: replaceNode2(vector.nodeBlocks, otherNodeRows.nodeBlocks),
      nodeTransactions: replaceNode2(
        vector.nodeTransactions,
        otherNodeRows.nodeTransactions
      ),
    };
    const node1Only: MempoolCleanupVector = {
      ...vector,
      nodeBlocks: vector.nodeBlocks.filter((row) => row.node === 1),
      nodeTransactions: vector.nodeTransactions.filter((row) => row.node === 1),
    };
    const original = planVector(vector);
    [planVector(swapped), planVector(node1Only)].forEach((outcome) => {
      t.deepEqual(forNode(outcome.history, 1), forNode(original.history, 1));
      t.deepEqual(
        forNode(outcome.remaining, 1),
        forNode(original.remaining, 1)
      );
    });
  });
});

const shuffle = <T>(items: readonly T[], random: () => number) => {
  const copy = [...items];
  copy.forEach((_, i) => {
    const j = i + Math.floor(random() * (copy.length - i));
    [copy[i], copy[j]] = [copy[j]!, copy[i]!];
  });
  return copy;
};

test('mempool-graph: plans do not depend on input order (random vectors)', (t) => {
  randomSeeds.forEach((seed) => {
    const vector = randomVector(seed);
    const random = seededRandom(seed + 20_000);
    vector.nodes.forEach(({ internalId: node }) => {
      const mempool = vectorNodeMempool(vector, node);
      const inclusions = vectorNodeInclusions(vector, node);
      const shuffledMempool: NodeMempool = {
        txs: new Map(
          shuffle([...mempool.txs], random).map(([tx, entry]) => [
            tx,
            { ...entry, spends: shuffle(entry.spends, random) },
          ])
        ),
      };
      const shuffledInclusions = shuffle(inclusions, random).map(
        (inclusion) => ({
          ...inclusion,
          spends: shuffle(inclusion.spends, random),
        })
      );
      const expected = planAcceptedBlockCleanup(mempool, inclusions);
      const actual = planAcceptedBlockCleanup(
        shuffledMempool,
        shuffledInclusions
      );
      t.deepEqual(actual, expected);
      t.is(new Set(actual.map((archive) => archive.tx)).size, actual.length);
    });
  });
});

test('mempool-graph: random vectors archive each row once and report direct rows from history', (t) => {
  randomSeeds.forEach((seed) => {
    const { expected } = randomVector(seed);
    const keys = expected.history.map((row) => `${row.node}:${row.tx}`);
    t.is(new Set(keys).size, keys.length);
    t.deepEqual(
      expected.archived,
      expected.history.filter((row) =>
        expected.archived.some(
          (archived) => archived.node === row.node && archived.tx === row.tx
        )
      )
    );
  });
});

const tx = (id: number) => vectorTxHash(id);
const spend = (id: number, index = 0): Outpoint => outpoint(tx(id), index);
const at = (day: number) =>
  timestampToDate(`2026-01-${day.toString().padStart(2, '0')} 00:00:00`);
const mempoolOf = (
  entries: readonly (readonly [number, readonly Outpoint[]])[]
): NodeMempool => ({
  txs: new Map(
    entries.map(([id, spends]) => [tx(id), { spends, validatedAt: at(1) }])
  ),
});
const summarize = (
  archives: readonly { tx: TxKey; replacedAt: Date | null; cause: string }[]
) =>
  archives.map((archive) => ({
    cause: archive.cause,
    replacedAt: archive.replacedAt?.toISOString() ?? null,
    tx: parseInt(archive.tx.slice(0, 2), 16),
  }));

test('mempool-graph: replacement archives same-outpoint spenders, then cascades', (t) => {
  const mempool = mempoolOf([
    [1, [spend(10)]],
    [2, [spend(1)]],
    [3, [spend(2)]],
    [4, [spend(11)]],
    [5, [spend(10), spend(11)]],
    [6, [spend(12)]],
  ]);
  const index = indexMempool(mempool);
  t.deepEqual(
    summarize(
      planMempoolReplacement(
        mempool,
        { spends: [spend(10), spend(11)], tx: tx(5), validatedAt: at(3) },
        index
      )
    ),
    [
      { cause: 'replaced', replacedAt: at(3).toISOString(), tx: 1 },
      { cause: 'replaced', replacedAt: at(3).toISOString(), tx: 4 },
      { cause: 'descendant', replacedAt: at(3).toISOString(), tx: 2 },
      { cause: 'descendant', replacedAt: at(3).toISOString(), tx: 3 },
    ]
  );
  t.deepEqual(
    planMempoolReplacement(mempool, {
      spends: [spend(12, 1)],
      tx: tx(7),
      validatedAt: at(3),
    }),
    [],
    'a different output index is not a conflict'
  );
});

test('mempool-graph: expiry archives the transaction and its descendants', (t) => {
  const mempool = mempoolOf([
    [1, [spend(10)]],
    [2, [spend(1)]],
    [3, [spend(2), spend(4)]],
    [4, [spend(11)]],
  ]);
  t.deepEqual(
    summarize(planMempoolExpiry(mempool, { replacedAt: at(9), tx: tx(1) })),
    [
      { cause: 'expired', replacedAt: at(9).toISOString(), tx: 1 },
      { cause: 'descendant', replacedAt: at(9).toISOString(), tx: 2 },
      { cause: 'descendant', replacedAt: at(9).toISOString(), tx: 3 },
    ]
  );
  t.deepEqual(planMempoolExpiry(mempool, { replacedAt: at(9), tx: tx(9) }), []);
});

test('mempool-graph: a descendant of several conflicts takes the earliest replacement', (t) => {
  const mempool = mempoolOf([
    [1, [spend(10)]],
    [2, [spend(11)]],
    [3, [spend(1), spend(2)]],
    [4, [spend(3)]],
  ]);
  t.deepEqual(
    summarize(
      planAcceptedBlockCleanup(mempool, [
        { acceptedAt: at(5), spends: [spend(10)], tx: tx(8) },
        { acceptedAt: at(2), spends: [spend(11)], tx: tx(9) },
      ])
    ),
    [
      { cause: 'conflict', replacedAt: at(5).toISOString(), tx: 1 },
      { cause: 'conflict', replacedAt: at(2).toISOString(), tx: 2 },
      { cause: 'descendant', replacedAt: at(2).toISOString(), tx: 3 },
      { cause: 'descendant', replacedAt: at(2).toISOString(), tx: 4 },
    ]
  );
});
