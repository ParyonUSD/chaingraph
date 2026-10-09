/* eslint-disable @typescript-eslint/no-magic-numbers, max-params, @typescript-eslint/require-array-sort-compare */
// cspell:ignore clickhouse
import test from 'ava';

import type { ChaingraphTransaction } from '../../types/chaingraph.js';

import {
  assignTransactionIds,
  blockUtxoDelta,
  chunked,
} from './block-commit.js';
import type { UtxoOutput } from './utxo.js';

test('chunked: splits into bounded chunks', (t) => {
  t.deepEqual(chunked([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]]);
  t.deepEqual(chunked([], 2), []);
});

test('assignTransactionIds: stored ids win, new ids in block order, duplicates share one id', (t) => {
  const { internalIds, inserted } = assignTransactionIds(
    ['a', 'b', 'c', 'b', 'd'],
    new Map([['b', 50n]]),
    [100n, 101n, 102n]
  );
  t.deepEqual(internalIds, [100n, 50n, 101n, 50n, 102n]);
  t.deepEqual([...inserted].sort(), ['a', 'c', 'd']);
  t.throws(() => assignTransactionIds(['a', 'b'], new Map(), [1n]));
  t.throws(() => assignTransactionIds(['a'], new Map(), [1n, 2n]));
});

const tx = (
  hash: string,
  spends: [string, number][],
  outputs: number,
  isCoinbase = false
): ChaingraphTransaction => ({
  hash,
  inputs: spends.map(([outpointTransactionHash, outpointIndex]) => ({
    outpointIndex,
    outpointTransactionHash,
    sequenceNumber: 0,
    unlockingBytecode: '',
  })),
  isCoinbase,
  locktime: 0,
  outputs: Array.from({ length: outputs }, (_, index) => ({
    lockingBytecode: `5${index}`,
    valueSatoshis: BigInt(index + 1),
  })),
  sizeBytes: 100,
  version: 2,
});

const stored = (hash: string, index: number): UtxoOutput => ({
  lockingBytecode: 'aa',
  outputIndex: index,
  transactionHash: hash,
  transactionInternalId: 9n,
  valueSatoshis: 10n,
});

test('blockUtxoDelta: per node, only transactions not already accepted transition; unknown spends are pending', (t) => {
  const transactions = [
    {
      internalId: 1n,
      transaction: tx('cb', [['00'.repeat(32), 0xffffffff]], 1, true),
    },
    { internalId: 2n, transaction: tx('t1', [['old', 0]], 2) },
    {
      internalId: 3n,
      transaction: tx(
        't2',
        [
          ['t1', 0],
          ['unknown', 4],
        ],
        1
      ),
    },
    { internalId: 4n, transaction: tx('mempool', [['old', 1]], 1) },
  ];
  const resolveSpent = (hash: string, index: number) => {
    if (hash === 'old') return stored(hash, index);
    if (hash === 't1')
      return { ...stored(hash, index), transactionInternalId: 2n };
    return undefined;
  };
  const node1 = blockUtxoDelta({
    acceptedBefore: new Set(['mempool']),
    nodeInternalId: 1,
    resolveSpent,
    transactions,
  });
  t.deepEqual(node1.transitions, ['cb', 't1', 't2']);
  t.deepEqual(
    node1.rows.map((row) => [
      row.nodeInternalId,
      `${row.output.transactionHash}:${row.output.outputIndex}`,
      row.sign,
    ]),
    [
      [1, 'cb:0', 1],
      [1, 't1:0', 1],
      [1, 't1:1', 1],
      [1, 'old:0', -1],
      [1, 't2:0', 1],
      [1, 't1:0', -1],
    ]
  );
  t.deepEqual(node1.pending, [
    { hash: 'unknown', index: 4, inputIndex: 1, spender: 't2' },
  ]);
  // node 2 has nothing in its mempool: every tx transitions; node 1's state is not consulted
  const node2 = blockUtxoDelta({
    acceptedBefore: new Set(),
    nodeInternalId: 2,
    resolveSpent,
    transactions,
  });
  t.deepEqual(node2.transitions, ['cb', 't1', 't2', 'mempool']);
  t.true(node2.rows.every((row) => row.nodeInternalId === 2));
  t.is(
    node2.rows.filter((row) => row.output.transactionHash === 'old').length,
    2
  );
  // net effect of the block on t1:0 (created and spent in the block) is 0
  const t10 = node2.rows
    .filter(
      (row) =>
        row.output.transactionHash === 't1' && row.output.outputIndex === 0
    )
    .reduce((sum, row) => sum + row.sign, 0);
  t.is(t10, 0);
});
