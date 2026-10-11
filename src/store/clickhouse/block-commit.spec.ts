/* eslint-disable @typescript-eslint/no-magic-numbers, @typescript-eslint/require-array-sort-compare */
// cspell:ignore clickhouse
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

import test from 'ava';

import type { Outpoint } from '../mempool-graph.js';

import {
  appendAll,
  assignTransactionIds,
  chunked,
  minMax,
} from './block-commit.js';
import { changeRows } from './mempool-commit.js';
import type {
  LiveMempoolEntry,
  MempoolTxFacts,
  NodeMempoolChange,
} from './mempool-state.js';
import type { UtxoOutput } from './utxo.js';

const execFileAsync = promisify(execFile);

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

const stored = (hash: string, index: number): UtxoOutput => ({
  lockingBytecode: 'aa',
  outputIndex: index,
  transactionHash: hash,
  transactionInternalId: 9n,
  valueSatoshis: 10n,
});

test('utxo off: changeRows produces no utxo rows; the other rows are unchanged; no pending-spend rows exist', (t) => {
  // every mempool transition: addition, replacement archive, confirmation, resolution
  const spends: Outpoint[] = ['old:0', 'gone:1'];
  const facts = (hash: string, internalId: bigint): MempoolTxFacts => ({
    hash,
    internalId,
    outputs: [stored(hash, 0)],
    spends,
    spent: new Map<Outpoint, UtxoOutput>([['old:0', stored('old', 0)]]),
  });
  const entry = (internalId: bigint): LiveMempoolEntry => ({
    internalId,
    spends,
    unresolved: new Set<Outpoint>(['gone:1']),
    validatedAt: new Date(0),
  });
  const changes: NodeMempoolChange[] = [
    {
      addition: { facts: facts('add', 5n), validatedAt: new Date(1) },
      archives: [
        {
          cause: 'conflict',
          entry: entry(6n),
          facts: facts('replaced', 6n),
          replacedAt: new Date(2),
          tx: 'replaced',
        },
        {
          cause: 'confirmed',
          entry: entry(7n),
          facts: facts('mined', 7n),
          replacedAt: null,
          tx: 'mined',
        },
      ],
      node: 1,
      resolutions: [
        {
          inputIndex: 1,
          outpoint: 'late:0',
          output: stored('late', 0),
          spender: 'waiting',
        },
      ],
    },
  ];
  const historyIds = [10n, 11n];
  const rowsOn = changeRows(changes, historyIds);
  const rowsOff = changeRows(changes, historyIds, { utxo: false });
  t.true(rowsOn.utxo.length > 0);
  t.deepEqual(rowsOff.utxo, []);
  t.deepEqual({ ...rowsOff, utxo: [] }, { ...rowsOn, utxo: [] });
  t.deepEqual(Object.keys(rowsOn).sort(), [
    'history',
    'nodeTransaction',
    'txAcceptance',
    'utxo',
  ]);
  /*
   * utxo on: the resolution's −1; the replaced entry's −1 output and +1 for
   * its written (known) spend only; the addition's +1 output and −1 for its
   * known spend only (no row of any kind for the unknown `gone:1`)
   */
  t.deepEqual(
    rowsOn.utxo.map((row) => [
      `${row.output.transactionHash}:${row.output.outputIndex}`,
      row.sign,
    ]),
    [
      ['late:0', -1],
      ['replaced:0', -1],
      ['old:0', 1],
      ['add:0', 1],
      ['old:0', -1],
    ]
  );
});

test('appendAll / minMax: 300k elements without a stack overflow (WP6b item 1)', async (t) => {
  const items = Array.from({ length: 300_000 }, (_, index) => index);
  const target = [-1];
  t.is(appendAll(target, items).length, 300_001);
  t.is(target[300_000], 299_999);
  t.deepEqual(minMax([5, ...items, -3]), [-3, 299_999]);
  t.throws(() => minMax([]), { instanceOf: RangeError });
  /*
   * AVA's worker threads have a larger stack than a process's main thread
   * (where the agent runs), so check the main-thread case in a child
   * process: the spread overflows there, the helpers do not.
   */
  const moduleUrl = new URL('./block-commit.js', import.meta.url).href;
  const { stdout } = await execFileAsync(process.execPath, [
    '--input-type=module',
    '-e',
    `import { appendAll, minMax } from ${JSON.stringify(moduleUrl)};
     const items = Array.from({ length: 300000 }, (_, index) => index);
     let spread = 'ok';
     try { [].push(...items); } catch (error) { spread = error.constructor.name; }
     const appended = appendAll([], items).length;
     console.log(JSON.stringify({ appended, range: minMax(items), spread }));`,
  ]);
  t.deepEqual(JSON.parse(stdout), {
    appended: 300_000,
    range: [0, 299_999],
    spread: 'RangeError',
  });
});
