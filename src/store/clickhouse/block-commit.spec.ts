/* eslint-disable @typescript-eslint/no-magic-numbers, max-params, @typescript-eslint/require-array-sort-compare */
// cspell:ignore clickhouse
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

import test from 'ava';

import type { ChaingraphTransaction } from '../../types/chaingraph.js';
import type { Outpoint } from '../mempool-graph.js';

import {
  appendAll,
  assignTransactionIds,
  blockUtxoDelta,
  chunked,
  minMax,
  pendingSpendRows,
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

test('utxo off: the pure transition functions produce no utxo rows; pending spends and other rows are unchanged', (t) => {
  const transactions = [
    { internalId: 2n, transaction: tx('t1', [['old', 0]], 2) },
    { internalId: 3n, transaction: tx('t2', [['unknown', 4]], 1) },
  ];
  const resolveSpent = (hash: string, index: number) =>
    hash === 'old' ? stored(hash, index) : undefined;
  const deltaOf = (utxo: boolean) =>
    blockUtxoDelta({
      acceptedBefore: new Set(),
      nodeInternalId: 1,
      resolveSpent,
      transactions,
      utxo,
    });
  const on = deltaOf(true);
  const off = deltaOf(false);
  t.true(on.rows.length > 0);
  t.deepEqual(off.rows, []);
  t.deepEqual(off.pending, on.pending);
  t.deepEqual(off.transitions, on.transitions);

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
  t.true(rowsOff.pendingSpend.length > 0);
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

test('pendingSpendRows: 100k unresolved spends for 2 nodes in linear time (WP6b item 2)', (t) => {
  const count = 100_000;
  const pendingInputs = Array.from({ length: count }, (_, index) => ({
    input: { outpointIndex: index, outpointTransactionHash: 'parent' },
    inputIndex: index % 1_000,
    transaction: { hash: `spender-${Math.floor(index / 1_000)}` },
  }));
  // node 1 and node 2 transition every spender; one input is node-agnostic
  const pendingUtxo = pendingInputs.slice(1).flatMap((item) =>
    [1, 2].map((node) => ({
      inputIndex: item.inputIndex,
      node,
      spender: item.transaction.hash,
    }))
  );
  /*
   * CPU time of this thread only (AVA runs other files in parallel worker
   * threads, which process.cpuUsage would count). The quadratic version took
   * about a minute; linear takes ~0.1–0.3 s, so 3 s leaves room for a loaded host.
   */
  const threadCpu = () => {
    // Node >= 23.9 (the typings here predate it); falls back to the process
    const { threadCpuUsage } = process as unknown as {
      threadCpuUsage?: () => NodeJS.CpuUsage;
    };
    const usage = threadCpuUsage?.() ?? process.cpuUsage();
    return (usage.user + usage.system) / 1_000;
  };
  const started = threadCpu();
  const plus = pendingSpendRows(pendingInputs, pendingUtxo, 1);
  const minus = pendingSpendRows(pendingInputs, pendingUtxo, -1);
  const cpuMs = threadCpu() - started;
  t.log(`${cpuMs.toFixed(0)} ms of thread CPU`);
  t.true(cpuMs < 3_000, `took ${cpuMs} ms of CPU`);
  t.is(plus.length, 1 + 2 * (count - 1));
  t.is(minus.length, plus.length);
  t.deepEqual(plus[0], {
    nodeInternalId: 0,
    outpointIndex: 0,
    outpointTransactionHash: 'parent',
    sign: 1,
    spenderInputIndex: 0,
    spenderTransactionHash: 'spender-0',
  });
  t.deepEqual(
    minus.slice(1, 3).map((row) => [row.nodeInternalId, row.sign]),
    [
      [1, -1],
      [2, -1],
    ]
  );
});
