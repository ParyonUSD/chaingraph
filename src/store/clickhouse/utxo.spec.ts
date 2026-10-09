/* eslint-disable @typescript-eslint/no-magic-numbers, functional/no-loop-statement, functional/no-let, complexity */
// cspell:ignore clickhouse
import { readFileSync } from 'node:fs';

import test from 'ava';

import type { UtxoOutput, UtxoRow } from './utxo.js';
import {
  AcceptanceCounter,
  acceptanceTransition,
  encodeUtxoRows,
  heightBatches,
  outpointKey,
  OutputRegistry,
  utxoByScriptColumns,
  utxoColumns,
  utxoRowsForTransition,
} from './utxo.js';

test('acceptanceTransition: only 0 ↔ accepted changes emit rows', (t) => {
  t.is(acceptanceTransition(false, true), 1);
  t.is(acceptanceTransition(true, false), -1);
  t.is(acceptanceTransition(true, true), 0);
  t.is(acceptanceTransition(false, false), 0);
});

const output = (hash: string, index: number): UtxoOutput => ({
  lockingBytecode: 'aa',
  outputIndex: index,
  transactionHash: hash,
  transactionInternalId: 1n,
  valueSatoshis: 1n,
});

test('utxoRowsForTransition: +outputs −spends, and the inverse', (t) => {
  const rows = utxoRowsForTransition({
    delta: 1,
    nodeInternalId: 3,
    outputs: [output('aa', 0), output('aa', 1)],
    spentOutputs: [output('bb', 0)],
  });
  t.deepEqual(
    rows.map((row) => [
      row.nodeInternalId,
      row.output.transactionHash,
      row.sign,
    ]),
    [
      [3, 'aa', 1],
      [3, 'aa', 1],
      [3, 'bb', -1],
    ]
  );
  const inverse = utxoRowsForTransition({
    delta: -1,
    nodeInternalId: 3,
    outputs: [output('aa', 0)],
    spentOutputs: [output('bb', 0)],
  });
  t.deepEqual(
    inverse.map((row) => row.sign),
    [-1, 1]
  );
  t.deepEqual(
    utxoRowsForTransition({
      delta: 0,
      nodeInternalId: 3,
      outputs: [output('aa', 0)],
      spentOutputs: [],
    }),
    []
  );
});

/**
 * A model chain: blocks with transactions spending outputs of earlier
 * transactions; some blocks belong to a fork that shares transactions with
 * the main chain (as re-org blocks do), and two blocks share a coinbase hash
 * (BIP30 duplicates).
 */
interface ModelTx {
  hash: string;
  outputs: number;
  spends: [string, number][];
}
interface ModelBlock {
  id: string;
  txs: ModelTx[];
}

const seededRandom = (seed: number) => {
  let state = seed;
  return () => {
    state = (state * 1103515245 + 12345) % 2147483648;
    return state / 2147483648;
  };
};

const buildModel = (random: () => number) => {
  const unspent: [string, number][] = [];
  const blocks: ModelBlock[] = [];
  let txCounter = 0;
  const makeTx = (spendCount: number): ModelTx => {
    txCounter += 1;
    const spends: [string, number][] = [];
    for (let i = 0; i < spendCount && unspent.length > 0; i += 1) {
      const index = Math.floor(random() * unspent.length);
      spends.push(unspent.splice(index, 1)[0]!);
    }
    const tx: ModelTx = { hash: `t${txCounter}`, outputs: 2, spends };
    for (let i = 0; i < tx.outputs; i += 1) unspent.push([tx.hash, i]);
    return tx;
  };
  for (let height = 0; height < 8; height += 1) {
    const coinbase =
      height === 5
        ? { hash: 'dup-coinbase', outputs: 1, spends: [] }
        : { hash: `cb${height}`, outputs: 1, spends: [] };
    const txs = [coinbase, makeTx(1), makeTx(2)];
    blocks.push({ id: `b${height}`, txs });
  }
  // the duplicate coinbase again (BIP30)
  blocks.push({
    id: 'b8',
    txs: [{ hash: 'dup-coinbase', outputs: 1, spends: [] }, makeTx(1)],
  });
  // a fork of b6..b8 that re-includes b6's non-coinbase transactions
  const fork: ModelBlock = {
    id: 'f6',
    txs: [{ hash: 'cbf6', outputs: 1, spends: [] }, ...blocks[6]!.txs.slice(1)],
  };
  return { blocks, fork };
};

test('UTXO sign rules: sum(sign) is exactly 0 or 1 per (node, outpoint) under random accept / release / re-accept orderings', (t) => {
  for (let seed = 1; seed <= 40; seed += 1) {
    const random = seededRandom(seed);
    const { blocks, fork } = buildModel(random);
    const counter = new AcceptanceCounter();
    const sums = new Map<string, number>();
    const nodes = [1, 2];
    /** Per node: the accepted chain (block ids in height order). */
    const chains = new Map<number, ModelBlock[]>(nodes.map((n) => [n, []]));
    const txByHash = new Map<string, ModelTx>();
    [...blocks, fork].forEach((block) => {
      block.txs.forEach((tx) => txByHash.set(tx.hash, tx));
    });
    const apply = (node: number, block: ModelBlock, sign: -1 | 1) => {
      const rows: UtxoRow[] = [];
      block.txs.forEach((tx) => {
        const delta = counter.apply(node, tx.hash, block.id, sign);
        rows.push(
          ...utxoRowsForTransition({
            delta,
            nodeInternalId: node,
            outputs: Array.from({ length: tx.outputs }, (_, i) =>
              output(tx.hash, i)
            ),
            spentOutputs: tx.spends.map(([hash, i]) => output(hash, i)),
          })
        );
      });
      // merge order is irrelevant: apply the rows shuffled
      rows
        .sort(() => random() - 0.5)
        .forEach((row) => {
          const key = `${row.nodeInternalId}|${outpointKey(
            row.output.transactionHash,
            row.output.outputIndex
          )}`;
          sums.set(key, (sums.get(key) ?? 0) + row.sign);
        });
    };
    const definitionalUnspent = (node: number) => {
      const accepted = new Set(
        chains.get(node)!.flatMap((block) => block.txs.map((tx) => tx.hash))
      );
      const spent = new Set(
        [...accepted].flatMap((hash) =>
          txByHash.get(hash)!.spends.map(([h, i]) => outpointKey(h, i))
        )
      );
      return new Set(
        [...accepted].flatMap((hash) =>
          Array.from({ length: txByHash.get(hash)!.outputs }, (_, i) =>
            outpointKey(hash, i)
          ).filter((key) => !spent.has(key))
        )
      );
    };
    for (let step = 0; step < 60; step += 1) {
      const node = nodes[Math.floor(random() * nodes.length)]!;
      const chain = chains.get(node)!;
      const action = random();
      if (action < 0.6) {
        // accept the next block (main chain, or the fork tip at height 6)
        const height = chain.length;
        const last = chain[chain.length - 1];
        const candidates =
          last === fork
            ? []
            : height === 6
            ? [blocks[6]!, fork]
            : height < blocks.length
            ? [blocks[height]!]
            : [];
        const next = candidates[Math.floor(random() * candidates.length)];
        if (next !== undefined) {
          chain.push(next);
          apply(node, next, 1);
        }
      } else if (chain.length > 0) {
        // re-org: release a suffix (stale blocks are always a suffix)
        const depth = 1 + Math.floor(random() * Math.min(3, chain.length));
        const released = chain.splice(chain.length - depth, depth);
        released.reverse().forEach((block) => {
          apply(node, block, -1);
        });
      }
      nodes.forEach((checkNode) => {
        const expected = definitionalUnspent(checkNode);
        sums.forEach((sum, key) => {
          const [keyNode, outpoint] = key.split('|') as [string, string];
          if (Number(keyNode) !== checkNode) return;
          // includes the BIP30 duplicate coinbase (two containers, one +1)
          t.true(
            sum === 0 || sum === 1,
            `seed ${seed} step ${step} ${key}: ${sum}`
          );
          t.is(
            sum === 1,
            expected.has(outpoint),
            `seed ${seed} step ${step} ${key}`
          );
        });
        expected.forEach((outpoint) => {
          t.is(
            sums.get(`${checkNode}|${outpoint}`),
            1,
            `seed ${seed} missing ${outpoint}`
          );
        });
      });
    }
  }
});

test('UTXO sign rules: a spender visible before its creator would read −1 (why child commits depend on parents)', (t) => {
  const counter = new AcceptanceCounter();
  const sums = new Map<string, number>();
  const add = (rows: UtxoRow[]) => {
    rows.forEach((row) => {
      const key = outpointKey(
        row.output.transactionHash,
        row.output.outputIndex
      );
      sums.set(key, (sums.get(key) ?? 0) + row.sign);
    });
  };
  // child block (spends p:0) accepted first, parent second
  add(
    utxoRowsForTransition({
      delta: counter.apply(1, 'c', 'b2', 1),
      nodeInternalId: 1,
      outputs: [output('c', 0)],
      spentOutputs: [output('p', 0)],
    })
  );
  t.is(sums.get('p:0'), -1);
  add(
    utxoRowsForTransition({
      delta: counter.apply(1, 'p', 'b1', 1),
      nodeInternalId: 1,
      outputs: [output('p', 0)],
      spentOutputs: [],
    })
  );
  t.is(sums.get('p:0'), 0);
  t.is(sums.get('c:0'), 1);
});

const utxoDdl = readFileSync(
  new URL('../../../src/store/clickhouse/ddl/030_utxo.sql', import.meta.url),
  'utf8'
);

const insertedColumns = (table: string) => {
  const match = new RegExp(
    `CREATE TABLE IF NOT EXISTS cg\\.${table}\\s*\\(([\\s\\S]*?)\\n\\)\\nENGINE`,
    'u'
  ).exec(utxoDdl);
  return (match?.[1] ?? '')
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line !== '' && !line.includes(' MATERIALIZED '))
    .map((line) => line.split(/\s+/u)[0]);
};

test('encodeUtxoRows: column lists match the DDL; one row per table per UtxoRow', (t) => {
  t.deepEqual([...utxoColumns], insertedColumns('utxo'));
  t.deepEqual([...utxoByScriptColumns], insertedColumns('utxo_by_script'));
  const rows: UtxoRow[] = [
    {
      nodeInternalId: 1,
      output: {
        ...output('11'.repeat(32), 0),
        fungibleTokenAmount: 5n,
        nonfungibleTokenCapability: 'minting',
        nonfungibleTokenCommitment: 'beef',
        tokenCategory: 'cc'.repeat(32),
      },
      sign: 1,
    },
    { nodeInternalId: 2, output: output('22'.repeat(32), 3), sign: -1 },
  ];
  const encoded = encodeUtxoRows(rows, 42n);
  t.is(encoded.rowCount, 2);
  // fixed widths + LEB128 lengths + Nullable flags (see utxoColumns)
  const first =
    4 +
    32 +
    32 +
    4 +
    8 +
    4 +
    8 +
    (1 + 1) +
    (1 + 8) +
    (1 + 1) +
    (1 + 1 + 2) +
    1 +
    8 +
    8;
  const second = 4 + 32 + 32 + 4 + 8 + 4 + 8 + (1 + 1) + 1 + 1 + 1 + 1 + 8 + 8;
  t.is(encoded.utxo.length, first + second);
  t.is(encoded.utxoByScript.length, first + second);
});

test('heightBatches: half-open batches covering the range', (t) => {
  t.deepEqual(heightBatches(0, 9, 4), [
    [0, 4],
    [4, 8],
    [8, 10],
  ]);
  t.deepEqual(heightBatches(5, 5, 100), [[5, 6]]);
  t.deepEqual(heightBatches(5, 4, 100), []);
  t.throws(() => heightBatches(0, 1, 0));
});

test('OutputRegistry: pinned outputs resolve waiters; committed ones move to the bounded cache', async (t) => {
  const registry = new OutputRegistry<string>(2);
  const waiting = registry.waitFor('aa:1');
  registry.register('op1', [
    {
      hash: 'aa',
      internalId: Promise.resolve(7n),
      outputs: [
        { lockingBytecode: '51', valueSatoshis: 1n },
        { lockingBytecode: '52', valueSatoshis: 2n },
      ],
    },
  ]);
  const resolved = await waiting.promise;
  t.is(resolved.output.lockingBytecode, '52');
  t.is(resolved.owner, 'op1');
  t.is(await resolved.internalId, 7n);
  t.is(registry.pinnedCount, 2);
  registry.release('op1', true);
  t.is(registry.pinnedCount, 0);
  t.is(registry.lookup('aa:0')?.owner, undefined);
  registry.register('op2', [
    {
      hash: 'bb',
      internalId: Promise.resolve(8n),
      outputs: [{ lockingBytecode: '53', valueSatoshis: 3n }],
    },
  ]);
  registry.release('op2', false);
  t.is(registry.lookup('bb:0'), undefined);
  registry.remember('cc:0', {
    internalId: Promise.resolve(9n),
    output: {
      lockingBytecode: '',
      outputIndex: 0,
      transactionHash: 'cc',
      valueSatoshis: 0n,
    },
    owner: undefined,
  });
  // capacity 2: the oldest (aa:0) is evicted
  t.is(registry.lookup('aa:0'), undefined);
  t.not(registry.lookup('aa:1'), undefined);
  const cancelled = registry.waitFor('zz:0');
  cancelled.cancel();
  t.is(registry.waitingCount, 0);
});
