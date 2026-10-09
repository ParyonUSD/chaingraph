/* eslint-disable @typescript-eslint/no-magic-numbers, functional/no-loop-statement, functional/no-let, no-await-in-loop, max-lines, complexity, functional/no-throw-statement, max-params, @typescript-eslint/require-array-sort-compare, @typescript-eslint/init-declarations, @typescript-eslint/no-loop-func */
// cspell:ignore clickhouse unhex aabb varint seqs
import { execFile } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

import test from 'ava';
import type { ExecutionContext } from 'ava';

import type {
  ChaingraphBlock,
  ChaingraphOutput,
  ChaingraphTransaction,
} from '../../types/chaingraph.js';
import type { ChaingraphStore } from '../types.js';

import { SimulatedCrash } from './block-commit.js';
import type { FaultInjector } from './block-commit.js';
import {
  ClickHouseStore,
  hashChainFromBlocks,
  linkedBlockSize,
} from './clickhouse-store.js';
import { ClickHouseClient } from './client.js';
import { MempoolNotImplementedError } from './mempool-state.js';
import { e2eClickHouseUrl } from './test-support.js';
import { readSnapshot } from './visibility.js';

const execFileAsync = promisify(execFile);
const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

/* -------------------------------------------------------------------- */
/* pure helpers                                                          */
/* -------------------------------------------------------------------- */

test('hashChainFromBlocks: hashes at their heights, null gaps (Postgres blockArrayToHashChain)', (t) => {
  t.deepEqual(hashChainFromBlocks([]), []);
  t.deepEqual(
    hashChainFromBlocks([
      { hash: 'c', height: 2 },
      { hash: 'a', height: 0 },
    ]),
    ['a', null, 'c']
  );
});

test('linkedBlockSize: header + varint(count) + transaction bytes', (t) => {
  t.is(linkedBlockSize(1, 100), 181);
  t.is(linkedBlockSize(253, 0), 83);
  t.is(linkedBlockSize(65_536, 0), 85);
});

/* -------------------------------------------------------------------- */
/* fixtures                                                              */
/* -------------------------------------------------------------------- */

const sha = (label: string) => createHash('sha256').update(label).digest('hex');
const zeroHash = '00'.repeat(32);
const category = sha('category');

interface TxSpec {
  label: string;
  spends?: [string, number][];
  outputs: ChaingraphOutput[];
  coinbase?: boolean;
}

const makeTx = (spec: TxSpec): ChaingraphTransaction => ({
  hash: sha(spec.label),
  inputs:
    spec.coinbase === true
      ? [
          {
            outpointIndex: 0xffffffff,
            outpointTransactionHash: zeroHash,
            sequenceNumber: 0xffffffff,
            unlockingBytecode: Buffer.from(spec.label).toString('hex'),
          },
        ]
      : (spec.spends ?? []).map(([hash, index]) => ({
          outpointIndex: index,
          outpointTransactionHash: hash,
          sequenceNumber: 0xfffffffe,
          unlockingBytecode: 'aabb',
        })),
  isCoinbase: spec.coinbase === true,
  locktime: 0,
  outputs: spec.outputs,
  sizeBytes: 100 + spec.label.length,
  version: 2,
});

const makeBlock = (
  height: number,
  previousBlockHash: string,
  transactions: ChaingraphTransaction[],
  label = `block-${height}`,
  timestamp = 1_600_000_000 + height * 600
): ChaingraphBlock => ({
  bits: 0x1d00ffff,
  hash: sha(label),
  height,
  merkleRoot: sha(`${label}-merkle`),
  nonce: height,
  previousBlockHash,
  sizeBytes: linkedBlockSize(
    transactions.length,
    transactions.reduce((sum, tx) => sum + tx.sizeBytes, 0)
  ),
  timestamp,
  transactions,
  version: 4,
});

const p2pkh = (label: string) => `76a914${sha(label).slice(0, 40)}88ac`;

/**
 * A 3-block chain with a transaction chain across blocks:
 * - block 0: coinbase c0 (2 outputs, the second a token output: FT + NFT)
 * - block 1: coinbase c1; tx a spends c0:0 → a:0 (FT), a:1
 * - block 2: coinbase c2; tx b spends a:0 and c0:1 → b:0
 */
const threeBlockChain = () => {
  const c0 = makeTx({
    coinbase: true,
    label: 'c0',
    outputs: [
      { lockingBytecode: p2pkh('miner'), valueSatoshis: 5_000_000_000n },
      {
        fungibleTokenAmount: 1_000n,
        lockingBytecode: p2pkh('holder'),
        nonfungibleTokenCapability: 'mutable',
        nonfungibleTokenCommitment: '01',
        tokenCategory: category,
        valueSatoshis: 1_000n,
      },
    ],
  });
  const block0 = makeBlock(0, zeroHash, [c0]);
  const c1 = makeTx({
    coinbase: true,
    label: 'c1',
    outputs: [
      { lockingBytecode: p2pkh('miner'), valueSatoshis: 5_000_000_000n },
    ],
  });
  const a = makeTx({
    label: 'a',
    outputs: [
      {
        fungibleTokenAmount: 400n,
        lockingBytecode: p2pkh('alice'),
        tokenCategory: category,
        valueSatoshis: 800n,
      },
      { lockingBytecode: p2pkh('change'), valueSatoshis: 4_999_998_000n },
    ],
    spends: [[c0.hash, 0]],
  });
  const block1 = makeBlock(1, block0.hash, [c1, a]);
  const c2 = makeTx({
    coinbase: true,
    label: 'c2',
    outputs: [
      { lockingBytecode: p2pkh('miner'), valueSatoshis: 5_000_000_500n },
    ],
  });
  const b = makeTx({
    label: 'b',
    outputs: [
      {
        fungibleTokenAmount: 400n,
        lockingBytecode: p2pkh('bob'),
        tokenCategory: category,
        valueSatoshis: 1_300n,
      },
    ],
    spends: [
      [a.hash, 0],
      [c0.hash, 1],
    ],
  });
  const block2 = makeBlock(2, block1.hash, [c2, b]);
  return { a, b, block0, block1, block2, c0, c1, c2 };
};

/** Definitional unspent(n, o) from the blocks a node accepts. */
const expectedUnspent = (blocks: readonly ChaingraphBlock[]) => {
  const txs = blocks.flatMap((block) => block.transactions);
  const spent = new Set(
    txs
      .filter((tx) => !tx.isCoinbase)
      .flatMap((tx) =>
        tx.inputs.map(
          (input) => `${input.outpointTransactionHash}:${input.outpointIndex}`
        )
      )
  );
  return txs
    .flatMap((tx) => tx.outputs.map((_, index) => `${tx.hash}:${index}`))
    .filter((key) => !spent.has(key))
    .sort();
};

/* -------------------------------------------------------------------- */
/* scratch databases and stores                                          */
/* -------------------------------------------------------------------- */

const applyScript = fileURLToPath(
  new URL('../../../src/store/clickhouse/ddl/apply.sh', import.meta.url)
);

const connectionFor = (database: string) => ({
  database,
  password: process.env.CHAINGRAPH_E2E_CLICKHOUSE_PASSWORD ?? '',
  requestTimeoutMs: 120_000,
  url: e2eClickHouseUrl!,
  username: process.env.CHAINGRAPH_E2E_CLICKHOUSE_USER ?? '',
});

const leaseTtlMs = 1_500;

const scratch = async (t: ExecutionContext, label: string) => {
  const name = `ch1_wp5a_${label}_${randomBytes(4).toString('hex')}`;
  await execFileAsync('bash', [applyScript, e2eClickHouseUrl!, name], {
    env: process.env,
    timeout: 120_000,
  });
  const client = new ClickHouseClient(connectionFor(name));
  const stores: ClickHouseStore[] = [];
  t.teardown(async () => {
    await Promise.all(
      stores.map(async (store) => store.close().catch(() => undefined))
    );
    if (process.env.WP5A_KEEP_DB === undefined) {
      await client.command(`DROP DATABASE IF EXISTS ${name}`);
    }
    await client.close();
  });
  const openStore = async (fault?: FaultInjector) => {
    const store = new ClickHouseStore({
      connection: connectionFor(name),
      fault,
      lease: { safetyMarginMs: 500, settleMs: 20, ttlMs: leaseTtlMs },
      pendingSpendTimeoutMs: 20_000,
      publisher: { minIntervalMs: 10 },
    });
    await store.init();
    stores.push(store);
    return store;
  };
  return { client, name, openStore };
};

const registerNodes = async (store: ClickHouseStore) => {
  const node1 = await store.registerNode({
    latestConnectionBeganAt: new Date('2026-10-09T00:00:00Z'),
    nodeName: 'node-one',
    protocolVersion: 70016,
    userAgent: '/BCHN:28.0.0/',
  });
  const node2 = await store.registerNode({
    latestConnectionBeganAt: new Date('2026-10-09T00:00:00Z'),
    nodeName: 'node-two',
    protocolVersion: 70016,
    userAgent: '/BCHN:28.0.0/',
  });
  return { node1: node1.internalId, node2: node2.internalId };
};

const acceptance = (
  nodeInternalId: number,
  acceptedAt: Date | null = null
) => ({
  acceptedAt,
  nodeInternalId,
  nodeName: `node-${nodeInternalId}`,
});

const notSaved = () => false;

/** What a reader of node n sees, through the pinned `*_at` views of one snapshot. */
const nodeView = async (client: ClickHouseClient, node: number) => {
  const snapshot = await readSnapshot(client, node);
  const params = {
    node,
    tail: snapshot.committedTail,
    visible: snapshot.visible,
    visible0: snapshot.visible0,
  };
  const [blocks, txs, utxo, utxoByScript, history, inputs] = await Promise.all([
    client.query<{ hash: string; height: number; accepted: string | null }>(
      `SELECT lower(hex(block_hash)) AS hash, height, toString(accepted_at) AS accepted
       FROM node_block_at(node = {node:UInt32}, visible = {visible:UInt64}) ORDER BY height`,
      params
    ),
    client.query<{ hash: string; block: string }>(
      `SELECT lower(hex(transaction_hash)) AS hash, toString(block_internal_id) AS block
       FROM tx_acceptance_at(node = {node:UInt32}, visible = {visible:UInt64}) ORDER BY hash`,
      params
    ),
    client.query<{ key: string; amount: string | null; category: string }>(
      `SELECT concat(lower(hex(transaction_hash)), ':', toString(output_index)) AS key,
         fungible_token_amount AS amount, lower(hex(token_category)) AS category
       FROM utxo_at(node = {node:UInt32}, visible = {visible:UInt64}) ORDER BY key`,
      params
    ),
    client.query<{ key: string }>(
      `SELECT concat(lower(hex(transaction_hash)), ':', toString(output_index)) AS key
       FROM utxo_by_script_at(node = {node:UInt32}, visible = {visible:UInt64}) ORDER BY key`,
      params
    ),
    client.query<{ block: string; removed: string }>(
      `SELECT toString(block_internal_id) AS block, toString(removed_at) AS removed
       FROM node_block_history_at(node = {node:UInt32}, visible = {visible:UInt64})`,
      params
    ),
    client.query<{ key: string; amount: string | null; value: string }>(
      `SELECT concat(lower(hex(transaction_hash)), ':', toString(input_index)) AS key,
         fungible_token_amount AS amount, toString(value_satoshis) AS value
       FROM input_at(visible0 = {visible0:UInt64}, tail = {tail:Array(UInt64)}) ORDER BY key`,
      params
    ),
  ]);
  return {
    blockRows: blocks,
    blocks: blocks.map((row) => row.hash),
    history,
    inputs,
    txs: [...new Set(txs.map((row) => row.hash))].sort(),
    utxo: utxo.map((row) => row.key),
    utxoByScript: utxoByScript.map((row) => row.key),
    utxoRows: utxo,
  };
};

/** Every (node, outpoint) sum over valid (non-void) rows is 0 or 1. */
const badUtxoSums = async (client: ClickHouseClient) =>
  client.query<{ node: number; key: string; s: string }>(
    `SELECT node_internal_id AS node, concat(lower(hex(transaction_hash)), ':', toString(output_index)) AS key,
       sum(sign) AS s
     FROM utxo WHERE commit_seq NOT IN (SELECT commit_seq FROM commit_void)
     GROUP BY node, transaction_hash, output_index HAVING s NOT IN (0, 1)`
  );

const txHashes = (blocks: readonly ChaingraphBlock[]) =>
  [
    ...new Set(
      blocks.flatMap((block) => block.transactions.map((tx) => tx.hash))
    ),
  ].sort();

/* -------------------------------------------------------------------- */
/* e2e                                                                    */
/* -------------------------------------------------------------------- */

e2e(
  '[e2e] ClickHouseStore: a 3-block chain for 2 nodes; each node sees exactly its blocks, txs and UTXOs',
  async (t) => {
    const { client, openStore } = await scratch(t, 'chain');
    const store = await openStore();
    const { node1, node2 } = await registerNodes(store);
    const chain = threeBlockChain();
    const liveAt = new Date();
    const first = await store.saveBlock({
      block: chain.block0,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1), acceptance(node2)],
    });
    t.is(first.attemptedSavedTransactions.length, 1);
    t.is(first.transactionCacheMisses, 0);
    await store.saveBlock({
      block: chain.block1,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1), acceptance(node2)],
    });
    await store.saveBlock({
      block: chain.block2,
      isSavedTransaction: (hash) => hash === chain.c2.hash,
      nodeAcceptances: [acceptance(node1, liveAt)],
    });
    await store.publishWatermarks();

    const view1 = await nodeView(client, node1);
    const view2 = await nodeView(client, node2);
    t.deepEqual(view1.blocks, [
      chain.block0.hash,
      chain.block1.hash,
      chain.block2.hash,
    ]);
    t.deepEqual(view2.blocks, [chain.block0.hash, chain.block1.hash]);
    t.is(
      view1.blockRows[2]!.accepted?.startsWith(
        liveAt.toISOString().slice(0, 10)
      ),
      true
    );
    t.is(view1.blockRows[0]!.accepted, null);
    t.deepEqual(
      view1.txs,
      txHashes([chain.block0, chain.block1, chain.block2])
    );
    t.deepEqual(view2.txs, txHashes([chain.block0, chain.block1]));
    t.deepEqual(
      view1.utxo,
      expectedUnspent([chain.block0, chain.block1, chain.block2])
    );
    t.deepEqual(view2.utxo, expectedUnspent([chain.block0, chain.block1]));
    t.deepEqual(view1.utxoByScript, view1.utxo);
    t.deepEqual(view2.utxoByScript, view2.utxo);
    const tokenUtxo = view2.utxoRows.find(
      (row) => row.key === `${chain.a.hash}:0`
    );
    t.is(tokenUtxo?.amount, '400');
    t.is(tokenUtxo?.category, category);
    // inputs carry the spent output's attributes, including the FT amount (WP5a)
    const bInputs = view1.inputs.filter((row) =>
      row.key.startsWith(chain.b.hash)
    );
    t.deepEqual(
      bInputs.map((row) => [row.amount, row.value]),
      [
        ['400', '800'],
        ['1000', '1000'],
      ]
    );
    t.deepEqual(await badUtxoSums(client), []);

    // node-agnostic reads and the Postgres return semantics
    const known = await store.getAllKnownBlockHashes();
    t.deepEqual(
      known.sort(),
      [chain.block0.hash, chain.block1.hash, chain.block2.hash].sort()
    );
    const blockRow = await client.query<{
      generated: string;
      outputs: string;
      count: number;
    }>(
      `SELECT toString(generated_value_satoshis) AS generated, toString(output_value_satoshis) AS outputs,
       transaction_count AS count FROM block WHERE hash = toFixedString(unhex({hash:String}), 32)`,
      { hash: chain.block2.hash }
    );
    t.deepEqual(blockRow[0], {
      count: 2,
      generated: '5000000000',
      outputs: '5000001800',
    });
    const again = await store.saveBlock({
      block: chain.block2,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1)],
    });
    t.is(
      again.transactionCacheMisses,
      2,
      'all txs already stored: Postgres counts cache misses'
    );
    t.deepEqual((await nodeView(client, node1)).utxo, view1.utxo);

    // registerNode restores each node's chain from its committed node_block rows
    const restored = await store.registerNode({
      latestConnectionBeganAt: new Date(),
      nodeName: 'node-two',
      protocolVersion: 70016,
      userAgent: '/BCHN:28.0.0/',
    });
    t.is(restored.internalId, node2);
    t.deepEqual(restored.syncedHeaderHashChain, [
      chain.block0.hash,
      chain.block1.hash,
    ]);
    const incomplete = await store.getIncompleteBlocks({
      excludedBlockHashes: [],
      heightLowerBound: 0,
      heightUpperBound: 10,
      limit: 10,
      nodeInternalIds: [node1, node2],
    });
    t.deepEqual(incomplete, { incompleteBlocks: [], scannedBlockCount: 3 });
    const asStore: ChaingraphStore = store;
    await t.throwsAsync(asStore.saveMempoolTransaction(chain.a, []), {
      instanceOf: MempoolNotImplementedError,
    });
    t.deepEqual(await store.enableMempoolTracking(), { schemaIsCurrent: true });
    t.deepEqual(Object.keys(store.poolStats()).sort(), [
      'clients',
      'waitingRequests',
    ]);
  }
);

e2e(
  '[e2e] ClickHouseStore: a re-org of node 1 leaves node 2 untouched; re-acceptance restores node 1',
  async (t) => {
    const { client, openStore } = await scratch(t, 'reorg');
    const store = await openStore();
    const { node1, node2 } = await registerNodes(store);
    const chain = threeBlockChain();
    for (const block of [chain.block0, chain.block1, chain.block2]) {
      await store.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(node1), acceptance(node2)],
      });
    }
    await store.publishWatermarks();
    const node2Before = await nodeView(client, node2);
    const removedAt = new Date('2026-10-09T12:00:00.000Z');
    await store.removeStaleBlocksForNode(node1, [chain.block2.hash], removedAt);
    await store.publishWatermarks();
    const node1After = await nodeView(client, node1);
    const node2After = await nodeView(client, node2);
    t.deepEqual(
      node2After,
      node2Before,
      'node 2 is untouched by node 1 re-org'
    );
    t.deepEqual(node1After.blocks, [chain.block0.hash, chain.block1.hash]);
    t.deepEqual(node1After.txs, txHashes([chain.block0, chain.block1]));
    t.deepEqual(node1After.utxo, expectedUnspent([chain.block0, chain.block1]));
    t.deepEqual(node1After.utxoByScript, node1After.utxo);
    t.is(node1After.history.length, 1);
    t.is(node1After.history[0]!.removed, '2026-10-09 12:00:00.000');
    t.deepEqual(node2After.history, []);
    t.deepEqual(await badUtxoSums(client), []);
    // releasing a block the node no longer accepts is a no-op
    await store.removeStaleBlocksForNode(node1, [
      chain.block2.hash,
      sha('unknown'),
    ]);
    t.deepEqual((await nodeView(client, node1)).history.length, 1);

    // a competing block 2' re-includes b (as re-org blocks do), then node 1 re-orgs back
    const c2alt = makeTx({
      coinbase: true,
      label: 'c2-alt',
      outputs: [
        {
          lockingBytecode: p2pkh('other-miner'),
          valueSatoshis: 5_000_000_500n,
        },
      ],
    });
    const block2alt = makeBlock(
      2,
      chain.block1.hash,
      [c2alt, chain.b],
      'block-2-alt'
    );
    const altResult = await store.saveBlock({
      block: block2alt,
      isSavedTransaction: (hash) => hash === chain.b.hash,
      nodeAcceptances: [acceptance(node1)],
    });
    t.is(altResult.attemptedSavedTransactions.length, 1);
    await store.publishWatermarks();
    const onAlt = await nodeView(client, node1);
    t.deepEqual(onAlt.blocks, [
      chain.block0.hash,
      chain.block1.hash,
      block2alt.hash,
    ]);
    t.deepEqual(
      onAlt.utxo,
      expectedUnspent([chain.block0, chain.block1, block2alt])
    );
    await store.removeStaleBlocksForNode(node1, [block2alt.hash]);
    t.is(
      await store.acceptBlocksViaHeaders(
        node1,
        [{ hash: chain.block2.hash, height: 2 }],
        new Date()
      ),
      1
    );
    await store.publishWatermarks();
    const back = await nodeView(client, node1);
    t.deepEqual(back.blocks, [
      chain.block0.hash,
      chain.block1.hash,
      chain.block2.hash,
    ]);
    t.deepEqual(
      back.utxo,
      expectedUnspent([chain.block0, chain.block1, chain.block2])
    );
    t.deepEqual(back.utxoByScript, back.utxo);
    t.is(back.history.length, 2);
    // node 2's facts are untouched (input_at is node-agnostic: block 2' added inputs)
    t.deepEqual(
      { ...(await nodeView(client, node2)), inputs: [] },
      { ...node2Before, inputs: [] }
    );
    t.deepEqual(await badUtxoSums(client), []);
    const restored = await store.registerNode({
      latestConnectionBeganAt: new Date(),
      nodeName: 'node-one',
      protocolVersion: 70016,
      userAgent: '/BCHN:28.0.0/',
    });
    t.deepEqual(restored.syncedHeaderHashChain, [
      chain.block0.hash,
      chain.block1.hash,
      chain.block2.hash,
    ]);
  }
);

e2e(
  '[e2e] ClickHouseStore: header acceptance by node 2 of blocks node 1 stored',
  async (t) => {
    const { client, openStore } = await scratch(t, 'headers');
    const store = await openStore();
    const { node1, node2 } = await registerNodes(store);
    const chain = threeBlockChain();
    const blocks = [chain.block0, chain.block1, chain.block2];
    for (const block of blocks) {
      await store.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(node1)],
      });
    }
    await store.publishWatermarks();
    const node1Before = await nodeView(client, node1);
    t.deepEqual((await nodeView(client, node2)).blocks, []);
    // block 2 is "recent": accepted_at kept; older blocks get NULL (Postgres rule)
    const acceptedAt = new Date((chain.block2.timestamp + 7_200 - 300) * 1_000);
    const accepted = await store.acceptBlocksViaHeaders(
      node2,
      blocks.map((block) => ({ hash: block.hash, height: block.height })),
      acceptedAt
    );
    t.is(accepted, 3);
    await store.publishWatermarks();
    const view2 = await nodeView(client, node2);
    t.deepEqual(
      view2.blocks,
      blocks.map((block) => block.hash)
    );
    t.deepEqual(
      view2.blockRows.map((row) => row.accepted !== null),
      [false, false, true]
    );
    t.deepEqual(view2.txs, txHashes(blocks));
    t.deepEqual(view2.utxo, expectedUnspent(blocks));
    t.deepEqual(view2.utxo, node1Before.utxo);
    t.deepEqual(view2.utxoByScript, view2.utxo);
    t.deepEqual(
      await nodeView(client, node1),
      node1Before,
      'node 1 is untouched'
    );
    t.is(
      await store.acceptBlocksViaHeaders(
        node2,
        [{ hash: chain.block1.hash, height: 1 }],
        acceptedAt
      ),
      0,
      'already accepted: nothing inserted'
    );
    t.deepEqual(await badUtxoSums(client), []);
  }
);

e2e(
  '[e2e] ClickHouseStore: child-before-parent across two concurrent saves; no torn read, exact final state',
  async (t) => {
    t.timeout(120_000);
    const { client, openStore } = await scratch(t, 'child');
    const chain = threeBlockChain();
    let releaseParent: (() => void) | undefined;
    const childIncomplete = new Promise<void>((resolve) => {
      releaseParent = resolve;
    });
    const steps: string[] = [];
    const store = await openStore(async (step, context) => {
      steps.push(`${context.kind}:${step}`);
      if (step === 'incomplete') {
        releaseParent?.();
      }
      // slow every insert down so the poller sees intermediate states
      await new Promise((resolve) => {
        setTimeout(resolve, 15);
      });
    });
    const { node1, node2 } = await registerNodes(store);
    await store.saveBlock({
      block: chain.block0,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1), acceptance(node2)],
    });
    await store.publishWatermarks();

    const poll = { running: true };
    const torn: string[] = [];
    let polls = 0;
    const poller = (async () => {
      while (poll.running) {
        for (const node of [node1, node2]) {
          const view = await nodeView(client, node);
          polls += 1;
          const hasChild = view.blocks.includes(chain.block2.hash);
          const hasParent = view.blocks.includes(chain.block1.hash);
          const accepted = [
            chain.block0,
            ...(hasParent ? [chain.block1] : []),
            ...(hasChild ? [chain.block2] : []),
          ];
          if (hasChild && !hasParent)
            torn.push(`node ${node}: child without parent`);
          if (JSON.stringify(view.txs) !== JSON.stringify(txHashes(accepted))) {
            torn.push(`node ${node}: txs ${view.txs.length}`);
          }
          if (
            JSON.stringify(view.utxo) !==
            JSON.stringify(expectedUnspent(accepted))
          ) {
            torn.push(`node ${node}: utxo ${view.utxo.length}`);
          }
          if (
            hasChild &&
            view.inputs.filter((row) => row.key.startsWith(chain.b.hash))
              .length !== 2
          ) {
            torn.push(`node ${node}: child inputs missing`);
          }
        }
      }
    })();

    // child (block 2) first: it spends a:0, which only block 1 creates
    const child = store.saveBlock({
      block: chain.block2,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1), acceptance(node2)],
    });
    await childIncomplete;
    const incompleteRows = await client.query<{ c: string }>(
      "SELECT count() AS c FROM commit_log WHERE state = 'incomplete'"
    );
    t.is(
      incompleteRows[0]?.c,
      '1',
      'the child commit is incomplete while it waits'
    );
    const parent = store.saveBlock({
      block: chain.block1,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1), acceptance(node2)],
    });
    await Promise.all([child, parent]);
    await store.publishWatermarks();
    await new Promise((resolve) => {
      setTimeout(resolve, 100);
    });
    poll.running = false;
    await poller;
    t.true(polls > 4, `poller ran (${polls} reads)`);
    t.deepEqual(torn, []);
    t.true(steps.includes('block:fill'));
    for (const node of [node1, node2]) {
      const view = await nodeView(client, node);
      const blocks = [chain.block0, chain.block1, chain.block2];
      t.deepEqual(
        view.blocks,
        blocks.map((block) => block.hash)
      );
      t.deepEqual(view.utxo, expectedUnspent(blocks));
      t.deepEqual(view.utxoByScript, view.utxo);
      t.deepEqual(
        view.inputs
          .filter((row) => row.key.startsWith(chain.b.hash))
          .map((row) => [row.amount, row.value]),
        [
          ['400', '800'],
          ['1000', '1000'],
        ]
      );
    }
    const pending = await client.query<{ s: string }>(
      'SELECT sum(sign) AS s FROM pending_spend'
    );
    t.is(pending[0]?.s, '0', 'pending spends were filled under the child seq');
    t.deepEqual(await badUtxoSums(client), []);
  }
);

const blockSteps = [
  'intent',
  'output',
  'input',
  'transaction',
  'block',
  'block_transaction',
  'node_block',
  'tx_acceptance',
  'utxo',
  'utxo_by_script',
  'rows-written',
  'committed',
];

e2e(
  '[e2e] ClickHouseStore: crash between any two steps of a block save, re-org or header acceptance leaves no partial node facts',
  async (t) => {
    t.timeout(300_000);
    const { client, openStore } = await scratch(t, 'crash');
    const chain = threeBlockChain();
    const setup = await openStore();
    const { node1, node2 } = await registerNodes(setup);
    for (const block of [chain.block0, chain.block1]) {
      await setup.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(node1), acceptance(node2)],
      });
    }
    await setup.close();
    const base = [chain.block0, chain.block1];
    const full = [...base, chain.block2];

    const scenarios: {
      name: string;
      steps: string[];
      run: (store: ClickHouseStore) => Promise<unknown>;
      after: Map<number, readonly ChaingraphBlock[]>;
      undo: (store: ClickHouseStore) => Promise<unknown>;
    }[] = [
      {
        after: new Map([
          [node1, full],
          [node2, full],
        ]),
        name: 'block',
        run: async (store) =>
          store.saveBlock({
            block: chain.block2,
            isSavedTransaction: notSaved,
            nodeAcceptances: [acceptance(node1), acceptance(node2)],
          }),
        steps: blockSteps,
        undo: async (store) => {
          await store.removeStaleBlocksForNode(node1, [chain.block2.hash]);
          await store.removeStaleBlocksForNode(node2, [chain.block2.hash]);
        },
      },
      {
        after: new Map([
          [node1, base],
          [node2, full],
        ]),
        name: 'reorg',
        run: async (store) =>
          store.removeStaleBlocksForNode(node1, [chain.block2.hash]),
        steps: [
          'intent',
          'node_block',
          'node_block_history',
          'utxo',
          'tx_acceptance',
          'committed',
        ],
        undo: async (store) =>
          store.acceptBlocksViaHeaders(
            node1,
            [{ hash: chain.block2.hash, height: 2 }],
            new Date()
          ),
      },
      {
        after: new Map([
          [node1, full],
          [node2, full],
        ]),
        name: 'header_accept',
        run: async (store) =>
          store.acceptBlocksViaHeaders(
            node1,
            [{ hash: chain.block2.hash, height: 2 }],
            new Date()
          ),
        steps: ['intent', 'node_block', 'utxo', 'tx_acceptance', 'committed'],
        undo: async (store) =>
          store.removeStaleBlocksForNode(node1, [chain.block2.hash]),
      },
    ];

    // the state before each scenario: block 2 saved for node 2 only after the first scenario
    let before = new Map<number, readonly ChaingraphBlock[]>([
      [node1, base],
      [node2, base],
    ]);
    for (const scenario of scenarios) {
      for (const crashAt of scenario.steps) {
        let crashedSeq: bigint | undefined;
        const crashing = await openStore((step, context) => {
          if (context.kind !== scenario.name) return;
          if (step === crashAt) {
            crashedSeq = context.seq;
            throw new SimulatedCrash(`crash after ${scenario.name}:${step}`);
          }
        });
        await t.throwsAsync(scenario.run(crashing), {
          instanceOf: SimulatedCrash,
        });
        await crashing.simulateCrash();
        const label = `${scenario.name} crash after ${crashAt}`;
        /*
         * before recovery: a node sees the whole save or none of it (a commit
         * that reached 'committed' may have been published before the crash)
         */
        for (const [node, blocks] of before) {
          const view = await nodeView(client, node);
          const candidates =
            crashAt === 'committed'
              ? [blocks, scenario.after.get(node)!]
              : [blocks];
          t.true(
            candidates.some(
              (option) =>
                JSON.stringify(view.blocks) ===
                  JSON.stringify(option.map((block) => block.hash)) &&
                JSON.stringify(view.txs) === JSON.stringify(txHashes(option)) &&
                JSON.stringify(view.utxo) ===
                  JSON.stringify(expectedUnspent(option))
            ),
            `${label}: node ${node} before recovery: ${view.blocks.length} blocks, ${view.utxo.length} utxos`
          );
        }
        await new Promise((resolve) => {
          setTimeout(resolve, leaseTtlMs + 100);
        });
        const recovered = await openStore();
        const committed = crashAt === 'committed';
        const expected = committed ? scenario.after : before;
        for (const [node, blocks] of expected) {
          const view = await nodeView(client, node);
          t.deepEqual(
            view.blocks,
            blocks.map((block) => block.hash),
            `${label}: node ${node} blocks after recovery`
          );
          t.deepEqual(
            view.txs,
            txHashes(blocks),
            `${label}: node ${node} txs after recovery`
          );
          t.deepEqual(
            view.utxo,
            expectedUnspent(blocks),
            `${label}: node ${node} utxo after recovery`
          );
          t.deepEqual(
            view.utxoByScript,
            view.utxo,
            `${label}: node ${node} utxo_by_script after recovery`
          );
        }
        if (crashedSeq !== undefined) {
          const states = await recovered.commitLog.listCommits({
            fromSeq: crashedSeq,
            toSeq: crashedSeq,
          });
          t.is(
            states[0]?.state,
            committed ? 'committed' : 'aborted',
            `${label}: commit state`
          );
        }
        t.deepEqual(await badUtxoSums(client), [], `${label}: utxo sums`);
        if (committed) {
          // return to the state before the scenario for the next crash point
          await scenario.undo(recovered);
          await recovered.publishWatermarks();
        }
        await recovered.close();
      }
      before = scenario.after;
      if (scenario.name !== 'header_accept') {
        const store = await openStore();
        await scenario.run(store);
        await store.close();
      }
    }
  }
);

e2e(
  '[e2e] ClickHouseStore: bulk horizon — no inline UTXO rows during initial sync, one exact build at finishInitialSync',
  async (t) => {
    t.timeout(120_000);
    const { client, openStore } = await scratch(t, 'bulk');
    const chain = threeBlockChain();
    let store = await openStore();
    const { node1, node2 } = await registerNodes(store);
    // block 0 above the horizon (tip mode: inline rows), then bulk mode
    await store.saveBlock({
      block: chain.block0,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1), acceptance(node2)],
    });
    t.false(await store.prepareForInitialSync());
    t.is(store.storeMode, 'bulk');
    for (const block of [chain.block1, chain.block2]) {
      await store.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(node1), acceptance(node2)],
      });
    }
    // a re-org during bulk releases block 2 for node 2 (no inline inverse rows either)
    await store.removeStaleBlocksForNode(node2, [chain.block2.hash]);
    await store.publishWatermarks();
    const duringBulk = await nodeView(client, node1);
    t.deepEqual(
      duringBulk.utxo,
      expectedUnspent([chain.block0]),
      'utxo lags acceptance in bulk mode'
    );
    // a restart mid-sync resumes bulk mode
    await store.close();
    store = await openStore();
    t.is(store.storeMode, 'bulk');
    t.false(await store.prepareForInitialSync());
    const progress: string[] = [];
    await store.finishInitialSync({
      onIndexProgress: (items) => {
        progress.push(...items.map(([name, pct]) => `${name}:${pct}`));
      },
      onNonFatalError: (error) => {
        throw error;
      },
      onSyncSettingsRestored: () => undefined,
    });
    t.is(store.storeMode, 'tip');
    t.true(progress.length > 0);
    const view1 = await nodeView(client, node1);
    const view2 = await nodeView(client, node2);
    t.deepEqual(
      view1.utxo,
      expectedUnspent([chain.block0, chain.block1, chain.block2])
    );
    t.deepEqual(view2.utxo, expectedUnspent([chain.block0, chain.block1]));
    t.deepEqual(view1.utxoByScript, view1.utxo);
    t.deepEqual(view2.utxoByScript, view2.utxo);
    t.deepEqual(await badUtxoSums(client), []);
    // tip mode again: node 2 re-accepts block 2 inline
    await store.acceptBlocksViaHeaders(
      node2,
      [{ hash: chain.block2.hash, height: 2 }],
      new Date()
    );
    await store.publishWatermarks();
    t.deepEqual((await nodeView(client, node2)).utxo, view1.utxo);
    // the next start's bulk period builds only its own transitions
    await store.close();
    store = await openStore();
    t.is(store.storeMode, 'tip');
    await store.prepareForInitialSync();
    await store.finishInitialSync({
      onIndexProgress: () => undefined,
      onNonFatalError: () => undefined,
      onSyncSettingsRestored: () => undefined,
    });
    t.deepEqual((await nodeView(client, node1)).utxo, view1.utxo);
    t.deepEqual(await badUtxoSums(client), []);
  }
);

e2e(
  '[e2e] ClickHouseStore: merges never collapse an uncommitted or aborted −1 with a committed +1',
  async (t) => {
    t.timeout(60_000);
    const { client, openStore } = await scratch(t, 'merge');
    const chain = threeBlockChain();
    const setup = await openStore();
    const { node1, node2 } = await registerNodes(setup);
    for (const block of [chain.block0, chain.block1, chain.block2]) {
      await setup.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(node1), acceptance(node2)],
      });
    }
    await setup.close();
    const full = [chain.block0, chain.block1, chain.block2];
    // a re-org of node 1 writes all its rows, then the writer dies before 'committed'
    const crashing = await openStore((step, context) => {
      if (context.kind === 'reorg' && step === 'tx_acceptance') {
        throw new SimulatedCrash('crash before committed');
      }
    });
    await t.throwsAsync(
      crashing.removeStaleBlocksForNode(node1, [chain.block2.hash]),
      { instanceOf: SimulatedCrash }
    );
    await crashing.simulateCrash();
    // force every merge now: the −1 rows must not take the committed +1 rows with them
    for (const table of [
      'node_block',
      'tx_acceptance',
      'utxo',
      'utxo_by_script',
    ]) {
      await client.command(`OPTIMIZE TABLE ${table} FINAL`);
    }
    const during = await nodeView(client, node1);
    t.deepEqual(
      during.blocks,
      full.map((block) => block.hash)
    );
    t.deepEqual(during.utxo, expectedUnspent(full));
    await new Promise((resolve) => {
      setTimeout(resolve, leaseTtlMs + 100);
    });
    const recovered = await openStore();
    for (const table of [
      'node_block',
      'tx_acceptance',
      'utxo',
      'utxo_by_script',
    ]) {
      await client.command(`OPTIMIZE TABLE ${table} FINAL`);
    }
    const after = await nodeView(client, node1);
    t.deepEqual(
      after.blocks,
      full.map((block) => block.hash)
    );
    t.deepEqual(after.txs, txHashes(full));
    t.deepEqual(after.utxo, expectedUnspent(full));
    t.deepEqual(after.utxoByScript, after.utxo);
    // the committed re-org, merged, is exact too
    await recovered.removeStaleBlocksForNode(node1, [chain.block2.hash]);
    await recovered.publishWatermarks();
    for (const table of [
      'node_block',
      'tx_acceptance',
      'utxo',
      'utxo_by_script',
    ]) {
      await client.command(`OPTIMIZE TABLE ${table} FINAL`);
    }
    const released = await nodeView(client, node1);
    t.deepEqual(released.blocks, [chain.block0.hash, chain.block1.hash]);
    t.deepEqual(released.utxo, expectedUnspent([chain.block0, chain.block1]));
    t.deepEqual((await nodeView(client, node2)).utxo, expectedUnspent(full));
    t.deepEqual(await badUtxoSums(client), []);
  }
);

e2e(
  '[e2e] ClickHouseStore: getIncompleteBlocks reports a block whose commit was aborted, until it is saved again',
  async (t) => {
    t.timeout(60_000);
    const { openStore } = await scratch(t, 'incomplete');
    const chain = threeBlockChain();
    const setup = await openStore();
    const { node1 } = await registerNodes(setup);
    for (const block of [chain.block0, chain.block1]) {
      await setup.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(node1)],
      });
    }
    await setup.close();
    const crashing = await openStore((step, context) => {
      if (context.kind === 'block' && step === 'node_block') {
        throw new SimulatedCrash('crash mid-save');
      }
    });
    await t.throwsAsync(
      crashing.saveBlock({
        block: chain.block2,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(node1)],
      }),
      { instanceOf: SimulatedCrash }
    );
    await crashing.simulateCrash();
    await new Promise((resolve) => {
      setTimeout(resolve, leaseTtlMs + 100);
    });
    const store = await openStore();
    const scan = async () =>
      store.getIncompleteBlocks({
        excludedBlockHashes: [],
        heightLowerBound: 0,
        heightUpperBound: 10,
        limit: 10,
        nodeInternalIds: [node1],
      });
    t.deepEqual(await scan(), {
      incompleteBlocks: [
        {
          hash: chain.block2.hash,
          height: 2,
          linkedSizeBytes: 0,
          sizeBytes: chain.block2.sizeBytes,
          transactionCount: 0,
        },
      ],
      scannedBlockCount: 3,
    });
    t.false((await store.getAllKnownBlockHashes()).includes(chain.block2.hash));
    await store.saveBlock({
      block: chain.block2,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1)],
    });
    t.deepEqual(await scan(), { incompleteBlocks: [], scannedBlockCount: 3 });
    t.true((await store.getAllKnownBlockHashes()).includes(chain.block2.hash));
  }
);
