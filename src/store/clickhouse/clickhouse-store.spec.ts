/* eslint-disable max-lines, @typescript-eslint/no-magic-numbers, functional/no-loop-statement, functional/no-let, no-await-in-loop, complexity, functional/no-throw-statement, @typescript-eslint/require-array-sort-compare, @typescript-eslint/init-declarations, @typescript-eslint/no-loop-func */
// cspell:ignore clickhouse unhex aabb varint seqs

import test from 'ava';

import type { ChaingraphBlock } from '../../types/chaingraph.js';
import type { ChaingraphStore } from '../types.js';

import { SimulatedCrash } from './block-commit.js';
import type { ClickHouseStore } from './clickhouse-store.js';
import { hashChainFromBlocks, linkedBlockSize } from './clickhouse-store.js';
import {
  acceptance,
  badUtxoSums,
  category,
  expectedUnspent,
  leaseTtlMs,
  makeBlock,
  makeTx,
  nodeView,
  notSaved,
  p2pkh,
  registerNodes,
  scratch,
  sha,
  threeBlockChain,
  txHashes,
  zeroHash,
} from './spec-fixtures.js';
import { e2eClickHouseUrl } from './test-support.js';
import { nodeViewParams, pinnedView, readSnapshot } from './visibility.js';

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
    // a known transaction with no validations changes nothing (Postgres: ON CONFLICT)
    await t.notThrowsAsync(asStore.saveMempoolTransaction(chain.a, []));
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

e2e(
  '[e2e] ClickHouseStore: in-flight cap 2, blocks N+2, N+1, N saved concurrently in that order complete; final state exact',
  async (t) => {
    t.timeout(120_000);
    const cap = 2;
    let peakActive = 0;
    let peakWaiting = 0;
    let overCap = 0;
    const holder: { store?: ClickHouseStore } = {};
    const { client, openStore } = await scratch(
      t,
      'cap',
      /*
       * a child that waited out the timeout would be stored with stand-in
       * inputs: keep it far above the completion bound below
       */
      {
        maxBlocksPerCommit: 1,
        maxInFlightSaves: cap,
        pendingSpendTimeoutMs: 90_000,
        // one commit per block, all three working at once (non-batched store)
        runningBatchesPerNodeSet: 3,
      }
    );
    const sample = () => {
      const stats = holder.store?.poolStats();
      if (stats !== undefined) {
        peakActive = Math.max(peakActive, stats.clients.active);
        peakWaiting = Math.max(peakWaiting, stats.waitingRequests);
        if (stats.clients.active > cap) overCap += 1;
      }
    };
    const store = await openStore(async () => {
      sample();
      // slow every step so the three saves overlap
      await new Promise((resolve) => {
        setTimeout(resolve, 10);
      });
    });
    holder.store = store;
    t.is(store.poolStats().clients.max, cap);
    const { node1, node2 } = await registerNodes(store);
    const chain = threeBlockChain();
    const d = makeTx({
      label: 'd',
      outputs: [
        {
          fungibleTokenAmount: 400n,
          lockingBytecode: p2pkh('dave'),
          tokenCategory: category,
          valueSatoshis: 1_300n,
        },
      ],
      spends: [[chain.b.hash, 0]],
    });
    const c3 = makeTx({
      coinbase: true,
      label: 'c3',
      outputs: [
        { lockingBytecode: p2pkh('miner'), valueSatoshis: 5_000_000_000n },
      ],
    });
    const block3 = makeBlock(3, chain.block2.hash, [c3, d]);
    const both = [acceptance(node1), acceptance(node2)];
    await store.saveBlock({
      block: chain.block0,
      isSavedTransaction: notSaved,
      nodeAcceptances: both,
    });

    // N+2, N+1, N: each child is called (and queued) before its parent
    const saves = [block3, chain.block2, chain.block1].map(async (block) =>
      store.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: both,
      })
    );
    const sampler = setInterval(sample, 1);
    let timer: ReturnType<typeof setTimeout> | undefined;
    const outcome = await Promise.race([
      (async () => {
        await Promise.all(saves);
        // a parked child reports early; wait for every commit
        await store.operations.drain();
        return 'completed';
      })(),
      new Promise<string>((resolve) => {
        timer = setTimeout(() => {
          resolve('stuck');
        }, 30_000);
      }),
    ]);
    clearTimeout(timer);
    clearInterval(sampler);
    t.is(outcome, 'completed', 'no deadlock under the cap');
    t.is(overCap, 0, 'never more than the cap working at once');
    t.is(peakActive, cap, `peak active ${peakActive}`);
    t.true(peakWaiting >= 1, `a call queued for a slot (peak ${peakWaiting})`);
    t.deepEqual(store.poolStats(), {
      clients: { active: 0, max: cap, total: 0 },
      waitingRequests: 0,
    });

    await store.publishWatermarks();
    const blocks = [chain.block0, chain.block1, chain.block2, block3];
    for (const node of [node1, node2]) {
      const view = await nodeView(client, node);
      t.deepEqual(
        view.blocks,
        blocks.map((block) => block.hash)
      );
      t.deepEqual(view.txs, txHashes(blocks));
      t.deepEqual(view.utxo, expectedUnspent(blocks));
      t.deepEqual(view.utxoByScript, view.utxo);
      // resolved spent outputs, never the timeout's stand-in (value 0)
      t.deepEqual(
        view.inputs
          .filter(
            (row) =>
              row.key.startsWith(chain.b.hash) || row.key.startsWith(d.hash)
          )
          .map((row) => [row.key.slice(0, 4), row.amount, row.value])
          .sort(),
        [
          [chain.b.hash.slice(0, 4), '1000', '1000'],
          [chain.b.hash.slice(0, 4), '400', '800'],
          [d.hash.slice(0, 4), '400', '1300'],
        ].sort()
      );
    }
    const states = await client.query<{ state: string; c: string }>(
      `SELECT state, count() AS c FROM (SELECT argMax(state, state_rank) AS state FROM commit_log
         WHERE kind = 'block' GROUP BY commit_seq) GROUP BY state ORDER BY state`
    );
    t.deepEqual(
      states.map((row) => [row.state, row.c]),
      [['committed', '4']],
      'four block commits, none aborted'
    );
    const pending = await client.query<{ s: string }>(
      'SELECT sum(sign) AS s FROM pending_spend'
    );
    t.is(pending[0]?.s ?? '0', '0');
    t.deepEqual(await badUtxoSums(client), []);
  }
);

e2e(
  '[e2e] ClickHouseStore: in-flight cap 1, a parked child outlives its pending timeout while its parent is queued for a slot: no stand-in',
  async (t) => {
    t.timeout(120_000);
    const slow = { on: false };
    let childParked: (() => void) | undefined;
    const parked = new Promise<void>((resolve) => {
      childParked = resolve;
    });
    const { client, openStore } = await scratch(t, 'rearm', {
      maxInFlightSaves: 1,
      pendingSpendTimeoutMs: 300,
    });
    const store = await openStore(async (step) => {
      if (step === 'incomplete') childParked?.();
      if (slow.on) {
        await new Promise((resolve) => {
          setTimeout(resolve, 150);
        });
      }
    });
    const { node1, node2 } = await registerNodes(store);
    const node3 = (
      await store.registerNode({
        latestConnectionBeganAt: new Date('2026-10-09T00:00:00Z'),
        nodeName: 'node-three',
        protocolVersion: 70016,
        userAgent: '/BCHN:28.0.0/',
      })
    ).internalId;
    const chain = threeBlockChain();
    const both = [acceptance(node1), acceptance(node2)];
    await store.saveBlock({
      block: chain.block0,
      isSavedTransaction: notSaved,
      nodeAcceptances: both,
    });
    // the child parks (incomplete) and gives its slot up
    const child = store.saveBlock({
      block: chain.block2,
      isSavedTransaction: notSaved,
      nodeAcceptances: both,
    });
    await parked;
    // an unrelated slow save (node 3 only) holds the only slot for > 1 s
    slow.on = true;
    const other = makeBlock(
      0,
      zeroHash,
      [
        makeTx({
          coinbase: true,
          label: 'other-c0',
          outputs: [
            { lockingBytecode: p2pkh('other'), valueSatoshis: 5_000_000_000n },
          ],
        }),
      ],
      'other-0'
    );
    const holder = store.saveBlock({
      block: other,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node3)],
    });
    // the parent is queued behind it for several pending timeouts
    const parent = store.saveBlock({
      block: chain.block1,
      isSavedTransaction: notSaved,
      nodeAcceptances: both,
    });
    await new Promise((resolve) => {
      setTimeout(resolve, 50);
    });
    /*
     * the parent waits for a slot; it registered its outputs when it was
     * called (WP6b), so the child already resumed and waits for one too
     */
    t.true(
      store.poolStats().waitingRequests >= 1,
      'the parent waits for a slot'
    );
    await Promise.all([child, holder, parent]);
    await store.operations.drain();
    slow.on = false;
    await store.publishWatermarks();
    const blocks = [chain.block0, chain.block1, chain.block2];
    for (const node of [node1, node2]) {
      const view = await nodeView(client, node);
      t.deepEqual(
        view.blocks,
        blocks.map((block) => block.hash)
      );
      t.deepEqual(view.utxo, expectedUnspent(blocks));
      t.deepEqual(
        view.inputs
          .filter((row) => row.key.startsWith(chain.b.hash))
          .map((row) => [row.amount, row.value]),
        [
          ['400', '800'],
          ['1000', '1000'],
        ],
        'the spent output was resolved, not the stand-in'
      );
    }
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

/* -------------------------------------------------------------------- */
/* WP6b: write path at scale (docs/clickhouse-port/wp6b-write-path.md)   */
/* -------------------------------------------------------------------- */

/**
 * Two blocks with `txCount × outputsPerTx` outputs each; every output of
 * block 1 is spent in block 2 (block 2: 150k inputs, 150k outputs, so a
 * tip-mode commit with 300k UTXO rows).
 */
const wideChain = (txCount: number, outputsPerTx: number) => {
  const c0 = makeTx({
    coinbase: true,
    label: 'wide-c0',
    outputs: Array.from({ length: txCount }, (_, index) => ({
      lockingBytecode: p2pkh(`wide-miner-${index}`),
      valueSatoshis: 1_000_000_000n,
    })),
  });
  const block0 = makeBlock(0, zeroHash, [c0], 'wide-block-0');
  const fanOut = Array.from({ length: txCount }, (_, txIndex) =>
    makeTx({
      label: `wide-fan-${txIndex}`,
      outputs: Array.from({ length: outputsPerTx }, (__, index) => ({
        lockingBytecode: p2pkh(`wide-${index % 97}`),
        valueSatoshis: 1_000n + BigInt(index),
      })),
      spends: [[c0.hash, txIndex]],
    })
  );
  const c1 = makeTx({
    coinbase: true,
    label: 'wide-c1',
    outputs: [{ lockingBytecode: p2pkh('miner'), valueSatoshis: 1n }],
  });
  const block1 = makeBlock(1, block0.hash, [c1, ...fanOut], 'wide-block-1');
  const fanIn = fanOut.map((parent, txIndex) =>
    makeTx({
      label: `wide-next-${txIndex}`,
      outputs: Array.from({ length: outputsPerTx }, (__, index) => ({
        lockingBytecode: p2pkh(`wide-next-${index % 89}`),
        valueSatoshis: 500n + BigInt(index),
      })),
      spends: parent.outputs.map((__, index): [string, number] => [
        parent.hash,
        index,
      ]),
    })
  );
  const c2 = makeTx({
    coinbase: true,
    label: 'wide-c2',
    outputs: [{ lockingBytecode: p2pkh('miner'), valueSatoshis: 1n }],
  });
  const block2 = makeBlock(2, block1.hash, [c2, ...fanIn], 'wide-block-2');
  return { block0, block1, block2 };
};

const utxoCountAt = async (
  client: Parameters<typeof nodeView>[0],
  node: number
) => {
  const snapshot = await readSnapshot(client, node);
  const rows = await client.query<{ n: string; s: string }>(
    `SELECT toString(count()) AS n, toString(sum(value_satoshis)) AS s
     FROM ${pinnedView('utxo_at')}`,
    nodeViewParams(snapshot)
  );
  return rows[0]!;
};

e2e(
  '[e2e] ClickHouseStore: a block with 150k inputs and 150k outputs (300k UTXO rows) commits in tip mode',
  async (t) => {
    t.timeout(300_000);
    const { client, openStore } = await scratch(t, 'wide');
    const store = await openStore();
    const { node1 } = await registerNodes(store);
    const { block0, block1, block2 } = wideChain(50, 3_000);
    for (const block of [block0, block1, block2]) {
      await store.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(node1)],
      });
    }
    await store.publishWatermarks();
    const unspent = await utxoCountAt(client, node1);
    // block 2's 150k outputs and the two 1-sat coinbase outputs; everything else is spent
    t.is(unspent.n, String(150_000 + 2));
    t.deepEqual(await badUtxoSums(client), []);
  }
);

e2e(
  '[e2e] ClickHouseStore: a child block with 150k unresolved spends parks, its parent completes it',
  async (t) => {
    t.timeout(300_000);
    const { client, openStore } = await scratch(t, 'wide_child');
    const store = await openStore();
    const { node1 } = await registerNodes(store);
    const { block0, block1, block2 } = wideChain(50, 3_000);
    await store.saveBlock({
      block: block0,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1)],
    });
    const started = Date.now();
    // child first: it parks (incomplete) with 150k pending spends
    await store.saveBlock({
      block: block2,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1)],
    });
    const parkedMs = Date.now() - started;
    await store.saveBlock({
      block: block1,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1)],
    });
    while (store.operations.activeCount > 0) {
      await new Promise((resolve) => {
        setTimeout(resolve, 50);
      });
    }
    await store.publishWatermarks();
    t.log(
      `parked after ${parkedMs} ms, all committed after ${
        Date.now() - started
      } ms`
    );
    const unspent = await utxoCountAt(client, node1);
    t.is(unspent.n, String(150_000 + 2));
    const pending = await client.query<{ s: string; n: string }>(
      'SELECT toString(sum(sign)) AS s, toString(count()) AS n FROM pending_spend'
    );
    t.deepEqual(pending[0], { n: String(2 * 150_000), s: '0' });
    t.deepEqual(await badUtxoSums(client), []);
  }
);

/**
 * A chain of `count` blocks after `block0` (from `threeBlockChain`'s style):
 * block i has a coinbase and a tx spending the previous block's coinbase
 * output 0 and (from block 2) the previous tx's output 0.
 */
const linearChain = (count: number, label: string) => {
  const blocks: ChaingraphBlock[] = [];
  let previousHash = zeroHash;
  let previousCoinbase: string | undefined;
  let previousTx: string | undefined;
  for (let height = 0; height < count; height += 1) {
    const coinbase = makeTx({
      coinbase: true,
      label: `${label}-c${height}`,
      outputs: [
        { lockingBytecode: p2pkh(`${label}-miner`), valueSatoshis: 1_000n },
        { lockingBytecode: p2pkh(`${label}-keep`), valueSatoshis: 7n },
      ],
    });
    const spends: [string, number][] = [];
    if (previousCoinbase !== undefined) spends.push([previousCoinbase, 0]);
    if (previousTx !== undefined) spends.push([previousTx, 0]);
    const transactions = [coinbase];
    if (spends.length > 0) {
      const transaction = makeTx({
        label: `${label}-t${height}`,
        outputs: [
          { lockingBytecode: p2pkh(`${label}-chain`), valueSatoshis: 900n },
          { lockingBytecode: p2pkh(`${label}-side`), valueSatoshis: 1n },
        ],
        spends,
      });
      transactions.push(transaction);
      previousTx = transaction.hash;
    }
    const block = makeBlock(
      height,
      previousHash,
      transactions,
      `${label}-block-${height}`
    );
    blocks.push(block);
    previousHash = block.hash;
    previousCoinbase = coinbase.hash;
  }
  return blocks;
};

const blockCommitCount = async (client: Parameters<typeof nodeView>[0]) =>
  Number(
    (
      await client.query<{ n: string }>(
        `SELECT toString(count()) AS n FROM commit_log FINAL WHERE kind = 'block' AND state = 'committed'`
      )
    )[0]!.n
  );

e2e(
  '[e2e] ClickHouseStore: queued blocks are coalesced into multi-block commits, all-or-none per node (WP6b item 4)',
  async (t) => {
    t.timeout(300_000);
    const { client, openStore } = await scratch(t, 'batch');
    const blocks = linearChain(41, 'batch');
    const both = () => [acceptance(1), acceptance(2)];
    const control = { commits: 0, crash: false, holdFirst: false };
    let release: (() => void) | undefined;
    const crashing = await openStore(async (step, context) => {
      if (context.kind !== 'block') return;
      if (step === 'intent' && control.holdFirst) {
        // the first commit waits, so the next blocks queue up behind it
        control.holdFirst = false;
        await new Promise<void>((resolve) => {
          release = resolve;
        });
      }
      if (step === 'utxo' && control.crash && control.commits === 1) {
        throw new SimulatedCrash('crash inside a multi-block commit');
      }
      if (step === 'committed') control.commits += 1;
    });
    const { node1, node2 } = await registerNodes(crashing);
    t.deepEqual([node1, node2], [1, 2]);
    await crashing.saveBlock({
      block: blocks[0]!,
      isSavedTransaction: notSaved,
      nodeAcceptances: both(),
    });
    control.commits = 0;
    control.holdFirst = true;
    control.crash = true;
    // block 1 starts alone; blocks 2..20 queue behind it as one batch
    const saves = blocks.slice(1, 21).map(async (block) =>
      crashing.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: both(),
      })
    );
    // eslint-disable-next-line no-unmodified-loop-condition
    for (let tries = 0; tries < 500 && release === undefined; tries += 1) {
      await new Promise((resolve) => {
        setTimeout(resolve, 10);
      });
    }
    release!();
    const outcomes = await Promise.allSettled(saves);
    t.is(outcomes[0]!.status, 'fulfilled', 'block 1 committed alone');
    t.true(
      outcomes
        .slice(1)
        .every(
          (outcome) =>
            outcome.status === 'rejected' &&
            outcome.reason instanceof SimulatedCrash
        ),
      'blocks 2..20 were one commit, which crashed'
    );
    await crashing.simulateCrash();
    /*
     * before recovery the crashed batch is invisible to both nodes
     * (watermark); block 1 may or may not have been published before the crash
     */
    for (const node of [node1, node2]) {
      const seen = (await nodeView(client, node)).blocks;
      t.true(
        [1, 2].some(
          (count) =>
            JSON.stringify(seen) ===
            JSON.stringify(blocks.slice(0, count).map((block) => block.hash))
        ),
        `node ${node} sees ${seen.length} blocks`
      );
    }

    // recovery aborts it; a reader still sees none of it
    await new Promise((resolve) => {
      setTimeout(resolve, leaseTtlMs + 200);
    });
    const store = await openStore();
    await store.publishWatermarks();
    for (const node of [node1, node2]) {
      const view = await nodeView(client, node);
      t.deepEqual(
        view.blocks,
        blocks.slice(0, 2).map((block) => block.hash)
      );
      t.deepEqual(view.utxo, expectedUnspent(blocks.slice(0, 2)));
    }
    const before = await blockCommitCount(client);
    // the rest, called without waiting: coalesced, exact for both nodes
    await Promise.all(
      blocks.slice(2).map(async (block) =>
        store.saveBlock({
          block,
          isSavedTransaction: notSaved,
          nodeAcceptances: both(),
        })
      )
    );
    await store.operations.drain();
    await store.publishWatermarks();
    const commits = (await blockCommitCount(client)) - before;
    t.log(`${blocks.length - 2} blocks in ${commits} commits`);
    t.true(commits < (blocks.length - 2) / 2, `${commits} commits`);
    for (const node of [node1, node2]) {
      const view = await nodeView(client, node);
      t.deepEqual(
        view.blocks,
        blocks.map((block) => block.hash)
      );
      t.deepEqual(view.txs, txHashes(blocks));
      t.deepEqual(view.utxo, expectedUnspent(blocks));
      t.deepEqual(view.utxoByScript, view.utxo);
    }
    t.deepEqual(await badUtxoSums(client), []);
    const incomplete = await store.getIncompleteBlocks({
      excludedBlockHashes: [],
      heightLowerBound: 0,
      heightUpperBound: 100,
      limit: 100,
      nodeInternalIds: [node1, node2],
    });
    t.deepEqual(incomplete.incompleteBlocks, []);
  }
);

e2e(
  '[e2e] ClickHouseStore: a batch with a child before its parent parks and completes; maxBlocksPerCommit 1 commits per block',
  async (t) => {
    t.timeout(300_000);
    const { client, openStore } = await scratch(t, 'batch_child', {
      maxBlocksPerCommit: 1,
    });
    const store = await openStore();
    const { node1 } = await registerNodes(store);
    const blocks = linearChain(12, 'single');
    await store.saveBlock({
      block: blocks[0]!,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1)],
    });
    const before = await blockCommitCount(client);
    // reverse order: every block is saved before its parent
    await Promise.all(
      blocks
        .slice(1)
        .reverse()
        .map(async (block) =>
          store.saveBlock({
            block,
            isSavedTransaction: notSaved,
            nodeAcceptances: [acceptance(node1)],
          })
        )
    );
    await store.operations.drain();
    await store.publishWatermarks();
    t.is((await blockCommitCount(client)) - before, blocks.length - 1);
    const view = await nodeView(client, node1);
    t.deepEqual(
      view.blocks,
      blocks.map((block) => block.hash)
    );
    t.deepEqual(view.utxo, expectedUnspent(blocks));
    t.deepEqual(await badUtxoSums(client), []);
  }
);

e2e(
  '[e2e] ClickHouseStore: a new block behind a running re-org writes its node-agnostic rows first, its node facts after (WP6b item 5)',
  async (t) => {
    t.timeout(120_000);
    const { client, openStore } = await scratch(t, 'pipeline');
    let releaseReorg: (() => void) | undefined;
    const held = new Promise<void>((resolve) => {
      releaseReorg = resolve;
    });
    let reorgHeld: (() => void) | undefined;
    const reorgReached = new Promise<void>((resolve) => {
      reorgHeld = resolve;
    });
    const store = await openStore(async (step, context) => {
      if (context.kind === 'reorg' && step === 'node_block') {
        reorgHeld?.();
        await held;
      }
    });
    const { node1 } = await registerNodes(store);
    const chain = threeBlockChain();
    for (const block of [chain.block0, chain.block1, chain.block2]) {
      await store.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(node1)],
      });
    }
    const reorg = store.removeStaleBlocksForNode(node1, [chain.block2.hash]);
    await reorgReached;
    const competing = makeBlock(
      2,
      chain.block1.hash,
      [
        makeTx({
          coinbase: true,
          label: 'pipeline-c2',
          outputs: [{ lockingBytecode: p2pkh('miner'), valueSatoshis: 1n }],
        }),
      ],
      'pipeline-block-2'
    );
    const saved = store.saveBlock({
      block: competing,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1)],
    });
    const outputRows = async () =>
      Number(
        (
          await client.query<{ n: string }>(
            `SELECT toString(count()) AS n FROM output
             WHERE transaction_hash = toFixedString(unhex({hash:String}), 32)`,
            { hash: competing.transactions[0]!.hash }
          )
        )[0]!.n
      );
    let rows = 0;
    for (let tries = 0; tries < 200 && rows === 0; tries += 1) {
      rows = await outputRows();
      if (rows === 0) {
        await new Promise((resolve) => {
          setTimeout(resolve, 10);
        });
      }
    }
    t.is(rows, 1, 'the output row is written while the re-org is held');
    await store.publishWatermarks();
    t.false(
      (await nodeView(client, node1)).blocks.includes(competing.hash),
      'and nothing of the block is visible to the node yet'
    );
    releaseReorg!();
    await reorg;
    await saved;
    await store.publishWatermarks();
    const view = await nodeView(client, node1);
    t.deepEqual(view.blocks, [
      chain.block0.hash,
      chain.block1.hash,
      competing.hash,
    ]);
    t.deepEqual(
      view.utxo,
      expectedUnspent([chain.block0, chain.block1, competing])
    );
    t.deepEqual(await badUtxoSums(client), []);
  }
);

/*
 * WP6b: the agent hung after "initial sync is complete" with
 * CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES=16: finishInitialSync drains
 * every operation, and parked children re-armed their pending-spend timeout
 * while ANY call was queued for a slot, including children that had only
 * given their slot up for the wait, so they completed one at a time.
 */
for (const [cap, maxBlocksPerCommit] of [
  [2, 1],
  [16, 1],
  [16, 64],
] as const) {
  e2e(
    `[e2e] ClickHouseStore: initial sync of blocks spending unknown outputs completes with in-flight cap ${cap} (max ${maxBlocksPerCommit} blocks per commit)`,
    async (t) => {
      t.timeout(120_000);
      const { client, openStore } = await scratch(t, `sync_cap${cap}`, {
        maxBlocksPerCommit,
        maxInFlightSaves: cap,
        pendingSpendTimeoutMs: 1,
        // per-block commits all working at once, as the agent's buffer feeds them
        runningBatchesPerNodeSet: maxBlocksPerCommit === 1 ? 64 : undefined,
      });
      const store = await openStore();
      const { node1, node2 } = await registerNodes(store);
      await store.prepareForInitialSync();
      // every non-coinbase input spends an outpoint that never exists (the e2e mockchain)
      const blocks: ChaingraphBlock[] = [];
      let previous = zeroHash;
      for (let height = 0; height < 300; height += 1) {
        const block = makeBlock(
          height,
          previous,
          [
            makeTx({
              coinbase: true,
              label: `sync-${cap}-${maxBlocksPerCommit}-c${height}`,
              outputs: [{ lockingBytecode: p2pkh('m'), valueSatoshis: 1n }],
            }),
            makeTx({
              label: `sync-${cap}-${maxBlocksPerCommit}-t${height}`,
              outputs: [{ lockingBytecode: p2pkh('x'), valueSatoshis: 1n }],
              spends: [[sha(`nowhere-${height}`), 0]],
            }),
          ],
          `sync-${cap}-${maxBlocksPerCommit}-b${height}`
        );
        blocks.push(block);
        previous = block.hash;
      }
      const started = Date.now();
      await Promise.all(
        blocks.map(async (block) =>
          store.saveBlock({
            block,
            isSavedTransaction: notSaved,
            nodeAcceptances: [acceptance(node1), acceptance(node2)],
          })
        )
      );
      const outcome = await Promise.race([
        store
          .finishInitialSync({
            onIndexProgress: () => undefined,
            onNonFatalError: () => undefined,
          } as unknown as Parameters<ClickHouseStore['finishInitialSync']>[0])
          .then(async () => store.enableMempoolTracking())
          .then(() => 'enabled mempool tracking'),
        new Promise<string>((resolve) => {
          setTimeout(() => {
            resolve('hung');
          }, 30_000);
        }),
      ]);
      t.log(`${outcome} after ${Date.now() - started} ms`);
      t.is(outcome, 'enabled mempool tracking');
      t.is(store.storeMode, 'tip');
      await store.publishWatermarks();
      t.is((await nodeView(client, node2)).blocks.length, blocks.length);
    }
  );
}

e2e(
  '[e2e] ClickHouseStore: a parked save is answered with `committed`, which resolves only once the block is committed (WP6b item 8)',
  async (t) => {
    t.timeout(120_000);
    const { client, openStore } = await scratch(t, 'parked');
    const store = await openStore();
    const { node1 } = await registerNodes(store);
    const chain = threeBlockChain();
    const saved = await store.saveBlock({
      block: chain.block0,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1)],
    });
    t.is(saved.committed, undefined, 'a committed save has no `committed`');
    const child = await store.saveBlock({
      block: chain.block2,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1)],
    });
    t.not(child.committed, undefined, 'the child parked');
    let childCommitted = false;
    const committedPromise = child.committed!.then(() => {
      childCommitted = true;
    });
    await new Promise((resolve) => {
      setTimeout(resolve, 300);
    });
    t.false(childCommitted, 'not committed while its parent is missing');
    await store.publishWatermarks();
    t.false((await nodeView(client, node1)).blocks.includes(chain.block2.hash));
    await store.saveBlock({
      block: chain.block1,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1)],
    });
    await committedPromise;
    await store.publishWatermarks();
    t.deepEqual((await nodeView(client, node1)).blocks, [
      chain.block0.hash,
      chain.block1.hash,
      chain.block2.hash,
    ]);
  }
);
