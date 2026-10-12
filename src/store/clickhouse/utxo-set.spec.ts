/* eslint-disable @typescript-eslint/no-magic-numbers, functional/no-loop-statement, no-await-in-loop, functional/no-throw-statement, functional/no-let, complexity, max-params, @typescript-eslint/no-loop-func */
// cspell:ignore clickhouse utxoset rebases
/**
 * The stored per-node UTXO set maintained off the commit path
 * (docs/clickhouse-port/utxo-async.md). One history on two nodes: blocks
 * with spends, mempool additions, a replacement and a conflict, a re-org onto
 * a competing block and back by header acceptance, a commit voided after its
 * rows were written, a mempool confirmation and a restart. After every step,
 * for every node, the read rule (stored rows up to U plus the correction for
 * (U, visible(n)]) and the per-category totals equal the query-time
 * definition, at the snapshot's own watermark and at EVERY published
 * watermark of the node (a reader lagging by any amount), and the stored
 * rows keep their invariant (every outpoint's sum of signs is 0 or 1).
 * The maintainer is held (paused) across several steps, so the correction
 * covers mempool, re-org, header and void transitions.
 */
import type { ExecutionContext } from 'ava';
import test from 'ava';

import type { ClickHouseStore } from './clickhouse-store.js';
import {
  acceptance,
  category,
  makeBlock,
  makeTx,
  notSaved,
  p2pkh,
  registerNodes,
  scratch,
  threeBlockChain,
} from './spec-fixtures.js';
import { e2eClickHouseUrl } from './test-support.js';
import {
  checkUtxoSet,
  publishedUtxoMarks,
  unspentAtQueryTimeSql,
  utxoSetViewStatement,
} from './utxo-set.js';
import {
  readSnapshotMulti,
  snapshotForNode,
  snapshotParams,
} from './visibility.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const at = (seconds: number) =>
  new Date(Date.UTC(2026, 9, 12, 12, 0, 0) + seconds * 1_000);

const fixture = () => {
  const chain = threeBlockChain();
  /** p spends a:1 on both nodes; r spends a:1 too (replaces p on node 1) */
  const p = makeTx({
    label: 'utxoset-p',
    outputs: [{ lockingBytecode: p2pkh('p'), valueSatoshis: 1_000n }],
    spends: [[chain.a.hash, 1]],
  });
  const r = makeTx({
    label: 'utxoset-r',
    outputs: [
      {
        fungibleTokenAmount: 7n,
        lockingBytecode: p2pkh('r'),
        tokenCategory: category,
        valueSatoshis: 950n,
      },
    ],
    spends: [[chain.a.hash, 1]],
  });
  /** q spends p:0 (a mempool child of p, node 2 only) */
  const q = makeTx({
    label: 'utxoset-q',
    outputs: [{ lockingBytecode: p2pkh('q'), valueSatoshis: 900n }],
    spends: [[p.hash, 0]],
  });
  /** v: saved by a commit that fails after its rows are written (voided) */
  const v = makeTx({
    label: 'utxoset-v',
    outputs: [{ lockingBytecode: p2pkh('v'), valueSatoshis: 500n }],
    spends: [[chain.c1.hash, 0]],
  });
  const c2alt = makeTx({
    coinbase: true,
    label: 'utxoset-c2-alt',
    outputs: [
      { lockingBytecode: p2pkh('other-miner'), valueSatoshis: 5_000_000_000n },
    ],
  });
  /** block 2' spends c0:1 differently (b's spend is released on node 1) */
  const w = makeTx({
    label: 'utxoset-w',
    outputs: [{ lockingBytecode: p2pkh('w'), valueSatoshis: 600n }],
    spends: [[chain.c0.hash, 1]],
  });
  const block2alt = makeBlock(
    2,
    chain.block1.hash,
    [c2alt, w],
    'utxoset-block-2-alt'
  );
  const c3 = makeTx({
    coinbase: true,
    label: 'utxoset-c3',
    outputs: [
      { lockingBytecode: p2pkh('miner'), valueSatoshis: 5_000_000_000n },
    ],
  });
  /** block 3 confirms r (node 1's mempool) on top of block 2 */
  const block3 = makeBlock(3, chain.block2.hash, [c3, r], 'utxoset-block-3');
  return { block2alt, block3, chain, p, q, r, v };
};

/** Every node: exact at the snapshot's own mark and at every published mark. */
const assertExact = async (
  t: ExecutionContext,
  store: ClickHouseStore,
  client: Parameters<typeof checkUtxoSet>[0],
  nodes: readonly number[],
  step: string
) => {
  await store.publishWatermarks();
  for (const node of nodes) {
    const own = await checkUtxoSet(client, node);
    const snapshot = snapshotForNode(
      await readSnapshotMulti(client, [node], { utxo: true }),
      node
    );
    const marks = await publishedUtxoMarks(client, node, snapshot.visible);
    const checks = [
      ...(own === undefined ? [] : [own]),
      ...(await Promise.all(
        marks.map(async (mark) => checkUtxoSet(client, node, mark))
      )),
    ];
    for (const check of checks) {
      const label = `${step}: node ${node} at U=${check!.watermark} g${
        check!.generation
      } (visible ${check!.visible})`;
      t.deepEqual(
        {
          extra: check!.extra,
          invariant: check!.invariant,
          mismatched: check!.mismatched,
          missing: check!.missing,
          totalsEqual: check!.totalsEqual,
        },
        {
          extra: [],
          invariant: [],
          mismatched: [],
          missing: [],
          totalsEqual: true,
        },
        label
      );
    }
    t.log(
      `${step}: node ${node}: ${checks.length} reads exact (marks ${marks
        .map((mark) => `${mark.watermark}/${mark.generation}`)
        .join(' ')}), own U ${
        snapshot.utxo === undefined
          ? 'none'
          : `${snapshot.utxo.watermark} lag ${snapshot.utxo.lagCommits}`
      }`
    );
  }
};

/** The query-time set of a node as `hash:index` keys (for history assertions). */
const unspentKeys = async (
  client: Parameters<typeof checkUtxoSet>[0],
  node: number
) => {
  const snapshot = snapshotForNode(
    await readSnapshotMulti(client, [node]),
    node
  );
  const rows = await client.query<{ k: string }>(
    `SELECT concat(lower(hex(transaction_hash)), ':', toString(output_index)) AS k
     FROM (${unspentAtQueryTimeSql(snapshot)}) ORDER BY k`,
    snapshotParams(snapshot)
  );
  return rows.map((row) => row.k);
};

const scenario = (utxo: 'off' | 'on', rebase: boolean) => {
  e2e(
    `[e2e] stored UTXO set (utxo ${utxo}${
      rebase ? ', rebasing every batch' : ''
    }): exact per node at every lag across mempool, re-org, header acceptance, a voided commit and a restart`,
    async (t) => {
      t.timeout(300_000);
      const { client, openStore } = await scratch(
        t,
        `${utxo}${rebase ? '_rb' : ''}`,
        {
          utxo,
          utxoSetOptions: {
            bulkBuildMinCommits: 0,
            intervalMs: 20,
            /* rebasing: old generations are kept during the steps (every published mark is read) */
            ...(rebase
              ? { dropGraceMs: 600_000, rebaseFactor: 0, rebaseMinRows: 1 }
              : {}),
          },
        },
        'ch1_utxoset'
      );
      const fx = fixture();
      const { chain } = fx;
      const failMempool = { armed: false, failed: 0 };
      let store = await openStore(async (step, context) => {
        if (
          failMempool.armed &&
          context.kind === 'mempool_batch' &&
          step === 'rows-written'
        ) {
          failMempool.failed += 1;
          throw new Error('injected failure after rows-written (utxo set)');
        }
      });
      const { node1, node2 } = await registerNodes(store);
      const nodes = [node1, node2];
      const both = [acceptance(node1, at(0)), acceptance(node2, at(0))];

      for (const block of [chain.block0, chain.block1]) {
        await store.saveBlock({
          block,
          isSavedTransaction: notSaved,
          nodeAcceptances: both,
        });
      }
      await store.publishWatermarks();
      await store.catchUpUtxoSet();
      await assertExact(t, store, client, nodes, 'blocks 0-1, caught up');

      /* pause the job: everything below is answered through the correction */
      const held = (await store.publishWatermarks()).get(node1)!;
      store.holdUtxoSet(held);
      await store.saveBlock({
        block: chain.block2,
        isSavedTransaction: notSaved,
        nodeAcceptances: both,
      });
      await store.saveMempoolTransaction(fx.p, [
        { nodeInternalId: node1, validatedAt: at(10) },
        { nodeInternalId: node2, validatedAt: at(10) },
      ]);
      await store.saveMempoolTransaction(fx.q, [
        { nodeInternalId: node2, validatedAt: at(11) },
      ]);
      /* r replaces p on node 1 (a conflict: same outpoint a:1) */
      await store.saveMempoolTransaction(fx.r, [
        { nodeInternalId: node1, validatedAt: at(20) },
      ]);
      await assertExact(t, store, client, nodes, 'mempool, job paused');
      t.true(
        (await unspentKeys(client, node1)).includes(`${fx.r.hash}:0`),
        'node 1 holds r'
      );
      t.true(
        (await unspentKeys(client, node2)).includes(`${fx.q.hash}:0`),
        'node 2 holds q'
      );

      /* re-org of node 1 onto block 2' while paused, then catch up */
      await store.removeStaleBlocksForNode(node1, [chain.block2.hash], at(30));
      await store.saveBlock({
        block: fx.block2alt,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(node1, at(31))],
      });
      await assertExact(t, store, client, nodes, 're-org, job paused');
      store.holdUtxoSet(undefined);
      await store.catchUpUtxoSet();
      await assertExact(t, store, client, nodes, 're-org, caught up');

      /* back to block 2 by header acceptance, job paused again */
      store.holdUtxoSet((await store.publishWatermarks()).get(node1));
      await store.removeStaleBlocksForNode(node1, [fx.block2alt.hash], at(40));
      t.is(
        await store.acceptBlocksViaHeaders(
          node1,
          [{ hash: chain.block2.hash, height: 2 }],
          at(41)
        ),
        1
      );
      await assertExact(t, store, client, nodes, 'header acceptance, paused');

      /* a mempool commit fails after writing its rows: voided, never visible */
      failMempool.armed = true;
      await t.throwsAsync(
        store.saveMempoolTransaction(fx.v, [
          { nodeInternalId: node1, validatedAt: at(50) },
          { nodeInternalId: node2, validatedAt: at(50) },
        ])
      );
      failMempool.armed = false;
      t.true(failMempool.failed > 0, 'the injected failure ran');
      await store.publishWatermarks();
      const [voided] = await client.query<{ n: string }>(
        'SELECT toString(count()) AS n FROM commit_void'
      );
      t.true(Number(voided?.n) > 0, 'the failed commit is void');
      await assertExact(t, store, client, nodes, 'voided commit, paused');
      t.false(
        (await unspentKeys(client, node1)).includes(`${fx.v.hash}:0`),
        'the void transaction is not visible'
      );
      store.holdUtxoSet(undefined);
      await store.catchUpUtxoSet();
      await assertExact(t, store, client, nodes, 'voided commit, caught up');

      /* block 3 confirms r on node 1 (mempool -1, block +1) */
      store.holdUtxoSet((await store.publishWatermarks()).get(node1));
      await store.saveBlock({
        block: fx.block3,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(node1, at(60))],
      });
      await assertExact(t, store, client, nodes, 'confirmation, paused');

      /*
       * restart: the next store's maintainer resumes from the published
       * marks (rebasing: and drops every older generation at once, with its
       * marks)
       */
      await store.close();
      store = await openStore(undefined, {
        utxoSetOptions: {
          bulkBuildMinCommits: 0,
          dropGraceMs: 0,
          intervalMs: 20,
          ...(rebase ? { rebaseFactor: 0, rebaseMinRows: 1 } : {}),
        },
      });
      await store.publishWatermarks();
      await store.catchUpUtxoSet();
      if (rebase) {
        const generations = async () =>
          client.query<{ n: number; g: number }>(
            `SELECT n, uniqExact(g) AS g FROM (
               SELECT node_internal_id AS n, generation AS g FROM utxo_set
               UNION ALL SELECT node_internal_id, generation FROM utxo_watermark)
             GROUP BY n HAVING g > 1`
          );
        for (let tries = 0; tries < 200; tries += 1) {
          if ((await generations()).length === 0) break;
          await new Promise((resolve) => {
            setTimeout(resolve, 50);
          });
        }
        t.deepEqual(
          await generations(),
          [],
          'older generations dropped (rows and marks)'
        );
      }
      await assertExact(t, store, client, nodes, 'restart, caught up');
      const status = store.utxoSetStatus();
      t.truthy(status, 'the maintainer runs by default');
      if (rebase) {
        const [generations] = await client.query<{ g: number }>(
          'SELECT max(generation) AS g FROM utxo_watermark'
        );
        t.true(Number(generations?.g) > 2, 'the set was rebased');
      }
      /* the commit path wrote nothing to utxo_set (the maintainer did) */
      const [batches] = await client.query<{ n: string }>(
        'SELECT toString(countIf(batch NOT IN (SELECT watermark FROM utxo_watermark))) AS n FROM utxo_set'
      );
      t.is(batches?.n, '0', 'every utxo_set row belongs to a published batch');
    }
  );
};

scenario('off', false);
scenario('off', true);
scenario('on', false);

e2e(
  '[e2e] stored UTXO set: off (CHAINGRAPH_CLICKHOUSE_UTXO_SET=off) writes nothing; the snapshot has no mark',
  async (t) => {
    t.timeout(120_000);
    const { client, openStore } = await scratch(
      t,
      'disabled',
      { utxo: 'off', utxoSet: 'off' },
      'ch1_utxoset'
    );
    const store = await openStore();
    const { node1 } = await registerNodes(store);
    const { chain } = fixture();
    await store.saveBlock({
      block: chain.block0,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1, at(0))],
    });
    await store.publishWatermarks();
    t.is(store.utxoSetStatus(), undefined);
    await t.throwsAsync(store.catchUpUtxoSet());
    const [rows] = await client.query<{ n: string }>(
      'SELECT toString(count()) AS n FROM utxo_set'
    );
    t.is(rows?.n, '0');
    t.is(await checkUtxoSet(client, node1), undefined);
  }
);

test('utxo_set_at in ddl/090_utxo_set.sql is the generated statement', async (t) => {
  const { readFileSync } = await import('node:fs');
  const { fileURLToPath } = await import('node:url');
  const ddl = readFileSync(
    fileURLToPath(
      new URL(
        '../../../src/store/clickhouse/ddl/090_utxo_set.sql',
        import.meta.url
      )
    ),
    'utf8'
  );
  t.true(
    ddl.includes(utxoSetViewStatement()),
    'regenerate with `yarn clickhouse:views:generate`'
  );
});
