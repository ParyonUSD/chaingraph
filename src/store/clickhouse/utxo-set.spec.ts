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

import { ClickHouseChecker } from './checker.js';
import type { ClickHouseStore, StoreDiagnostic } from './clickhouse-store.js';
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
import type { UtxoSetSpotCheckMismatch } from './utxo-set.js';
import {
  checkOutputsPerChunk,
  checkUtxoSet,
  hashRangeChunks,
  nodeCheckChunkCount,
  publishedUtxoMarks,
  referenceTotalsChunked,
  spotCheckUtxoSet,
  totalsAnswer,
  unspentAtQueryTimeSql,
  utxoSetMetricsText,
  utxoSetViewStatement,
  UtxoSpotCheckFailedError,
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
      /* the same read in 16 hash-range chunks (fix-pass-7.md §5) */
      ...(own === undefined
        ? []
        : [await checkUtxoSet(client, node, undefined, { chunks: 16 })]),
    ];
    if (own !== undefined) {
      t.deepEqual(
        { ...checks.at(-1)!, chunks: 1 },
        { ...own, chunks: 1 },
        `${step}: 16 chunks answer as ${own.chunks}`
      );
    }
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

const scenario = (utxo: 'off' | 'on', rebase: boolean, build = false) => {
  e2e(
    `[e2e] stored UTXO set (utxo ${utxo}${
      rebase ? ', rebasing every batch' : ''
    }${
      build ? ', bulk build in 4 slices' : ''
    }): exact per node at every lag across mempool, re-org, header acceptance, a voided commit and a restart`,
    async (t) => {
      t.timeout(300_000);
      const { client, openStore } = await scratch(
        t,
        `${utxo}${rebase ? '_rb' : ''}${build ? '_bb' : ''}`,
        {
          utxo,
          utxoSetOptions: {
            bulkBuildMinCommits: build ? 1 : 0,
            intervalMs: 20,
            slices: 4,
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
      if (build) {
        const kinds = await client.query<{ kind: string }>(
          `SELECT toString(kind) AS kind FROM utxo_watermark
           ORDER BY node_internal_id, watermark, generation LIMIT 1 BY node_internal_id`
        );
        t.deepEqual(
          kinds.map((row) => row.kind),
          ['build', 'build'],
          "each node's first set is a bulk build"
        );
        const [work] = await client.query<{ n: string }>(
          'SELECT toString(count()) AS n FROM utxo_build_spent'
        );
        t.is(work?.n, '0', 'the build left no work rows');
      }

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
      if (utxo === 'off' && !rebase && !build) {
        /* the check is not vacuous: a stored row with another value, or a duplicate, is reported */
        await store.close();
        const mark = (await checkUtxoSet(client, node1))!;
        await client.command(
          `INSERT INTO utxo_set (node_internal_id, generation, batch, transaction_hash, output_index,
             transaction_internal_id, value_satoshis, locking_bytecode, token_category, fungible_token_amount,
             nonfungible_token_capability, nonfungible_token_commitment, sign)
           SELECT node_internal_id, generation, batch, transaction_hash, output_index, transaction_internal_id,
             value_satoshis + 1, locking_bytecode, token_category, fungible_token_amount,
             nonfungible_token_capability, nonfungible_token_commitment, sign
           FROM utxo_set
           WHERE node_internal_id = {node:UInt32} AND generation = {generation:UInt32} AND sign = 1
             AND fungible_token_amount IS NULL
             AND (transaction_hash, output_index) IN (
               SELECT transaction_hash, output_index FROM utxo_set
               WHERE node_internal_id = {node:UInt32} AND generation = {generation:UInt32}
               GROUP BY transaction_hash, output_index HAVING sum(sign) = 1)
           ORDER BY transaction_hash, output_index LIMIT 1`,
          { generation: mark.generation, node: node1 }
        );
        const broken = (await checkUtxoSet(client, node1))!;
        t.is(broken.invariant.length, 1, 'the duplicate breaks the invariant');
        t.false(broken.totalsEqual, 'and the totals');
        /* a -1 for another unspent outpoint: the read rule loses it, and says so */
        await client.command(
          `INSERT INTO utxo_set (node_internal_id, generation, batch, transaction_hash, output_index,
             transaction_internal_id, value_satoshis, locking_bytecode, token_category, fungible_token_amount,
             nonfungible_token_capability, nonfungible_token_commitment, sign)
           SELECT node_internal_id, generation, batch, transaction_hash, output_index, transaction_internal_id,
             value_satoshis, locking_bytecode, token_category, fungible_token_amount,
             nonfungible_token_capability, nonfungible_token_commitment, -1
           FROM utxo_set
           WHERE node_internal_id = {node:UInt32} AND generation = {generation:UInt32} AND sign = 1
             AND (transaction_hash, output_index) IN (
               SELECT transaction_hash, output_index FROM utxo_set
               WHERE node_internal_id = {node:UInt32} AND generation = {generation:UInt32}
               GROUP BY transaction_hash, output_index HAVING sum(sign) = 1)
           ORDER BY transaction_hash DESC, output_index LIMIT 1`,
          { generation: mark.generation, node: node1 }
        );
        const lost = (await checkUtxoSet(client, node1))!;
        t.is(lost.missing.length, 1, 'the lost outpoint is reported missing');
      }
    }
  );
};

scenario('off', false);
scenario('off', true);
scenario('on', false);
scenario('off', false, true);

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

e2e(
  '[e2e] UTXO set spot-check: random categories, prefixes and totals equal the query-time answer at one snapshot; a rewritten history is a counted, reported mismatch',
  async (t) => {
    t.timeout(180_000);
    const diagnostics: StoreDiagnostic[] = [];
    const { client, name, openStore } = await scratch(
      t,
      'spot',
      {
        onDiagnostic: (diagnostic) => {
          diagnostics.push(diagnostic);
        },
        utxo: 'off',
        utxoSetOptions: {
          intervalMs: 20,
          spotCheck: { totals: true, totalsChunks: 4 },
          spotCheckIntervalMs: 0,
        },
      },
      'ch1_utxoset'
    );
    const fx = fixture();
    const { chain } = fx;
    const store = await openStore();
    const { node1, node2 } = await registerNodes(store);
    const both = [acceptance(node1, at(0)), acceptance(node2, at(0))];
    for (const block of [chain.block0, chain.block1, chain.block2]) {
      await store.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: both,
      });
    }
    /* r: a token output, in node 1's mempool only */
    await store.saveMempoolTransaction(fx.r, [
      { nodeInternalId: node1, validatedAt: at(20) },
    ]);
    await store.publishWatermarks();
    await store.catchUpUtxoSet();
    await store.publishWatermarks();

    const rPrefix = fx.r.outputs[0]!.lockingBytecode.slice(0, 50);
    const scoped = {
      categoryValues: [category],
      prefixValues: [rPrefix],
    };
    /* random scopes (the job's default): every scope equal, nothing skipped */
    const random = await store.spotCheckUtxoSet();
    t.deepEqual(
      random.map((check) => check.node),
      [node1, node2]
    );
    for (const check of random) {
      t.deepEqual(check.failed, []);
      t.true(check.scopes.length >= 2, 'categories / prefixes and totals');
      t.true(
        check.scopes.every((scope) => scope.equal),
        JSON.stringify(check.scopes)
      );
      t.true(check.scopes.some((scope) => scope.kind === 'totals'));
    }
    const before = await spotCheckUtxoSet(client, node1, scoped);
    t.true(before!.scopes.every((scope) => scope.equal));
    t.regex(
      before!.scopes.find((scope) => scope.kind === 'category')!.stored,
      /^[1-9]\d* outputs, /u,
      "node 1's set holds r's token output"
    );

    /* fault injection: forget node 1's acceptance of r outside the commit protocol */
    const checker = new ClickHouseChecker(client, name);
    await checker.forgetNodeValidation('node-one', fx.r.hash);
    await store.publishWatermarks();
    const status = store.utxoSetStatus()!;
    const counted = status.spotCheckMismatches;
    const after = await store.spotCheckUtxoSet();
    const one = after.find((check) => check.node === node1)!;
    t.true(
      one.scopes.some((scope) => scope.kind === 'totals' && !scope.equal),
      "node 1's totals differ"
    );
    t.true(
      after
        .find((check) => check.node === node2)!
        .scopes.every((scope) => scope.equal),
      'node 2 is untouched'
    );
    const direct = await spotCheckUtxoSet(client, node1, {
      ...scoped,
      totals: false,
    });
    t.deepEqual(
      direct!.scopes.map((scope) => [scope.kind, scope.equal]),
      [
        ['category', false],
        ['prefix', false],
      ],
      'the category and prefix of r differ'
    );
    const mismatches = diagnostics.filter(
      (diagnostic): diagnostic is UtxoSetSpotCheckMismatch =>
        diagnostic.event === 'utxo_set_spot_check_mismatch'
    );
    t.true(mismatches.length >= 1);
    t.true(
      mismatches.every(
        (mismatch) =>
          mismatch.node === node1 &&
          mismatch.stored !== mismatch.reference &&
          mismatch.stored !== '' &&
          mismatch.reference !== ''
      ),
      'each report names the node, the scope and both answers'
    );
    t.is(
      status.spotCheckMismatches,
      counted + mismatches.length,
      'the mismatch counter'
    );
    t.regex(
      utxoSetMetricsText(status),
      new RegExp(
        `chaingraph_utxo_set_spot_check_mismatches_total ${status.spotCheckMismatches}\\n`,
        'u'
      )
    );
    t.true(status.spotCheckMismatches > 0);

    /* a scope whose statement fails is a FAILED check: it throws (fix-pass-7.md §5), not "skipped" */
    const failing = {
      query: async <T>(...args: Parameters<typeof client.query>) =>
        args[0].includes('chunk_lo')
          ? // eslint-disable-next-line functional/no-promise-reject
            Promise.reject(new Error('MEMORY_LIMIT_EXCEEDED (injected)'))
          : client.query<T>(...args),
    };
    const thrown = await t.throwsAsync(
      spotCheckUtxoSet(failing, node2, { totals: true, totalsChunks: 2 }),
      { instanceOf: UtxoSpotCheckFailedError }
    );
    t.regex(thrown!.message, /FAILED: 1 scope\(s\) not compared \(totals/u);
    t.deepEqual(
      (thrown as UtxoSpotCheckFailedError).check.failed.map((f) => f.kind),
      ['totals']
    );
    const quiet = await spotCheckUtxoSet(failing, node2, {
      throwOnFailure: false,
      totals: true,
    });
    t.is(quiet!.failed.length, 1, 'reported as failed when not thrown');
    /* and the whole-set check throws too */
    await t.throwsAsync(
      checkUtxoSet(failing, node2, undefined, { chunks: 2 }),
      {
        message: /injected/u,
      }
    );
  }
);

test('whole-set check chunks follow the UTXO count of the node, not the outputs of the database (fix-pass-8.md §6)', (t) => {
  /* mainnet 2026-10-13: node 1 and node 39 in one database of 1.16 B outputs (256 chunks for both before) */
  t.is(nodeCheckChunkCount(125_659_151n), 128);
  t.is(nodeCheckChunkCount(1_185_908n), 2);
  t.is(nodeCheckChunkCount(242_512_925), 256);
  t.is(nodeCheckChunkCount(0n), 1);
  t.is(nodeCheckChunkCount(1_000_000), 1);
  t.is(nodeCheckChunkCount(5, 1), 8);
  t.is(nodeCheckChunkCount(10n ** 12n), 65_536);
  /* fix-pass-9.md §6: and ~8 M created outputs per chunk (node 1's ~1.16 B: 256 chunks, 2.6 GiB, as fix 7) */
  t.is(nodeCheckChunkCount(1_160_000_000n, checkOutputsPerChunk), 256);
});

e2e(
  '[e2e] whole-set check and spot-check totals: chunks per node from its stored UTXO count; a small node reads what it holds (fix-pass-8.md §6)',
  async (t) => {
    t.timeout(180_000);
    const { client, openStore } = await scratch(
      t,
      'chunks',
      { utxo: 'off', utxoSetOptions: { intervalMs: 20 } },
      'ch1_utxoset'
    );
    const { chain } = fixture();
    const store = await openStore();
    const { node1, node2 } = await registerNodes(store);
    /* node 1 accepts the three blocks; node 2 only the first */
    await store.saveBlock({
      block: chain.block0,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1, at(0)), acceptance(node2, at(0))],
    });
    for (const block of [chain.block1, chain.block2]) {
      await store.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(node1, at(0))],
      });
    }
    await store.publishWatermarks();
    await store.catchUpUtxoSet();
    await store.publishWatermarks();
    /* count the chunk statements each check sends */
    let chunkStatements = 0;
    const counting = {
      query: async <T>(...args: Parameters<typeof client.query>) => {
        if (args[0].includes('chunk_lo')) chunkStatements += 1;
        return client.query<T>(...args);
      },
    };
    const sizes = new Map<number, number>();
    for (const node of [node1, node2]) {
      const single = await checkUtxoSet(client, node, undefined, {
        chunks: 1,
      });
      chunkStatements = 0;
      const perUtxo = await checkUtxoSet(counting, node, undefined, {
        utxosPerChunk: 1,
      });
      const expected = nodeCheckChunkCount(single!.count, 1);
      t.is(perUtxo!.chunks, expected, `node ${node}: chunks from its count`);
      /* one comparison and one invariant statement per chunk */
      t.is(chunkStatements, 2 * expected);
      t.deepEqual(
        { ...perUtxo!, chunks: 1 },
        single!,
        `node ${node}: answers as one chunk`
      );
      t.deepEqual(
        [perUtxo!.extra, perUtxo!.missing, perUtxo!.mismatched],
        [[], [], []]
      );
      t.true(perUtxo!.totalsEqual);
      sizes.set(node, expected);
      chunkStatements = 0;
      const spot = await spotCheckUtxoSet(counting, node, {
        categories: 0,
        prefixes: 0,
        totals: true,
        totalsUtxosPerChunk: 1,
      });
      t.deepEqual(spot!.failed, []);
      t.true(spot!.scopes.every((scope) => scope.equal));
      t.is(chunkStatements, expected, `node ${node}: totals chunks per node`);
      /* fix-pass-10.md §2: the small-node form answers as the chain-range form, in any chunking */
      const snapshot = snapshotForNode(
        await readSnapshotMulti(client, [node], { utxo: true }),
        node
      );
      const large = await referenceTotalsChunked(client, snapshot, {
        chunks: 1,
        form: 'large',
      });
      t.is(large.form, 'large');
      for (const chunks of [1, 4]) {
        const small = await referenceTotalsChunked(client, snapshot, {
          chunks,
          form: 'small',
        });
        t.is(small.form, 'small');
        t.is(
          totalsAnswer(small.totals),
          totalsAnswer(large.totals),
          `node ${node}: small-node form, ${chunks} chunks`
        );
      }
      const auto = await referenceTotalsChunked(client, snapshot, {
        utxos: BigInt(single!.count),
      });
      t.is(
        auto.form,
        'small',
        `node ${node}: a small node gets the small form`
      );
      t.is(totalsAnswer(auto.totals), totalsAnswer(large.totals));
    }
    t.true(
      sizes.get(node2)! < sizes.get(node1)!,
      'the node holding less is checked in fewer chunks'
    );
  }
);

test('hash-range chunks cover the hash space in order, powers of two only', (t) => {
  t.deepEqual(hashRangeChunks(1), [{ hi: undefined, lo: '00'.repeat(32) }]);
  const four = hashRangeChunks(4);
  t.deepEqual(
    four.map((range) => [range.lo.slice(0, 4), range.hi?.slice(0, 4)]),
    [
      ['0000', '4000'],
      ['4000', '8000'],
      ['8000', 'c000'],
      ['c000', undefined],
    ]
  );
  t.is(hashRangeChunks(65_536).length, 65_536);
  t.throws(() => hashRangeChunks(3), { message: /power of two/u });
  t.throws(() => hashRangeChunks(131_072));
});

e2e(
  '[e2e] UTXO set spot-check runs on its own every spotCheckIntervalMs',
  async (t) => {
    t.timeout(120_000);
    const reports: StoreDiagnostic[] = [];
    const { openStore } = await scratch(
      t,
      'spot_loop',
      {
        onDiagnostic: (diagnostic) => {
          if (diagnostic.event === 'utxo_set_spot_check')
            reports.push(diagnostic);
        },
        utxo: 'off',
        utxoSetOptions: { intervalMs: 20, spotCheckIntervalMs: 300 },
      },
      'ch1_utxoset'
    );
    const { chain } = fixture();
    const store = await openStore();
    const { node1 } = await registerNodes(store);
    for (const block of [chain.block0, chain.block1]) {
      await store.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(node1, at(0))],
      });
    }
    await store.publishWatermarks();
    await store.catchUpUtxoSet();
    const deadline = Date.now() + 30_000;
    while (store.utxoSetStatus()!.spotChecks < 2 && Date.now() < deadline) {
      await new Promise((resolve) => {
        setTimeout(resolve, 100);
      });
    }
    const status = store.utxoSetStatus()!;
    t.true(status.spotChecks >= 2, 'two rounds');
    t.is(status.spotCheckMismatches, 0);
    t.true(reports.length >= 2);
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
