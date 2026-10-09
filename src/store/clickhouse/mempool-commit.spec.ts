/* eslint-disable @typescript-eslint/no-magic-numbers, functional/no-loop-statement, functional/no-let, no-await-in-loop, functional/no-throw-statement, @typescript-eslint/init-declarations, @typescript-eslint/require-array-sort-compare, max-params, functional/no-mixed-type, complexity, @typescript-eslint/no-loop-func */
// cspell:ignore clickhouse seqs unhex mcrash
/**
 * WP5a-mempool `[e2e]` tests (CHAINGRAPH_E2E_CLICKHOUSE_URL; scratch
 * databases `ch1_wp5a_*`, dropped on teardown). Every scenario checks per
 * node: the node's mempool, history, `tx_acceptance` and UTXO set through
 * the pinned views (the checker), that the other node's facts did not
 * change, that every per-node row of the commit carries
 * `version = commit_seq`, and that every (node, outpoint) UTXO sum is 0 or 1.
 */
import test from 'ava';
import type { ExecutionContext } from 'ava';

import type { StoreChecker } from '../checker.js';

import { SimulatedCrash } from './block-commit.js';
import { createClickHouseChecker } from './checker.js';
import type { ClickHouseStore } from './clickhouse-store.js';
import type { ClickHouseClient } from './client.js';
import {
  acceptance,
  badUtxoSums,
  leaseTtlMs,
  makeBlock,
  makeTx,
  notSaved,
  p2pkh,
  registerNodes,
  scratch,
  threeBlockChain,
} from './spec-fixtures.js';
import { e2eClickHouseUrl } from './test-support.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const nodeOne = 'node-one';
const nodeTwo = 'node-two';

const at = (seconds: number) =>
  new Date(Date.UTC(2026, 9, 9, 12, 0, 0) + seconds * 1_000);

/**
 * The chain (block 0 and block 1, accepted by both nodes) plus mempool txs:
 * p spends a:1; q spends p:0 (child of p); r spends a:1 (conflicts with p);
 * u spends c1:0 (a parent that is first seen in a block); x spends u:0.
 */
const fixture = () => {
  const chain = threeBlockChain();
  const p = makeTx({
    label: 'p',
    outputs: [{ lockingBytecode: p2pkh('p'), valueSatoshis: 1_000n }],
    spends: [[chain.a.hash, 1]],
  });
  const q = makeTx({
    label: 'q',
    outputs: [{ lockingBytecode: p2pkh('q'), valueSatoshis: 900n }],
    spends: [[p.hash, 0]],
  });
  const r = makeTx({
    label: 'r',
    outputs: [{ lockingBytecode: p2pkh('r'), valueSatoshis: 950n }],
    spends: [[chain.a.hash, 1]],
  });
  const u = makeTx({
    label: 'u',
    outputs: [{ lockingBytecode: p2pkh('u'), valueSatoshis: 2_000n }],
    spends: [[chain.c1.hash, 0]],
  });
  const x = makeTx({
    label: 'x',
    outputs: [{ lockingBytecode: p2pkh('x'), valueSatoshis: 1_500n }],
    spends: [[u.hash, 0]],
  });
  const c2 = makeTx({
    coinbase: true,
    label: 'c2-mempool',
    outputs: [
      { lockingBytecode: p2pkh('miner'), valueSatoshis: 5_000_000_000n },
    ],
  });
  /** block 2 confirming p */
  const blockP = makeBlock(2, chain.block1.hash, [c2, p], 'block-2p');
  /** block 2 creating u (x's parent) */
  const blockU = makeBlock(2, chain.block1.hash, [c2, u], 'block-2u');
  return { blockP, blockU, chain, p, q, r, u, x };
};

type Fixture = ReturnType<typeof fixture>;

const setup = async (
  t: ExecutionContext,
  label: string,
  options: Parameters<typeof scratch>[2] = {}
) => {
  const env = await scratch(t, label, options);
  const store = await env.openStore();
  const nodes = await registerNodes(store);
  const fx = fixture();
  for (const block of [fx.chain.block0, fx.chain.block1]) {
    await store.saveBlock({
      block,
      isSavedTransaction: notSaved,
      nodeAcceptances: [
        acceptance(nodes.node1, at(0)),
        acceptance(nodes.node2, at(0)),
      ],
    });
  }
  const checker = createClickHouseChecker(env.client);
  return { ...env, checker, fx, nodes, store };
};

const outpoints = (
  rows: readonly { transactionHash: string; outputIndex: number }[]
) => rows.map((row) => `${row.transactionHash}:${row.outputIndex}`).sort();

/** Everything a reader of one node sees of the mempool and UTXOs. */
const mempoolView = async (checker: StoreChecker, node: string) => {
  const [mempool, history, unspent] = await Promise.all([
    checker.mempool(node),
    checker.transactionHistory(node),
    checker.unspent(node, {}),
  ]);
  return {
    history: history.map((row) => ({
      hash: row.hash,
      replacedAt: row.replacedAt?.toISOString() ?? null,
      validatedAt: row.validatedAt?.toISOString() ?? null,
    })),
    mempool: mempool
      .map((row) => `${row.hash}@${row.validatedAt?.toISOString() ?? 'null'}`)
      .sort(),
    unspent: outpoints(unspent),
  };
};

const settle = async (store: ClickHouseStore) => {
  await store.publishWatermarks();
};

/** The in-memory mempool of the writer (for a node) equals the stored one. */
const assertMemoryMatchesStore = async (
  t: ExecutionContext,
  store: ClickHouseStore,
  checker: StoreChecker,
  label: string
) => {
  const memory = await store.getMempoolTransactionsExpiringBefore({
    expirationMs: 0,
    expiresBefore: new Date('2100-01-01T00:00:00Z'),
  });
  for (const node of [nodeOne, nodeTwo]) {
    const inMemory = memory
      .filter((row) => row.nodeName === node)
      .map((row) => `${row.hash}@${row.validatedAt.toISOString()}`)
      .sort();
    t.deepEqual(
      inMemory,
      (await mempoolView(checker, node)).mempool,
      `${label}: ${node} in-memory mempool = stored mempool`
    );
  }
};

/** Per-node rows of `seq`: node, sign and whether version = commit_seq. */
const commitRows = async (client: ClickHouseClient, seq: bigint) => {
  const versioned = await client.query<{
    table: string;
    node: number;
    tx: string;
    sign: number;
    own: number;
  }>(
    `SELECT 'node_transaction' AS table, node_internal_id AS node, lower(hex(transaction_hash)) AS tx,
       sign, version = commit_seq AS own
     FROM node_transaction WHERE commit_seq = {seq:UInt64}
     UNION ALL
     SELECT 'tx_acceptance', node_internal_id, lower(hex(transaction_hash)), sign, version = commit_seq
     FROM tx_acceptance WHERE commit_seq = {seq:UInt64} AND block_internal_id = 0
     UNION ALL
     SELECT 'utxo', node_internal_id, concat(lower(hex(transaction_hash)), ':', toString(output_index)),
       sign, version = commit_seq
     FROM utxo WHERE commit_seq = {seq:UInt64}
     UNION ALL
     SELECT 'utxo_by_script', node_internal_id, concat(lower(hex(transaction_hash)), ':', toString(output_index)),
       sign, version = commit_seq
     FROM utxo_by_script WHERE commit_seq = {seq:UInt64}`,
    { seq }
  );
  const history = await client.query<{
    node: number;
    tx: string;
    replaced: string | null;
  }>(
    `SELECT h.node_internal_id AS node, lower(hex(t.hash)) AS tx, toString(h.replaced_at) AS replaced
     FROM node_transaction_history AS h
     INNER JOIN transaction AS t ON t.internal_id = h.transaction_internal_id
     WHERE h.commit_seq = {seq:UInt64}`,
    { seq }
  );
  return { history, versioned };
};

/** The seq of the latest commit of `kind`. */
const latestSeq = async (store: ClickHouseStore, kind: string) => {
  const commits = await store.commitLog.listCommits();
  const last = commits.filter((commit) => commit.kind === kind).pop();
  if (last === undefined) throw new Error(`no ${kind} commit`);
  return last.seq;
};

const short = (fx: Fixture) => {
  const names = new Map<string, string>();
  (['p', 'q', 'r', 'u', 'x'] as const).forEach((name) => {
    names.set(fx[name].hash, name);
  });
  names.set(fx.chain.a.hash, 'a');
  names.set(fx.chain.c1.hash, 'c1');
  names.set(fx.chain.c0.hash, 'c0');
  return (key: string) => {
    const [hash, index] = key.split(':');
    const name = names.get(hash!) ?? hash!.slice(0, 8);
    return index === undefined ? name : `${name}:${index}`;
  };
};

/** Log the rows of one commit (the end-state tables in wp5a-mempool.md). */
const logCommit = async (
  t: ExecutionContext,
  client: ClickHouseClient,
  fx: Fixture,
  seq: bigint,
  label: string
) => {
  const name = short(fx);
  const rows = await commitRows(client, seq);
  const lines = rows.versioned
    .map(
      (row) =>
        `${row.table} n${row.node} ${name(row.tx)} ${
          row.sign > 0 ? '+1' : '−1'
        }`
    )
    .sort();
  const history = rows.history
    .map(
      (row) =>
        `history n${row.node} ${name(row.tx)} replaced_at=${
          row.replaced ?? 'NULL'
        }`
    )
    .sort();
  t.log(`${label} (seq ${seq}): ${[...lines, ...history].join('; ')}`);
  t.true(
    rows.versioned.every((row) => row.own === 1),
    `${label}: every per-node row has version = commit_seq`
  );
  return { history, lines };
};

/* -------------------------------------------------------------------- */

e2e(
  '[e2e] mempool: replacement cascades per node; block and header acceptance clean only the accepting node',
  async (t) => {
    const { checker, client, fx, nodes, store } = await setup(t, 'replace');
    const name = short(fx);
    await store.saveMempoolTransaction(fx.p, [
      { nodeInternalId: nodes.node1, validatedAt: at(10) },
      { nodeInternalId: nodes.node2, validatedAt: at(11) },
    ]);
    const addP = await logCommit(
      t,
      client,
      fx,
      await latestSeq(store, 'mempool_batch'),
      'addition p (2 nodes)'
    );
    t.true(addP.lines.includes(`utxo n${nodes.node1} p:0 +1`));
    t.true(addP.lines.includes(`utxo n${nodes.node1} a:1 −1`));
    await store.saveMempoolTransaction(fx.q, [
      { nodeInternalId: nodes.node1, validatedAt: at(20) },
    ]);
    await store.recordNodeValidation(fx.q.hash, {
      nodeInternalId: nodes.node2,
      validatedAt: at(21),
    });
    await logCommit(
      t,
      client,
      fx,
      await latestSeq(store, 'mempool_batch'),
      'recordNodeValidation q (node 2)'
    );
    await settle(store);
    const node2Before = await mempoolView(checker, nodeTwo);
    t.deepEqual(
      node2Before.mempool,
      [
        `${fx.p.hash}@${at(11).toISOString()}`,
        `${fx.q.hash}@${at(21).toISOString()}`,
      ].sort()
    );

    // node 1 validates r, which replaces p (and q, its descendant)
    await store.saveMempoolTransaction(fx.r, [
      { nodeInternalId: nodes.node1, validatedAt: at(30) },
    ]);
    const replace = await logCommit(
      t,
      client,
      fx,
      await latestSeq(store, 'mempool_batch'),
      'replacement r ⇒ p replaced, q cascade (node 1)'
    );
    t.true(
      replace.lines.every((line) => line.includes(` n${nodes.node1} `)),
      'the replacement writes node 1 rows only'
    );
    await settle(store);
    const node1 = await mempoolView(checker, nodeOne);
    t.deepEqual(node1.mempool, [`${fx.r.hash}@${at(30).toISOString()}`]);
    t.deepEqual(
      node1.history.map((row) => [name(row.hash), row.replacedAt]),
      [
        ['p', at(30).toISOString()],
        ['q', at(30).toISOString()],
      ]
    );
    t.true(node1.unspent.includes(`${fx.r.hash}:0`));
    t.false(node1.unspent.includes(`${fx.p.hash}:0`));
    t.false(node1.unspent.includes(`${fx.q.hash}:0`));
    t.false(node1.unspent.includes(`${fx.chain.a.hash}:1`));
    t.deepEqual(
      await mempoolView(checker, nodeTwo),
      node2Before,
      'node 2 untouched'
    );
    await assertMemoryMatchesStore(t, store, checker, 'after replacement');

    // node 1 accepts a block confirming p: r conflicts (replaced_at = accepted_at)
    const node2BeforeBlock = await mempoolView(checker, nodeTwo);
    await store.saveBlock({
      block: fx.blockP,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(nodes.node1, at(40))],
    });
    await logCommit(
      t,
      client,
      fx,
      await latestSeq(store, 'block'),
      'block confirming p accepted by node 1 ⇒ r conflict'
    );
    await settle(store);
    const node1Block = await mempoolView(checker, nodeOne);
    t.deepEqual(node1Block.mempool, []);
    t.deepEqual(
      node1Block.history.map((row) => [name(row.hash), row.replacedAt]),
      [
        ['p', at(30).toISOString()],
        ['q', at(30).toISOString()],
        ['r', at(40).toISOString()],
      ]
    );
    t.true(node1Block.unspent.includes(`${fx.p.hash}:0`));
    t.false(node1Block.unspent.includes(`${fx.r.hash}:0`));
    t.deepEqual(await mempoolView(checker, nodeTwo), node2BeforeBlock);
    t.deepEqual(await checker.confirmedButInMempool(nodeOne), []);

    // node 2 accepts the same block via headers: p confirmed (NULL), q stays
    t.is(
      await store.acceptBlocksViaHeaders(
        nodes.node2,
        [{ hash: fx.blockP.hash, height: 2 }],
        at(50)
      ),
      1
    );
    await logCommit(
      t,
      client,
      fx,
      await latestSeq(store, 'header_accept'),
      'header acceptance by node 2 ⇒ p confirmed'
    );
    await settle(store);
    const node2After = await mempoolView(checker, nodeTwo);
    t.deepEqual(node2After.mempool, [`${fx.q.hash}@${at(21).toISOString()}`]);
    t.deepEqual(
      node2After.history.map((row) => [name(row.hash), row.replacedAt]),
      [['p', null]]
    );
    t.true(node2After.unspent.includes(`${fx.q.hash}:0`));
    t.false(node2After.unspent.includes(`${fx.p.hash}:0`));
    t.deepEqual(await checker.confirmedButInMempool(nodeTwo), []);
    t.deepEqual(await checker.orphanMempoolDescendants(nodeOne), []);
    t.deepEqual(await checker.orphanMempoolDescendants(nodeTwo), []);
    // the repair sweep finds nothing left
    t.deepEqual(await store.archiveMempoolTransactionsAcceptedByBlocks(), []);
    t.deepEqual(await badUtxoSums(client), []);
    await assertMemoryMatchesStore(t, store, checker, 'after blocks');
  }
);

e2e(
  '[e2e] mempool: expiry archives the entry and its descendants for one node only',
  async (t) => {
    const { checker, client, fx, nodes, store } = await setup(t, 'expiry');
    const name = short(fx);
    await store.saveMempoolTransaction(fx.p, [
      { nodeInternalId: nodes.node1, validatedAt: at(10) },
      { nodeInternalId: nodes.node2, validatedAt: at(10) },
    ]);
    await store.saveMempoolTransaction(fx.q, [
      { nodeInternalId: nodes.node1, validatedAt: at(100) },
      { nodeInternalId: nodes.node2, validatedAt: at(100) },
    ]);
    await settle(store);
    const node1Before = await mempoolView(checker, nodeOne);
    const expiring = await store.getMempoolTransactionsExpiringBefore({
      expirationMs: 30_000,
      expiresBefore: at(60),
    });
    t.deepEqual(
      expiring.map((row) => [
        row.nodeName,
        name(row.hash),
        row.expiresAt.toISOString(),
      ]),
      [
        [nodeOne, 'p', at(40).toISOString()],
        [nodeTwo, 'p', at(40).toISOString()],
      ]
    );
    const p2 = expiring.find((row) => row.nodeName === nodeTwo)!;
    t.is(
      await store.archiveMempoolTransaction({
        nodeInternalId: nodes.node2,
        replacedAt: at(61),
        transactionInternalId: p2.transactionInternalId,
      }),
      1
    );
    await logCommit(
      t,
      client,
      fx,
      await latestSeq(store, 'expiry'),
      'expiry of p (node 2) ⇒ q cascade'
    );
    t.is(
      await store.archiveMempoolTransaction({
        nodeInternalId: nodes.node2,
        replacedAt: at(62),
        transactionInternalId: p2.transactionInternalId,
      }),
      0,
      'a second expiry is a no-op'
    );
    await settle(store);
    const node2 = await mempoolView(checker, nodeTwo);
    t.deepEqual(node2.mempool, []);
    t.deepEqual(
      node2.history.map((row) => [
        name(row.hash),
        row.validatedAt,
        row.replacedAt,
      ]),
      [
        ['p', at(10).toISOString(), at(61).toISOString()],
        ['q', at(100).toISOString(), at(61).toISOString()],
      ]
    );
    t.true(node2.unspent.includes(`${fx.chain.a.hash}:1`));
    t.false(node2.unspent.includes(`${fx.p.hash}:0`));
    t.false(node2.unspent.includes(`${fx.q.hash}:0`));
    t.deepEqual(await mempoolView(checker, nodeOne), node1Before);
    t.deepEqual(await badUtxoSums(client), []);
    await assertMemoryMatchesStore(t, store, checker, 'after expiry');
  }
);

e2e(
  '[e2e] mempool: an orphan waits for its parent (mempool or block) without a commit',
  async (t) => {
    const { checker, client, fx, nodes, store } = await setup(t, 'orphan', {
      orphanGraceMs: 30_000,
    });
    // q before its parent p: parked, no commit
    const commitsBefore = (await store.commitLog.listCommits()).length;
    const qSaved = store.saveMempoolTransaction(fx.q, [
      { nodeInternalId: nodes.node1, validatedAt: at(20) },
    ]);
    await new Promise((resolve) => {
      setTimeout(resolve, 200);
    });
    t.true(store.mempool.orphans.has(fx.q.hash), 'q is parked');
    t.is(
      (await store.commitLog.listCommits()).length,
      commitsBefore,
      'parking writes no commit'
    );
    t.false(await checker.transactionExists(fx.q.hash));
    // a second node's validation of the parked q merges into the orphan
    const q2Saved = store.saveMempoolTransaction(fx.q, [
      { nodeInternalId: nodes.node2, validatedAt: at(22) },
    ]);
    await store.saveMempoolTransaction(fx.p, [
      { nodeInternalId: nodes.node1, validatedAt: at(10) },
      { nodeInternalId: nodes.node2, validatedAt: at(11) },
    ]);
    await Promise.all([qSaved, q2Saved]);
    t.false(store.mempool.orphans.has(fx.q.hash));
    await settle(store);
    for (const [node, qAt] of [
      [nodeOne, at(20)],
      [nodeTwo, at(22)],
    ] as const) {
      const view = await mempoolView(checker, node);
      t.true(view.mempool.includes(`${fx.q.hash}@${qAt.toISOString()}`));
      t.true(view.unspent.includes(`${fx.q.hash}:0`));
      t.false(view.unspent.includes(`${fx.p.hash}:0`));
    }

    // x spends u:0, u is first seen in a block: x waits; the block releases it
    const xSaved = store.saveMempoolTransaction(fx.x, [
      { nodeInternalId: nodes.node1, validatedAt: at(30) },
    ]);
    await new Promise((resolve) => {
      setTimeout(resolve, 200);
    });
    t.true(store.mempool.orphans.has(fx.x.hash));
    await store.saveBlock({
      block: fx.blockU,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(nodes.node1, at(31))],
    });
    await xSaved;
    await settle(store);
    const node1 = await mempoolView(checker, nodeOne);
    t.true(node1.mempool.includes(`${fx.x.hash}@${at(30).toISOString()}`));
    t.true(node1.unspent.includes(`${fx.x.hash}:0`));
    t.false(node1.unspent.includes(`${fx.u.hash}:0`));
    t.deepEqual(await badUtxoSums(client), []);
    await assertMemoryMatchesStore(t, store, checker, 'after orphans');
  }
);

e2e(
  '[e2e] mempool: an orphan whose parent never arrives is saved after the grace period; a later block resolves its spend',
  async (t) => {
    const { checker, client, fx, nodes, store } = await setup(t, 'grace', {
      orphanGraceMs: 300,
    });
    const started = Date.now();
    await store.saveMempoolTransaction(fx.x, [
      { nodeInternalId: nodes.node1, validatedAt: at(30) },
    ]);
    t.true(Date.now() - started >= 250, 'x waited for the grace period');
    await settle(store);
    let node1 = await mempoolView(checker, nodeOne);
    t.true(node1.mempool.includes(`${fx.x.hash}@${at(30).toISOString()}`));
    t.true(node1.unspent.includes(`${fx.x.hash}:0`));
    const pending = async () =>
      client.query<{ node: number; live: string }>(
        `SELECT node_internal_id AS node, toString(sum(sign)) AS live FROM pending_spend
         WHERE spender_transaction_hash = toFixedString(unhex({hash:String}), 32)
         GROUP BY node`,
        { hash: fx.x.hash }
      );
    t.deepEqual(await pending(), [{ live: '1', node: nodes.node1 }]);
    await logCommit(
      t,
      client,
      fx,
      await latestSeq(store, 'mempool_batch'),
      'addition x with unknown parent (after grace)'
    );
    // the parent arrives in a block node 1 accepts: x's −1 for u:0 is written then
    await store.saveBlock({
      block: fx.blockU,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(nodes.node1, at(40))],
    });
    await logCommit(
      t,
      client,
      fx,
      await latestSeq(store, 'block'),
      'block creating u ⇒ x spend resolved'
    );
    await settle(store);
    node1 = await mempoolView(checker, nodeOne);
    t.false(node1.unspent.includes(`${fx.u.hash}:0`));
    t.true(node1.unspent.includes(`${fx.x.hash}:0`));
    t.deepEqual(await pending(), [{ live: '0', node: nodes.node1 }]);
    t.deepEqual(await badUtxoSums(client), []);
  }
);

e2e(
  '[e2e] mempool: two nodes validating the same transaction concurrently share one id and one set of base rows',
  async (t) => {
    const { checker, client, fx, nodes, store } = await setup(t, 'concurrent');
    await Promise.all([
      store.saveMempoolTransaction(fx.p, [
        { nodeInternalId: nodes.node1, validatedAt: at(10) },
      ]),
      store.saveMempoolTransaction(fx.p, [
        { nodeInternalId: nodes.node2, validatedAt: at(11) },
      ]),
      store.recordNodeValidation(fx.p.hash, {
        nodeInternalId: nodes.node2,
        validatedAt: at(12),
      }),
      store.saveMempoolTransaction(fx.q, [
        { nodeInternalId: nodes.node1, validatedAt: at(20) },
        { nodeInternalId: nodes.node2, validatedAt: at(20) },
      ]),
    ]);
    await settle(store);
    t.is(await checker.transactionRowCount(fx.p.hash), 1);
    const outputs = await client.query<{ n: string }>(
      `SELECT toString(count()) AS n FROM output
       WHERE transaction_hash = toFixedString(unhex({hash:String}), 32)`,
      { hash: fx.p.hash }
    );
    t.deepEqual(outputs, [{ n: '1' }], 'one output row');
    const node1 = await mempoolView(checker, nodeOne);
    const node2 = await mempoolView(checker, nodeTwo);
    t.true(node1.mempool.includes(`${fx.p.hash}@${at(10).toISOString()}`));
    /* node 2: the first validation in call order wins (ON CONFLICT DO NOTHING) */
    t.true(node2.mempool.includes(`${fx.p.hash}@${at(11).toISOString()}`));
    t.deepEqual(node1.unspent, node2.unspent);
    t.true(node1.unspent.includes(`${fx.q.hash}:0`));
    t.false(node1.unspent.includes(`${fx.p.hash}:0`));
    t.deepEqual(await checker.validatingNodes(fx.p.hash), [nodeOne, nodeTwo]);
    t.deepEqual(await badUtxoSums(client), []);
    await assertMemoryMatchesStore(t, store, checker, 'after concurrency');
  }
);

/* -------------------------------------------------------------------- */
/* crash injection                                                        */
/* -------------------------------------------------------------------- */

interface CrashScenario {
  name: string;
  kind: string;
  run: (
    store: ClickHouseStore,
    nodes: { node1: number; node2: number }
  ) => Promise<unknown>;
}

const crashScenarios = (fx: Fixture): CrashScenario[] => [
  {
    kind: 'mempool_batch',
    name: 'save q for both nodes (new tx)',
    run: async (store, nodes) =>
      store.saveMempoolTransaction(fx.q, [
        { nodeInternalId: nodes.node1, validatedAt: at(20) },
        { nodeInternalId: nodes.node2, validatedAt: at(20) },
      ]),
  },
  {
    kind: 'mempool_batch',
    name: 'save r for node 1 (replacement + cascade)',
    run: async (store, nodes) =>
      store.saveMempoolTransaction(fx.r, [
        { nodeInternalId: nodes.node1, validatedAt: at(30) },
      ]),
  },
  {
    kind: 'block',
    name: 'block confirming p for node 2 (cleanup in the block commit)',
    run: async (store, nodes) =>
      store.saveBlock({
        block: fx.blockP,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(nodes.node2, at(40))],
      }),
  },
  {
    kind: 'expiry',
    name: 'expiry of q for node 2',
    run: async (store, nodes) => {
      const entry = (
        await store.getMempoolTransactionsExpiringBefore({
          expirationMs: 0,
          expiresBefore: new Date('2100-01-01T00:00:00Z'),
        })
      ).find(
        (row) => row.nodeInternalId === nodes.node2 && row.hash === fx.q.hash
      )!;
      return store.archiveMempoolTransaction({
        nodeInternalId: nodes.node2,
        replacedAt: at(50),
        transactionInternalId: entry.transactionInternalId,
      });
    },
  },
];

e2e(
  '[e2e] mempool: a crash between any two steps of a mempool commit leaves each node all-or-none, before and after recovery',
  async (t) => {
    t.timeout(600_000);
    const { checker, client, fx, nodes, openStore, store } = await setup(
      t,
      'mcrash'
    );
    await store.saveMempoolTransaction(fx.p, [
      { nodeInternalId: nodes.node1, validatedAt: at(10) },
      { nodeInternalId: nodes.node2, validatedAt: at(10) },
    ]);
    await settle(store);
    await store.close();
    const views = async () => ({
      [nodeOne]: await mempoolView(checker, nodeOne),
      [nodeTwo]: await mempoolView(checker, nodeTwo),
    });
    let crashPoints = 0;
    for (const scenario of crashScenarios(fx)) {
      const before = await views();
      let after: Awaited<ReturnType<typeof views>> | undefined;
      for (let crashAt = 0; after === undefined; crashAt += 1) {
        let calls = 0;
        let step = '';
        const crashing = await openStore((name, context) => {
          if (context.kind !== scenario.kind) return;
          if (calls === crashAt) {
            step = name;
            throw new SimulatedCrash(`crash after ${scenario.kind}:${name}`);
          }
          calls += 1;
        });
        const outcome = await scenario
          .run(crashing, nodes)
          .then(() => 'ok' as const)
          .catch((error: unknown) => {
            if (error instanceof SimulatedCrash) return 'crash' as const;
            throw error;
          });
        if (outcome === 'ok') {
          throw new Error(`${scenario.name}: ran past every crash point`);
        }
        crashPoints += 1;
        await crashing.simulateCrash();
        const label = `${scenario.name}: crash after ${step}`;
        const committed = step === 'committed';
        const seen = await views();
        if (!committed) {
          t.deepEqual(seen, before, `${label}: before recovery`);
        }
        await new Promise((resolve) => {
          setTimeout(resolve, leaseTtlMs + 100);
        });
        const recovered = await openStore();
        await recovered.publishWatermarks();
        const recoveredViews = await views();
        if (committed) {
          t.notDeepEqual(recoveredViews, before, `${label}: committed`);
          after = recoveredViews;
          t.true(
            JSON.stringify(seen) === JSON.stringify(before) ||
              JSON.stringify(seen) === JSON.stringify(after),
            `${label}: before recovery all-or-none`
          );
        } else {
          t.deepEqual(recoveredViews, before, `${label}: after recovery`);
        }
        await assertMemoryMatchesStore(t, recovered, checker, label);
        t.deepEqual(await badUtxoSums(client), [], `${label}: utxo sums`);
        await recovered.close();
      }
    }
    t.log(`${crashPoints} crash points`);
  }
);
