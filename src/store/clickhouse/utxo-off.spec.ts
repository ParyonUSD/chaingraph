/* eslint-disable @typescript-eslint/no-magic-numbers, functional/no-loop-statement, no-await-in-loop, functional/no-throw-statement, functional/no-let */
// cspell:ignore clickhouse utxooff
/**
 * `CHAINGRAPH_CLICKHOUSE_UTXO=off` (docs/clickhouse-port/utxo-off.md): the
 * same history (bulk horizon, a 3-block chain for 2 nodes with spends, a
 * re-org, a header re-acceptance and a mempool replacement) on two
 * databases, one with stored UTXO tables (on) and one without (off). Off
 * writes no `utxo` / `utxo_by_script` row and no `utxo_build` commit, and
 * the checker's query-time `unspent` per node equals the on-mode answer.
 */
import type { ExecutionContext } from 'ava';
import test from 'ava';

import type { CheckerOutpoint } from '../checker.js';

import { createClickHouseChecker } from './checker.js';
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

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const at = (seconds: number) =>
  new Date(Date.UTC(2026, 9, 10, 12, 0, 0) + seconds * 1_000);

const fixture = () => {
  const chain = threeBlockChain();
  /** p spends a:1 (unspent in the chain); r spends a:1 too (replaces p) */
  const p = makeTx({
    label: 'utxo-off-p',
    outputs: [{ lockingBytecode: p2pkh('p'), valueSatoshis: 1_000n }],
    spends: [[chain.a.hash, 1]],
  });
  const r = makeTx({
    label: 'utxo-off-r',
    outputs: [{ lockingBytecode: p2pkh('r'), valueSatoshis: 950n }],
    spends: [[chain.a.hash, 1]],
  });
  const c2alt = makeTx({
    coinbase: true,
    label: 'utxo-off-c2-alt',
    outputs: [
      { lockingBytecode: p2pkh('other-miner'), valueSatoshis: 5_000_000_000n },
    ],
  });
  const block2alt = makeBlock(
    2,
    chain.block1.hash,
    [c2alt],
    'utxo-off-block-2-alt'
  );
  return { block2alt, chain, p, r };
};

const finishHooks = {
  onIndexProgress: () => undefined,
  onNonFatalError: (error: unknown) => {
    throw error;
  },
  onSyncSettingsRestored: () => undefined,
};

/** Run the scenario on a fresh database; returns what the checker sees. */
const runScenario = async (t: ExecutionContext, utxo: 'off' | 'on') => {
  const { client, name, openStore } = await scratch(
    t,
    utxo,
    { utxo },
    'ch1_utxooff'
  );
  const fx = fixture();
  const { chain } = fx;
  let store: ClickHouseStore = await openStore();
  const { node1, node2 } = await registerNodes(store);
  const both = [acceptance(node1, at(0)), acceptance(node2, at(0))];
  // block 0 in bulk mode (initial sync), then the horizon switch back to tip
  t.false(await store.prepareForInitialSync());
  await store.saveBlock({
    block: chain.block0,
    isSavedTransaction: notSaved,
    nodeAcceptances: both,
  });
  await store.finishInitialSync(finishHooks);
  t.is(store.storeMode, 'tip');
  // a restart after the sync stays in tip mode (off: the bulk_exit switch)
  await store.close();
  store = await openStore();
  t.is(store.storeMode, 'tip', `${utxo}: tip mode after restart`);
  for (const block of [chain.block1, chain.block2]) {
    await store.saveBlock({
      block,
      isSavedTransaction: notSaved,
      nodeAcceptances: both,
    });
  }
  // mempool: both nodes hold p; node 1 then validates r, which replaces p
  await store.saveMempoolTransaction(fx.p, [
    { nodeInternalId: node1, validatedAt: at(10) },
    { nodeInternalId: node2, validatedAt: at(10) },
  ]);
  await store.saveMempoolTransaction(fx.r, [
    { nodeInternalId: node1, validatedAt: at(20) },
  ]);
  // re-org of node 1 onto block 2' (no b), then back to block 2 via headers
  await store.removeStaleBlocksForNode(node1, [chain.block2.hash], at(30));
  await store.saveBlock({
    block: fx.block2alt,
    isSavedTransaction: notSaved,
    nodeAcceptances: [acceptance(node1, at(31))],
  });
  await store.publishWatermarks();
  const checker = createClickHouseChecker(client, name, { utxo });
  const outpoints = (rows: CheckerOutpoint[]) =>
    rows.map((row) => `${row.transactionHash}:${row.outputIndex}`);
  const snapshot = async () => ({
    node1: outpoints(await checker.unspent('node-one', {})),
    node1Category: outpoints(await checker.unspent('node-one', { category })),
    node1Script: outpoints(
      await checker.unspent('node-one', { lockingBytecode: p2pkh('r') })
    ),
    node2: outpoints(await checker.unspent('node-two', {})),
    node2Category: outpoints(await checker.unspent('node-two', { category })),
    node2Script: outpoints(
      await checker.unspent('node-two', { lockingBytecode: p2pkh('p') })
    ),
    unknownNode: outpoints(await checker.unspent('no-such-node', {})),
  });
  const onAlt = await snapshot();
  await store.removeStaleBlocksForNode(node1, [fx.block2alt.hash], at(40));
  t.is(
    await store.acceptBlocksViaHeaders(
      node1,
      [{ hash: chain.block2.hash, height: 2 }],
      at(41)
    ),
    1
  );
  await store.publishWatermarks();
  const back = await snapshot();
  const [counts] = await client.query<{ utxo: string; byScript: string }>(
    `SELECT (SELECT count() FROM utxo) AS utxo, (SELECT count() FROM utxo_by_script) AS byScript`
  );
  const [builds] = await client.query<{ n: string }>(
    `SELECT count() AS n FROM commit_log WHERE kind = 'utxo_build'`
  );
  return {
    back,
    builds: Number(builds?.n),
    fx,
    onAlt,
    utxoRows: Number(counts?.utxo) + Number(counts?.byScript),
  };
};

e2e(
  '[e2e] utxo off: no utxo rows on any path; checker.unspent per node equals the on-mode answer',
  async (t) => {
    t.timeout(180_000);
    const on = await runScenario(t, 'on');
    const off = await runScenario(t, 'off');
    t.true(on.utxoRows > 0, 'on: the stored UTXO tables are written');
    t.is(on.builds, 1, 'on: one utxo_build commit');
    t.is(off.utxoRows, 0, 'off: utxo and utxo_by_script stay empty');
    t.is(off.builds, 0, 'off: no utxo_build commit');
    t.deepEqual(off.onAlt, on.onAlt, 'unspent per node on block 2 prime');
    t.deepEqual(off.back, on.back, 'unspent per node after re-acceptance');
    // sanity: the answers are per node and reflect the history
    const { chain, p, r } = off.fx;
    const key = (hash: string, index: number) => `${hash}:${index}`;
    t.true(off.onAlt.node1.includes(key(chain.a.hash, 0)), 'b released');
    t.false(off.onAlt.node2.includes(key(chain.a.hash, 0)), 'b spends a:0');
    t.true(off.back.node1.includes(key(r.hash, 0)), 'node 1 holds r');
    t.false(off.back.node1.includes(key(p.hash, 0)), 'p replaced on node 1');
    t.true(off.back.node2.includes(key(p.hash, 0)), 'node 2 still holds p');
    t.false(off.back.node2.includes(key(chain.a.hash, 1)), 'p spends a:1');
    t.deepEqual(off.back.node1Script, [key(r.hash, 0)]);
    t.deepEqual(off.back.node2Script, [key(p.hash, 0)]);
    t.deepEqual(off.back.unknownNode, []);
  }
);
