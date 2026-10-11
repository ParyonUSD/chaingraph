// cspell:ignore clickhouse
import test from 'ava';

import type { MempoolTxFacts } from './mempool-state.js';
import { MempoolState } from './mempool-state.js';

test('MempoolState: an empty mempool plans nothing', (t) => {
  const state = new MempoolState();
  t.true(state.isEmpty(1));
  const change = state.planBlockAcceptance(
    1,
    [],
    () => undefined,
    () => undefined
  );
  t.deepEqual(change, { archives: [], node: 1, resolutions: [] });
  t.is(state.nodeMempool(1).txs.size, 0);
});

test('MempoolState: with UTXO off (no spend tracking) entries have no outstanding spends', (t) => {
  const facts: MempoolTxFacts = {
    hash: 'child',
    internalId: 2n,
    outputs: [],
    spends: ['parent:0', 'parent:1'],
    spent: new Map(),
  };
  const off = new MempoolState(false);
  t.false(off.trackSpends);
  t.deepEqual([...off.entryFor(facts, new Date(0)).unresolved], []);
  const on = new MempoolState();
  t.deepEqual(
    [...on.entryFor(facts, new Date(0)).unresolved],
    ['parent:0', 'parent:1']
  );
});
