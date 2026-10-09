// cspell:ignore clickhouse
import test from 'ava';

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
  t.is(state.orphans.size, 0);
});
