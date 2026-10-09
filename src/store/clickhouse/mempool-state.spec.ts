// cspell:ignore clickhouse
import test from 'ava';

import { MempoolNotImplementedError, MempoolState } from './mempool-state.js';

test('MempoolState: an empty mempool plans nothing and never builds inclusions', (t) => {
  const state = new MempoolState();
  t.true(state.isEmpty(1));
  t.deepEqual(
    state.planBlockAcceptance(1, () => {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error('inclusions must not be built for an empty mempool');
    }),
    []
  );
  t.notThrows(() => {
    state.assertNoMempoolForHeaderAcceptance(1);
  });
  t.is(state.nodeMempool(1).txs.size, 0);
  t.is(state.orphans.size, 0);
  t.true(
    new MempoolNotImplementedError('x').message.startsWith('WP5a-mempool')
  );
});
