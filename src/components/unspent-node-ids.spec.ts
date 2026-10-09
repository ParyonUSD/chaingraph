/* eslint-disable @typescript-eslint/no-magic-numbers, camelcase, @typescript-eslint/naming-convention */
import test from 'ava';

import {
  batchDidWork,
  batchReachedLimits,
  configureTrackingTriggersSql,
  coveringBytecodeMaxBytes,
  nextSkipThrough,
  nextStallState,
  nodeIndexDefinitions,
  nodeIndexNamePattern,
  parseBatchResult,
  partitionOf,
} from './unspent-node-ids.js';

test('nodeIndexDefinitions: three partial indexes per node, keyed on the node id', (t) => {
  const definitions = nodeIndexDefinitions(7);
  t.deepEqual(Object.keys(definitions), [
    'output_unspent_node_7_category_index',
    'output_unspent_node_7_category_rest_index',
    'output_unspent_node_7_search_index',
  ]);
  Object.entries(definitions).forEach(([name, definition]) => {
    t.true(nodeIndexNamePattern.test(name));
    t.true(definition.startsWith(`CREATE INDEX ${name} ON output`));
    t.true(definition.includes('WHERE 7 = ANY (unspent_node_ids)'));
    t.false(
      definition.includes('unspent_node_ids IS NULL'),
      'no NULL (unprocessed) index'
    );
  });
  t.true(
    definitions.output_unspent_node_7_category_index!.includes(
      'INCLUDE (transaction_hash, output_index, value_satoshis, fungible_token_amount, nonfungible_token_commitment, locking_bytecode)'
    )
  );
  t.true(
    definitions.output_unspent_node_7_category_index!.includes(
      `octet_length(locking_bytecode) <= ${coveringBytecodeMaxBytes}`
    )
  );
  t.true(
    definitions.output_unspent_node_7_category_rest_index!.includes(
      `(token_category IS NULL OR octet_length(locking_bytecode) > ${coveringBytecodeMaxBytes})`
    )
  );
  t.true(
    definitions.output_unspent_node_7_search_index!.includes(
      'substring(locking_bytecode, 0, 26)'
    )
  );
});

test('configureTrackingTriggersSql: only triggers whose state differs', (t) => {
  t.deepEqual(
    configureTrackingTriggersSql(true, {
      trigger_unspent_tracking_node_block_delete: false,
      trigger_unspent_tracking_node_transaction_delete: true,
    }),
    [
      'ALTER TABLE node_block ENABLE TRIGGER trigger_unspent_tracking_node_block_delete;',
    ]
  );
  t.deepEqual(
    configureTrackingTriggersSql(false, {
      trigger_unspent_tracking_node_block_delete: true,
      trigger_unspent_tracking_node_transaction_delete: true,
    }),
    [
      'ALTER TABLE node_block DISABLE TRIGGER trigger_unspent_tracking_node_block_delete;',
      'ALTER TABLE node_transaction DISABLE TRIGGER trigger_unspent_tracking_node_transaction_delete;',
    ]
  );
  t.deepEqual(configureTrackingTriggersSql(true, {}), []);
});

test('partitionOf: matches ((get_byte(hash, 0) * n) >> 8)', (t) => {
  t.is(partitionOf('00ff', 4), 0);
  t.is(partitionOf('3fff', 4), 0);
  t.is(partitionOf('40ff', 4), 1);
  t.is(partitionOf('ff00', 4), 3);
  t.is(partitionOf('ff00', 1), 0);
  t.is(partitionOf('80', 3), 1);
  const counts = [0, 0, 0, 0, 0, 0, 0, 0];
  Array.from({ length: 256 }, (_, byte) => byte).forEach((byte) => {
    counts[partitionOf(byte.toString(16).padStart(2, '0'), 8)]! += 1;
  });
  t.deepEqual(counts, [32, 32, 32, 32, 32, 32, 32, 32]);
});

test('parseBatchResult / batchDidWork / batchReachedLimits', (t) => {
  const idle = parseBatchResult({
    blockWatermark: 5,
    inputWatermark: 10,
    previousBlockWatermark: 5,
    previousInputWatermark: 10,
    stalledAt: null,
  });
  t.false(batchDidWork(idle));
  t.is(idle.stalledAt, null);
  t.true(batchReachedLimits(idle, { blockLimit: 5, transactionLimit: 10 }));
  t.false(batchReachedLimits(idle, { blockLimit: 6, transactionLimit: 10 }));
  const worked = parseBatchResult({
    events: 2,
    inputWatermark: 10,
    previousInputWatermark: 10,
    stalledAt: 11,
  });
  t.true(batchDidWork(worked));
  t.is(worked.stalledAt, 11);
  t.deepEqual(parseBatchResult({ busy: true }).busy, true);
  t.deepEqual(parseBatchResult({ repartition: true }).repartition, true);
});

test('stall bookkeeping: skip only after the stall limit, monotonic', (t) => {
  const first = nextStallState(undefined, 42, 1_000);
  t.deepEqual(first, { since: 1_000, transactionInternalId: 42 });
  t.is(nextStallState(first, 42, 2_000), first);
  t.deepEqual(nextStallState(first, 43, 2_000), {
    since: 2_000,
    transactionInternalId: 43,
  });
  t.is(nextStallState(first, null, 2_000), undefined);
  t.is(nextSkipThrough(0, first, 1_500, 1_000, 100), 0);
  t.is(nextSkipThrough(0, first, 2_000, 1_000, 100), 100);
  t.is(nextSkipThrough(150, first, 2_000, 1_000, 100), 150);
  t.is(nextSkipThrough(150, undefined, 9_000, 1_000, 200), 150);
});
