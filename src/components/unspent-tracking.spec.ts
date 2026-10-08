/* eslint-disable @typescript-eslint/no-magic-numbers */
import test from 'ava';

import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../types/chaingraph.js';

import {
  copyStageTableSql,
  encodeStageSpends,
  stageTableColumns,
} from './block-copy-rows.js';
import { decodeBinaryCopy } from './pg-binary-copy.js';
import {
  buildDeleteSpentFromSetSql,
  buildMarkSpentOutputsSql,
  buildResolveNewOutputsSql,
  collectBlockSpends,
  configureUnspentTrackingTriggersSql,
  outputMarkerInsertParts,
  reacceptSpendsSql,
  resolveMempoolOutputsSql,
  resolveStagedNewOutputsSql,
  unspentSetInsertCte,
  unspentTrackingTriggerNames,
} from './unspent-tracking.js';

const hash = (byte: string) => byte.repeat(32);

const transaction = (
  byte: string,
  spends: [string, number][],
  isCoinbase = false
): ChaingraphTransaction => ({
  hash: hash(byte),
  inputs: spends.map(([outpointByte, outpointIndex]) => ({
    outpointIndex,
    outpointTransactionHash: hash(outpointByte),
    sequenceNumber: 0,
    unlockingBytecode: '51',
  })),
  isCoinbase,
  locktime: 0,
  outputs: [{ lockingBytecode: '51', valueSatoshis: 1n }],
  sizeBytes: 60,
  version: 2,
});

const block = {
  transactions: [
    transaction('aa', [['00', 0xffffffff]], true),
    transaction('bb', [
      ['11', 0],
      ['22', 3],
    ]),
    // spends an output created earlier in the same block
    transaction('cc', [['bb', 0]]),
  ],
} as unknown as ChaingraphBlock;

test('collectBlockSpends: every non-coinbase input, with its spender', (t) => {
  t.deepEqual(collectBlockSpends(block), [
    {
      outpointIndex: 0,
      outpointTransactionHash: hash('11'),
      spenderHash: hash('bb'),
    },
    {
      outpointIndex: 3,
      outpointTransactionHash: hash('22'),
      spenderHash: hash('bb'),
    },
    {
      outpointIndex: 0,
      outpointTransactionHash: hash('bb'),
      spenderHash: hash('cc'),
    },
  ]);
});

test('buildMarkSpentOutputsSql: one batched UPDATE over VALUES', (t) => {
  t.is(buildMarkSpentOutputsSql([]), undefined);
  const sql = buildMarkSpentOutputsSql(collectBlockSpends(block))!;
  t.is(sql.match(/UPDATE output/gu)?.length, 1);
  t.true(
    sql.includes(
      `('\\x${hash('11')}'::bytea, 0::bigint, '\\x${hash(
        'bb'
      )}'::bytea),('\\x${hash('22')}'::bytea, 3::bigint, '\\x${hash(
        'bb'
      )}'::bytea),('\\x${hash('bb')}'::bytea, 0::bigint, '\\x${hash(
        'cc'
      )}'::bytea)`
    ),
    sql
  );
  t.true(
    sql.includes('SET spent_by_transaction_internal_id = spender.internal_id')
  );
  t.true(
    sql.includes(
      'ELSE NOT unspent_tracking_transaction_is_block_accepted(o.spent_by_transaction_internal_id)'
    ),
    'a block-accepted marker is never overwritten'
  );
  t.false(sql.includes(hash('00')), 'coinbase excluded');
});

test('buildDeleteSpentFromSetSql: one batched DELETE USING VALUES', (t) => {
  t.is(buildDeleteSpentFromSetSql([]), undefined);
  const sql = buildDeleteSpentFromSetSql(collectBlockSpends(block))!;
  t.is(sql.match(/DELETE FROM unspent_output_set/gu)?.length, 1);
  t.true(
    sql.includes(
      `('\\x${hash('11')}'::bytea, 0::bigint),('\\x${hash(
        '22'
      )}'::bytea, 3::bigint),('\\x${hash('bb')}'::bytea, 0::bigint)`
    ),
    sql
  );
});

test('outputMarkerInsertParts: only marker mode writes the column', (t) => {
  t.deepEqual(outputMarkerInsertParts('off'), { column: '', value: '' });
  t.deepEqual(outputMarkerInsertParts('settable'), { column: '', value: '' });
  t.deepEqual(outputMarkerInsertParts('marker'), {
    column: ', spent_by_transaction_internal_id',
    value: ', 0::bigint',
  });
});

test('unspentSetInsertCte: only settable mode inserts set rows', (t) => {
  t.is(unspentSetInsertCte('off', 'src', 'new'), '');
  t.is(unspentSetInsertCte('marker', 'src', 'new'), '');
  const cte = unspentSetInsertCte('settable', 'src', 'new');
  t.true(cte.startsWith(',\nnewly_unspent_outputs AS ('));
  t.true(cte.includes('INSERT INTO unspent_output_set'));
  t.true(cte.includes('substring(locking_bytecode::bytea, 0, 26) FROM src'));
  t.true(cte.includes('WHERE transaction_hash IN (SELECT hash FROM new)'));
});

test('reacceptSpendsSql: marker fills unset markers, settable deletes', (t) => {
  t.is(reacceptSpendsSql('off'), undefined);
  t.regex(
    reacceptSpendsSql('marker')!,
    /spent_by_transaction_internal_id IS NULL OR o\.spent_by_transaction_internal_id = 0/u
  );
  t.regex(reacceptSpendsSql('settable')!, /DELETE FROM unspent_output_set/u);
});

test('configureUnspentTrackingTriggersSql: enables only the mode pair', (t) => {
  const allDisabled = Object.fromEntries(
    unspentTrackingTriggerNames.map((name) => [name, false])
  );
  t.deepEqual(configureUnspentTrackingTriggersSql('off', {}), []);
  t.deepEqual(configureUnspentTrackingTriggersSql('off', allDisabled), []);
  t.deepEqual(configureUnspentTrackingTriggersSql('marker', allDisabled), [
    'ALTER TABLE node_block ENABLE TRIGGER trigger_unspent_marker_node_block_delete;',
    'ALTER TABLE node_transaction ENABLE TRIGGER trigger_unspent_marker_node_transaction_delete;',
  ]);
  t.deepEqual(
    configureUnspentTrackingTriggersSql('settable', {
      ...allDisabled,
      // eslint-disable-next-line @typescript-eslint/naming-convention, camelcase
      trigger_unspent_marker_node_block_delete: true,
    }),
    [
      'ALTER TABLE node_block DISABLE TRIGGER trigger_unspent_marker_node_block_delete;',
      'ALTER TABLE node_block ENABLE TRIGGER trigger_unspent_settable_node_block_delete;',
      'ALTER TABLE node_transaction ENABLE TRIGGER trigger_unspent_settable_node_transaction_delete;',
    ]
  );
});

test('encodeStageSpends: binary rows match the staging table', (t) => {
  t.is(
    copyStageTableSql('chaingraph_stage_spend'),
    'COPY pg_temp.chaingraph_stage_spend (outpoint_transaction_hash, outpoint_index, spender_hash) FROM STDIN (FORMAT binary)'
  );
  const rows = decodeBinaryCopy(encodeStageSpends(collectBlockSpends(block)));
  t.is(rows[0]!.length, stageTableColumns.chaingraph_stage_spend.length);
  t.deepEqual(
    rows.map(([outpoint, index, spender]) => [
      outpoint!.toString('hex'),
      index!.readBigInt64BE(0),
      spender!.toString('hex'),
    ]),
    [
      [hash('11'), 0n, hash('bb')],
      [hash('22'), 3n, hash('bb')],
      [hash('bb'), 0n, hash('cc')],
    ]
  );
});

test('resolve (policy A): one batched statement over the new transactions', (t) => {
  t.is(buildResolveNewOutputsSql('marker', []), undefined);
  t.is(resolveStagedNewOutputsSql('off'), undefined);
  const marker = buildResolveNewOutputsSql('marker', [hash('bb'), hash('cc')])!;
  t.is(marker.match(/UPDATE output o/gu)?.length, 1);
  t.true(
    marker.includes(
      `(VALUES ('\\x${hash('bb')}'::bytea),('\\x${hash(
        'cc'
      )}'::bytea)) AS n (hash)`
    ),
    marker
  );
  t.true(
    marker.includes('WHERE input.outpoint_transaction_hash = n.hash OFFSET 0')
  );
  t.true(
    marker.includes(
      'unspent_tracking_transaction_is_accepted(i.transaction_internal_id)'
    )
  );
  t.true(
    marker.includes(
      'unspent_tracking_transaction_is_block_accepted(i.transaction_internal_id) DESC'
    )
  );
  t.true(marker.includes('AND o.spent_by_transaction_internal_id = 0;'));
  t.regex(
    resolveStagedNewOutputsSql('settable')!,
    /DELETE FROM unspent_output_set u[\s\S]*pg_temp\.chaingraph_stage_transaction/u
  );
  t.regex(
    resolveMempoolOutputsSql('marker')!,
    /\(SELECT \$1::bytea AS hash\) AS n/u
  );
});

test('reacceptSpendsSql: a block acceptance may replace a mempool-only marker', (t) => {
  t.regex(
    reacceptSpendsSql('marker', true)!,
    /ELSE NOT unspent_tracking_transaction_is_block_accepted/u
  );
  t.notRegex(reacceptSpendsSql('marker')!, /is_block_accepted/u);
});
