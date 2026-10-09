/* eslint-disable @typescript-eslint/no-magic-numbers, camelcase, @typescript-eslint/naming-convention, sort-keys, no-bitwise, max-params, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/require-array-sort-compare */
import { readdirSync, readFileSync } from 'fs';
import { join } from 'path';
import { fileURLToPath } from 'url';

import test from 'ava';
import pg from 'pg';

import type { DeferredBatchResult } from './unspent-deferred.js';
import {
  batchDidWork,
  blockReacceptedEventsSql,
  configureDeferredTriggersSql,
  deferredIndexDefinitions,
  deferredIndexNames,
  deferredQueryRootSql,
  deferredTriggerNames,
  formatBatchLog,
  headersAcceptedEventsSql,
  nextSkipThrough,
  nextStallState,
  parseBatchResult,
  readSequencesSql,
  transactionReacceptedEventsSql,
} from './unspent-deferred.js';

test('deferredIndexDefinitions: NULL partial indexes per kind, bitmask any-bit indexes', (t) => {
  t.deepEqual(Object.keys(deferredIndexDefinitions('marker')), [
    'output_unspent_null_search_index',
    'output_unspent_null_token_category_index',
  ]);
  t.deepEqual(Object.keys(deferredIndexDefinitions('bitmask')), [
    'output_unspent_bits_null_search_index',
    'output_unspent_bits_null_token_category_index',
    'output_unspent_bits_search_index',
    'output_unspent_bits_token_category_index',
  ]);
  t.regex(
    deferredIndexDefinitions('marker')
      .output_unspent_null_token_category_index ?? '',
    /WHERE spent_by_transaction_internal_id IS NULL AND token_category IS NOT NULL;$/u
  );
  t.regex(
    deferredIndexDefinitions('bitmask').output_unspent_bits_search_index ?? '',
    /substring\(locking_bytecode, 0, 26\)\) WHERE unspent_node_bits <> 0;$/u
  );
  t.is(deferredIndexNames.length, 6);
});

test('configureDeferredTriggersSql: only triggers whose state differs', (t) => {
  t.deepEqual(
    configureDeferredTriggersSql(true, {
      trigger_unspent_deferred_node_block_delete: false,
      trigger_unspent_deferred_node_transaction_delete: true,
    }),
    [
      'ALTER TABLE node_block ENABLE TRIGGER trigger_unspent_deferred_node_block_delete;',
    ]
  );
  t.deepEqual(
    configureDeferredTriggersSql(false, {
      trigger_unspent_deferred_node_block_delete: true,
      trigger_unspent_deferred_node_transaction_delete: true,
    }),
    [
      'ALTER TABLE node_block DISABLE TRIGGER trigger_unspent_deferred_node_block_delete;',
      'ALTER TABLE node_transaction DISABLE TRIGGER trigger_unspent_deferred_node_transaction_delete;',
    ]
  );
  t.deepEqual(configureDeferredTriggersSql(true, {}), []);
  t.is(deferredTriggerNames.length, 2);
});

test('deferredQueryRootSql: inlinable wrapper for the active kind', (t) => {
  t.regex(
    deferredQueryRootSql('bitmask'),
    /LANGUAGE sql STABLE AS \$\$\n {2}SELECT \* FROM unspent_output_deferred_bitmask\(node_name\)\n\$\$;/u
  );
});

test('re-acceptance event statements only record rows inserted by this transaction', (t) => {
  t.regex(
    blockReacceptedEventsSql,
    /node_block\.xmin = pg_current_xact_id\(\)::xid/u
  );
  t.regex(blockReacceptedEventsSql, /b\.xmin <> pg_current_xact_id\(\)::xid/u);
  t.regex(
    transactionReacceptedEventsSql,
    /nt\.xmin = pg_current_xact_id\(\)::xid/u
  );
  t.regex(
    headersAcceptedEventsSql(7, 'INSERT INTO node_block SELECT 1 RETURNING 1'),
    /SELECT 'accepted', 7::bigint, "blockInternalId" FROM inserted_node_blocks/u
  );
});

test('nextStallState / nextSkipThrough: skip everything settled once a stall lasts', (t) => {
  const first = nextStallState(undefined, 50, 1_000);
  t.deepEqual(first, { since: 1_000, transactionInternalId: 50 });
  t.is(nextStallState(first, 50, 5_000), first);
  t.deepEqual(nextStallState(first, 60, 5_000), {
    since: 5_000,
    transactionInternalId: 60,
  });
  t.is(nextStallState(first, null, 5_000), undefined);
  t.is(nextSkipThrough(0, first, 1_500, 1_000, 99), 0);
  t.is(nextSkipThrough(0, first, 2_000, 1_000, 99), 99);
  t.is(nextSkipThrough(120, first, 2_000, 1_000, 99), 120);
  t.is(nextSkipThrough(120, undefined, 9_000, 1_000, 200), 120);
});

test('parseBatchResult / batchDidWork / formatBatchLog', (t) => {
  const result = parseBatchResult({
    affected: 10,
    blockTransactions: 0,
    blockWatermark: 5,
    blocks: 0,
    changed: 7,
    events: 0,
    eventTransactions: 0,
    inputs: 4,
    inputWatermark: 20,
    previousBlockWatermark: 5,
    previousInputWatermark: 10,
    skippedInputs: 0,
    stalledAt: 21,
    sweep: 0,
    watchAdded: 0,
    watchChanged: 0,
    watchExpired: 0,
  });
  t.is(result.stalledAt, 21);
  t.true(batchDidWork(result));
  t.false(batchDidWork({ ...result, inputWatermark: 10, stalledAt: null }));
  t.regex(formatBatchLog('marker', result, 12), /stalled before tx 21/u);
  t.true(parseBatchResult({ busy: true }).busy === true);
});

/*
 * The job's batch SQL against a real database: set
 * CHAINGRAPH_UNIT_POSTGRES_URL (e.g. postgres://chaingraph:…@localhost:5432)
 * to run these; each kind gets a fresh database with every migration.
 */
const unitPostgresUrl = process.env.CHAINGRAPH_UNIT_POSTGRES_URL;
const migrationsDirectory = fileURLToPath(
  new URL('../../images/hasura/hasura-data/migrations/default', import.meta.url)
);
const hex = (byte: string) => Buffer.from(byte.repeat(32), 'hex');

const freshDatabase = async (name: string) => {
  const admin = new pg.Client({
    connectionString: `${unitPostgresUrl!}/postgres`,
  });
  await admin.connect();
  await admin.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE);`);
  await admin.query(`CREATE DATABASE ${name};`);
  await admin.end();
  const client = new pg.Client({
    connectionString: `${unitPostgresUrl!}/${name}`,
  });
  await client.connect();
  await readdirSync(migrationsDirectory)
    .sort()
    .reduce<Promise<unknown>>(
      async (chain, directory) =>
        chain.then(async () =>
          client.query(
            readFileSync(join(migrationsDirectory, directory, 'up.sql'), 'utf8')
          )
        ),
      Promise.resolve()
    );
  await client.query(/* sql */ `
    CREATE INDEX spent_by_index ON input (outpoint_transaction_hash, outpoint_index);
    CREATE INDEX block_inclusions_index ON block_transaction (transaction_internal_id);
    CREATE INDEX token_category_index ON output (token_category);
    CREATE INDEX output_search_index ON output (substring(locking_bytecode, 0, 26));
    ALTER TABLE node_block ENABLE TRIGGER trigger_unspent_deferred_node_block_delete;
    ALTER TABLE node_transaction ENABLE TRIGGER trigger_unspent_deferred_node_transaction_delete;
    INSERT INTO node (name, protocol_version, user_agent) VALUES ('a', 70016, 'unit'), ('b', 70016, 'unit');`);
  return client;
};

/**
 * A saved transaction (`byte` repeated as its hash) with `outputs` outputs and
 * the given spends; returns its internal ID.
 */
const saveTransaction = async (
  client: pg.Client,
  byte: string,
  spends: [string, number][],
  outputs = 2
) => {
  const id = Number(
    (
      await client.query<{ id: string }>(
        `INSERT INTO transaction (hash, version, locktime, size_bytes, is_coinbase) VALUES ($1, 2, 0, 60, false) RETURNING internal_id AS id;`,
        [hex(byte)]
      )
    ).rows[0]!.id
  );
  await Array.from({ length: outputs }, (_, index) => index).reduce<
    Promise<unknown>
  >(
    async (chain, index) =>
      chain.then(async () =>
        client.query(
          `INSERT INTO output (transaction_hash, output_index, value_satoshis, locking_bytecode) VALUES ($1, $2, 1000, '\\x51');`,
          [hex(byte), index]
        )
      ),
    Promise.resolve()
  );
  await spends.reduce<Promise<unknown>>(
    async (chain, [outpointByte, outpointIndex], inputIndex) =>
      chain.then(async () =>
        client.query(
          `INSERT INTO input (transaction_internal_id, input_index, outpoint_index, sequence_number, outpoint_transaction_hash, unlocking_bytecode) VALUES ($1, $2, $3, 0, $4, '\\x51');`,
          [id, inputIndex, outpointIndex, hex(outpointByte)]
        )
      ),
    Promise.resolve()
  );
  return id;
};
const saveBlock = async (
  client: pg.Client,
  byte: string,
  transactionIds: number[],
  nodes: number[]
) => {
  const blockId = Number(
    (
      await client.query<{ id: string }>(
        `INSERT INTO block (height, version, timestamp, hash, previous_block_hash, merkle_root, bits, nonce, size_bytes)
           VALUES (1, 1, 0, $1, $1, $1, 0, 0, 0) RETURNING internal_id AS id;`,
        [hex(byte)]
      )
    ).rows[0]!.id
  );
  await transactionIds.reduce<Promise<unknown>>(
    async (chain, transactionId, index) =>
      chain.then(async () =>
        client.query(
          `INSERT INTO block_transaction (block_internal_id, transaction_internal_id, transaction_index) VALUES ($1, $2, $3);`,
          [blockId, transactionId, index]
        )
      ),
    Promise.resolve()
  );
  await nodes.reduce<Promise<unknown>>(
    async (chain, node) =>
      chain.then(async () =>
        client.query(
          `INSERT INTO node_block (node_internal_id, block_internal_id) VALUES ($1, $2);`,
          [node, blockId]
        )
      ),
    Promise.resolve()
  );
  return blockId;
};
const limits = async (client: pg.Client) => {
  const row = (
    await client.query<{ blockLimit: string; transactionLimit: string }>(
      readSequencesSql
    )
  ).rows[0]!;
  return {
    block: Number(row.blockLimit),
    transaction: Number(row.transactionLimit),
  };
};
const batch = async (
  client: pg.Client,
  kind: string,
  options: {
    maxInputs?: number;
    skipStallThrough?: number;
    sweepRows?: number;
  } = {}
): Promise<DeferredBatchResult> => {
  const { block, transaction } = await limits(client);
  await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ;');
  const raw = (
    await client.query<{ result: { [key: string]: unknown } }>(
      `SELECT unspent_deferred_run_batch($1, $2, $3, $4, 200, 20000, $5, $6) AS result;`,
      [
        kind,
        transaction,
        block,
        options.maxInputs ?? 50_000,
        options.sweepRows ?? 0,
        options.skipStallThrough ?? 0,
      ]
    )
  ).rows[0]!.result;
  await client.query('COMMIT;');
  return parseBatchResult(raw);
};
const marker = async (client: pg.Client, byte: string, index: number) =>
  (
    await client.query<{ marker: string | null }>(
      `SELECT spent_by_transaction_internal_id AS marker FROM output WHERE transaction_hash = $1 AND output_index = $2;`,
      [hex(byte), index]
    )
  ).rows[0]!.marker;
const bits = async (client: pg.Client, byte: string, index: number) =>
  (
    await client.query<{ bits: string | null }>(
      `SELECT unspent_node_bits AS bits FROM output WHERE transaction_hash = $1 AND output_index = $2;`,
      [hex(byte), index]
    )
  ).rows[0]!.bits;

const sqlTest = unitPostgresUrl === undefined ? test.skip : test.serial;

sqlTest(
  '[sql] unspent_deferred_run_batch (marker): range, stall, release, watch, block range, idempotency',
  async (t) => {
    const client = await freshDatabase('chaingraph_unit_deferred_marker');
    // eslint-disable-next-line functional/no-try-statement
    try {
      const { block, transaction } = await limits(client);
      await client.query(
        `SELECT unspent_deferred_initialize('marker', $1, $2);`,
        [transaction, block]
      );
      t.true(
        (await batch(client, 'bitmask')).uninitialized === true,
        'kind not started'
      );
      const funding = await saveTransaction(client, 'f0', [], 4);
      await saveBlock(client, 'b0', [funding], [1]);
      const spender = await saveTransaction(client, 'a1', [['f0', 0]]);
      await saveBlock(client, 'b1', [spender], [1]);
      // a mempool spender of f0:1 accepted by node 2 only
      const mempoolSpender = await saveTransaction(client, 'a2', [['f0', 1]]);
      await client.query(
        `INSERT INTO node_transaction (node_internal_id, transaction_internal_id) VALUES (2, $1);`,
        [mempoolSpender]
      );
      // child before parent: c1 spends p1:0, p1 not saved yet
      const child = await saveTransaction(client, 'c1', [['e1', 0]]);
      await client.query(
        `INSERT INTO node_transaction (node_internal_id, transaction_internal_id) VALUES (1, $1);`,
        [child]
      );
      const first = await batch(client, 'marker');
      t.is(
        first.stalledAt,
        child,
        'stops before the child whose parent output is missing'
      );
      t.is(first.inputWatermark, child - 1);
      t.is(await marker(client, 'f0', 0), String(spender));
      t.is(await marker(client, 'f0', 1), String(mempoolSpender));
      t.is(await marker(client, 'f0', 2), '0');
      t.is(await marker(client, 'a1', 0), '0');
      t.is(await marker(client, 'c1', 0), null, 'child not processed yet');
      // still stalled; then the parent arrives
      t.is((await batch(client, 'marker')).stalledAt, child);
      const parent = await saveTransaction(client, 'e1', []);
      await client.query(
        `INSERT INTO node_transaction (node_internal_id, transaction_internal_id) VALUES (1, $1);`,
        [parent]
      );
      const caughtUp = await batch(client, 'marker');
      t.is(caughtUp.stalledAt, null);
      t.is(
        await marker(client, 'e1', 0),
        String(child),
        'parent output marked spent by the child'
      );
      t.is(await marker(client, 'c1', 0), '0');
      // idempotent: nothing changes on a re-run
      t.is((await batch(client, 'marker')).changed, 0);
      // release: node 2 drops the mempool spender -> event -> f0:1 unspent again, a2 watched
      await client.query(
        `DELETE FROM node_transaction WHERE transaction_internal_id = $1;`,
        [mempoolSpender]
      );
      const released = await batch(client, 'marker');
      t.is(released.events, 1);
      t.is(released.watchAdded, 1);
      t.is(await marker(client, 'f0', 1), '0');
      t.is(
        Number(
          (
            await client.query(
              `SELECT count(*) AS n FROM unspent_tracking_events;`
            )
          ).rows[0].n
        ),
        0
      );
      // re-acceptance through a path that writes no event: the watch set catches it
      await client.query(
        `INSERT INTO node_transaction (node_internal_id, transaction_internal_id) VALUES (2, $1);`,
        [mempoolSpender]
      );
      const watched = await batch(client, 'marker');
      t.is(watched.watchChanged, 1);
      t.is(await marker(client, 'f0', 1), String(mempoolSpender));
      // block range: a new block confirming an old (already processed) transaction that was in no mempool
      const late = await saveTransaction(client, 'a3', [['f0', 3]]);
      t.is((await batch(client, 'marker')).changed > 0, true);
      t.is(
        await marker(client, 'f0', 3),
        '0',
        'unaccepted spender: still unspent'
      );
      await saveBlock(client, 'b3', [late], [1]);
      const blockRange = await batch(client, 'marker');
      t.is(blockRange.blocks, 1);
      t.is(blockRange.blockTransactions, 1);
      t.is(await marker(client, 'f0', 3), String(late));
      // re-org: node 1 drops b1 -> a1 released -> f0:0 unspent
      await client.query(
        `DELETE FROM node_block WHERE block_internal_id = (SELECT internal_id FROM block WHERE hash = $1);`,
        [hex('b1')]
      );
      await batch(client, 'marker');
      t.is(await marker(client, 'f0', 0), '0');
      // the query root equals the F1g-style reference for node 1 (any-node spends)
      const root = (
        await client.query<{ k: string }>(
          `SELECT encode(transaction_hash, 'hex') || ':' || output_index AS k FROM unspent_output_deferred_marker('a') ORDER BY 1;`
        )
      ).rows.map((row) => row.k);
      t.true(root.includes(`${'f0'.repeat(32)}:0`));
      t.false(root.includes(`${'f0'.repeat(32)}:3`));
    } finally {
      await client.end();
    }
  }
);

sqlTest(
  '[sql] unspent_deferred_run_batch (bitmask): per-node bits, skip, sweep and backfill',
  async (t) => {
    const client = await freshDatabase('chaingraph_unit_deferred_bitmask');
    // eslint-disable-next-line functional/no-try-statement
    try {
      const before = await saveTransaction(client, '0d', [], 2);
      await client.query(
        `UPDATE output SET token_category = $1 WHERE transaction_hash = $2;`,
        [hex('cc'), hex('0d')]
      );
      await saveBlock(client, 'b9', [before], [1, 2]);
      const { block, transaction } = await limits(client);
      await client.query(
        `SELECT unspent_deferred_initialize('bitmask', $1, $2);`,
        [transaction, block]
      );
      const funding = await saveTransaction(client, 'f0', [], 3);
      await saveBlock(client, 'b0', [funding], [1, 2]);
      // node 1 only accepts a spender of f0:0
      const onlyA = await saveTransaction(client, 'a1', [['f0', 0]]);
      await client.query(
        `INSERT INTO node_transaction (node_internal_id, transaction_internal_id) VALUES (1, $1);`,
        [onlyA]
      );
      // an input whose output never arrives
      const orphan = await saveTransaction(client, '01', [['dd', 7]]);
      const stalled = await batch(client, 'bitmask');
      t.is(stalled.stalledAt, orphan);
      const skipped = await batch(client, 'bitmask', {
        skipStallThrough: orphan,
      });
      t.is(skipped.stalledAt, null);
      t.is(skipped.skippedInputs, 1);
      t.is(
        await bits(client, 'f0', 0),
        String(1 << 2),
        'unspent for node 2 only'
      );
      t.is(await bits(client, 'f0', 1), String((1 << 1) | (1 << 2)));
      t.is(
        await bits(client, 'a1', 0),
        String(1 << 1),
        'created by a node-1-only transaction'
      );
      t.is(await bits(client, '01', 0), '0', 'creator accepted by no node');
      // release for node 1 only -> its bit back on f0:0, cleared on a1's outputs
      await client.query(
        `DELETE FROM node_transaction WHERE transaction_internal_id = $1 AND node_internal_id = 1;`,
        [onlyA]
      );
      await batch(client, 'bitmask');
      t.is(await bits(client, 'f0', 0), String((1 << 1) | (1 << 2)));
      t.is(await bits(client, 'a1', 0), '0');
      // sweep: the pre-tracking token outputs (NULL) are processed
      t.is(await bits(client, '0d', 0), null);
      const swept = await batch(client, 'bitmask', { sweepRows: 10 });
      t.is(swept.sweep, 2);
      t.is(await bits(client, '0d', 0), String((1 << 1) | (1 << 2)));
      // backfill function (golden reads): category scope
      await client.query(
        `UPDATE output SET unspent_node_bits = NULL WHERE transaction_hash = $1;`,
        [hex('0d')]
      );
      t.is(
        Number(
          (
            await client.query(
              `SELECT unspent_deferred_backfill('bitmask', $1, NULL) AS n;`,
              [hex('cc')]
            )
          ).rows[0].n
        ),
        2
      );
      t.is(await bits(client, '0d', 1), String((1 << 1) | (1 << 2)));
    } finally {
      await client.end();
    }
  }
);
