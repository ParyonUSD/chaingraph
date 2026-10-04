import { readFileSync } from 'fs';

import type { TestFn } from 'ava';
import baseTest from 'ava';
import pg from 'pg';

import { indexDefinitions } from '../components/db-utils.js';

const test = baseTest as TestFn<{ pool: pg.Pool }>;
const host = process.env.CHAINGRAPH_E2E_POSTGRES_HOST ?? 'localhost';
const port = process.env.CHAINGRAPH_E2E_POSTGRES_PORT ?? '5432';
const migrations = new URL(
  '../../images/hasura/hasura-data/migrations/default/',
  import.meta.url
);
const migration = (name: string, direction: 'down' | 'up' = 'up') =>
  readFileSync(new URL(`${name}/${direction}.sql`, migrations), 'utf8');
const hashBytes = 32;
const otherNode = 2;

test.before(async (t) => {
  t.context.pool = new pg.Pool({
    connectionString: `postgres://chaingraph:very_insecure_postgres_password@${host}:${port}/postgres`,
    max: 1,
  });
});

test.after.always(async (t) => {
  await t.context.pool.end();
});

test.serial(
  '[e2e] node-block membership index preserves membership trigger behavior',
  async (t) => {
    const client = await t.context.pool.connect();
    // eslint-disable-next-line functional/no-try-statement
    try {
      await client.query('BEGIN;');
      await client.query("SET LOCAL statement_timeout = '10s';");
      await client.query(migration('1616195337538_init'));
      await client.query(
        migration('1778151011521_cascade_invalidate_mempool_descendants')
      );
      await client.query(migration('1790852400000_add_output_node_membership'));
      await client.query(
        migration('1790930801000_inline_output_membership_roots')
      );
      await client.query(
        migration('1790950000000_bound_block_confirmation_mempool_cleanup')
      );
      await client.query(
        migration('1791100003000_custom_plan_output_membership_refresh')
      );
      await client.query(indexDefinitions.block_inclusions_index);
      await client.query(
        migration('1791100004000_add_node_block_membership_index')
      );
      await client.query(
        migration('1791100004000_add_node_block_membership_index')
      );
      await client.query(/* sql */ `
ALTER TABLE node_block ENABLE TRIGGER trigger_public_node_block_insert;
ALTER TABLE node_transaction_history
  ENABLE TRIGGER trigger_public_node_transaction_history_insert;

INSERT INTO node (internal_id, name, protocol_version, user_agent)
  VALUES
    (1, 'fixture', 1, '/fixture/'),
    (2, 'other-fixture', 1, '/fixture/');
INSERT INTO transaction (internal_id, hash, version, locktime, size_bytes, is_coinbase)
  VALUES
    (1, decode(repeat('01', 32), 'hex'), 2, 0, 100, false),
    (2, decode(repeat('02', 32), 'hex'), 2, 0, 100, false),
    (3, decode(repeat('03', 32), 'hex'), 2, 0, 100, false),
    (4, decode(repeat('04', 32), 'hex'), 2, 0, 100, false),
    (10, decode(repeat('10', 32), 'hex'), 2, 0, 100, false);
INSERT INTO output (transaction_hash, output_index, value_satoshis, locking_bytecode)
  VALUES
    (decode(repeat('01', 32), 'hex'), 0, 1000, '\\x51'),
    (decode(repeat('02', 32), 'hex'), 0, 900, '\\x51'),
    (decode(repeat('02', 32), 'hex'), 1, 0, '\\x6a01ff'),
    (decode(repeat('03', 32), 'hex'), 0, 800, '\\x51'),
    (decode(repeat('04', 32), 'hex'), 0, 700, '\\x51'),
    (decode(repeat('10', 32), 'hex'), 0, 600, '\\x51');
INSERT INTO input (
  transaction_internal_id, input_index, outpoint_index, sequence_number,
  outpoint_transaction_hash, unlocking_bytecode
) VALUES
  (1, 0, 0, 4294967295, decode(repeat('dd', 32), 'hex'), '\\x'),
  (2, 0, 0, 4294967295, decode(repeat('01', 32), 'hex'), '\\x'),
  (3, 0, 0, 4294967295, decode(repeat('aa', 32), 'hex'), '\\x'),
  (4, 0, 0, 4294967295, decode(repeat('03', 32), 'hex'), '\\x'),
  (10, 0, 0, 4294967295, decode(repeat('aa', 32), 'hex'), '\\x');

-- Seed one coherent mempool snapshot; the production insert trigger normally
-- builds this state incrementally and would reject the deliberate conflict.
ALTER TABLE node_transaction DISABLE TRIGGER trigger_public_node_transaction_insert;
INSERT INTO node_transaction (node_internal_id, transaction_internal_id)
  VALUES (1, 1), (1, 2), (1, 3), (1, 4), (2, 1), (2, 3), (2, 4);
ALTER TABLE node_transaction ENABLE TRIGGER trigger_public_node_transaction_insert;

INSERT INTO block (
  internal_id, height, version, "timestamp", hash, previous_block_hash,
  merkle_root, bits, nonce, size_bytes
) VALUES (
  100, 1, 1, 1, decode(repeat('bb', 32), 'hex'),
  decode(repeat('00', 32), 'hex'), decode(repeat('cc', 32), 'hex'),
  1, 1, 100
);
INSERT INTO block_transaction (block_internal_id, transaction_internal_id, transaction_index)
  VALUES (100, 1, 0), (100, 10, 1);
INSERT INTO node_block (node_internal_id, block_internal_id, accepted_at)
  VALUES (1, 100, '2026-02-01');
`);

      const membership = await client.query(/* sql */ `
SELECT encode(transaction_hash, 'hex') AS hash, output_index::integer AS index,
       accepted_node_ids AS accepted, unspent_node_ids AS unspent
  FROM output
  WHERE transaction_hash IN (
    decode(repeat('02', 32), 'hex'),
    decode(repeat('03', 32), 'hex'),
    decode(repeat('04', 32), 'hex')
  )
  ORDER BY hash, index;
`);
      t.deepEqual(membership.rows, [
        { accepted: [1], hash: '02'.repeat(hashBytes), index: 0, unspent: [1] },
        { accepted: [1], hash: '02'.repeat(hashBytes), index: 1, unspent: [] },
        {
          accepted: [otherNode],
          hash: '03'.repeat(hashBytes),
          index: 0,
          unspent: [],
        },
        {
          accepted: [otherNode],
          hash: '04'.repeat(hashBytes),
          index: 0,
          unspent: [otherNode],
        },
      ]);
      t.deepEqual(
        (
          await client.query(/* sql */ `
SELECT transaction_internal_id::integer AS tx, replaced_at::text AS replaced
  FROM node_transaction_history ORDER BY tx;
`)
        ).rows,
        [
          { replaced: null, tx: 1 },
          { replaced: '2026-02-01 00:00:00', tx: 3 },
          { replaced: '2026-02-01 00:00:00', tx: 4 },
        ]
      );

      await client.query(/* sql */ `
DELETE FROM node_block
  WHERE node_internal_id = 1 AND block_internal_id = 100;
`);
      const afterReorg = await client.query(/* sql */ `
SELECT encode(transaction_hash, 'hex') AS hash,
       output_index::integer AS index,
       accepted_node_ids AS accepted, unspent_node_ids AS unspent
  FROM output
  WHERE transaction_hash IN (
    decode(repeat('01', 32), 'hex'),
    decode(repeat('02', 32), 'hex')
  )
  ORDER BY hash, index;
`);
      t.deepEqual(afterReorg.rows, [
        {
          accepted: [otherNode],
          hash: '01'.repeat(hashBytes),
          index: 0,
          unspent: [otherNode],
        },
        {
          accepted: [1],
          hash: '02'.repeat(hashBytes),
          index: 0,
          unspent: [1],
        },
        { accepted: [1], hash: '02'.repeat(hashBytes), index: 1, unspent: [] },
      ]);

      const indexes = await client.query<{
        definition: string;
        name: string;
      }>(/* sql */ `
SELECT indexname AS name, indexdef AS definition
  FROM pg_indexes
  WHERE schemaname = 'public'
    AND indexname IN ('block_inclusions_index', 'node_block_block_node_index')
  ORDER BY indexname;
`);
      t.deepEqual(indexes.rows, [
        {
          definition:
            'CREATE INDEX block_inclusions_index ON public.block_transaction USING btree (transaction_internal_id)',
          name: 'block_inclusions_index',
        },
        {
          definition:
            'CREATE INDEX node_block_block_node_index ON public.node_block USING btree (block_internal_id, node_internal_id)',
          name: 'node_block_block_node_index',
        },
      ]);

      await client.query(
        migration('1791100004000_add_node_block_membership_index', 'down')
      );
      const removed = await client.query<{ index: string | null }>(/* sql */ `
SELECT to_regclass('public.node_block_block_node_index') AS index;
`);
      t.is(removed.rows[0]!.index, null);

      await client.query(/* sql */ `
CREATE INDEX node_block_block_node_index
  ON node_block (node_internal_id, block_internal_id);
`);
      const mismatch = await t.throwsAsync(
        client.query(migration('1791100004000_add_node_block_membership_index'))
      );
      t.regex(
        mismatch?.message ?? '',
        /invalid or has an unexpected definition/u
      );
    } finally {
      await client.query('ROLLBACK;');
      client.release();
    }
  }
);
