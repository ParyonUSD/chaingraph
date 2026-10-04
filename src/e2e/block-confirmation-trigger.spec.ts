import { readFileSync } from 'fs';

import type { TestFn } from 'ava';
import baseTest from 'ava';

import type * as database from '../db.js';

// cspell:words functiondef prosrc regprocedure
const test = baseTest as TestFn<{ db: typeof database }>;
const host = process.env.CHAINGRAPH_E2E_POSTGRES_HOST ?? 'localhost';
const port = process.env.CHAINGRAPH_E2E_POSTGRES_PORT ?? '5432';
const migrations = new URL(
  '../../images/hasura/hasura-data/migrations/default/',
  import.meta.url
);
const readMigration = (name: string, direction: 'down' | 'up') =>
  readFileSync(new URL(`${name}/${direction}.sql`, migrations), 'utf8');

test.before(async (t) => {
  process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING = `postgres://chaingraph:very_insecure_postgres_password@${host}:${port}/postgres`;
  process.env.CHAINGRAPH_POSTGRES_MAX_CONNECTIONS = '1';
  t.context.db = await import('../db.js');
});

test.beforeEach(async (t) => {
  await t.context.db.pool.query(/* sql */ `
BEGIN;
SET LOCAL search_path = pg_temp;
SET LOCAL statement_timeout = '5s';
CREATE TEMP TABLE transaction (internal_id bigint PRIMARY KEY, hash bytea UNIQUE);
CREATE TEMP TABLE output (transaction_hash bytea, output_index integer,
  PRIMARY KEY (transaction_hash, output_index));
CREATE TEMP TABLE input (transaction_internal_id bigint, input_index bigint,
  outpoint_transaction_hash bytea, outpoint_index bigint,
  PRIMARY KEY (transaction_internal_id, input_index));
CREATE INDEX spent_by_index ON input (outpoint_transaction_hash, outpoint_index);
CREATE TEMP TABLE node_transaction (node_internal_id smallint,
  transaction_internal_id bigint, validated_at timestamp DEFAULT '2026-01-01',
  PRIMARY KEY (node_internal_id, transaction_internal_id));
CREATE TEMP TABLE node_transaction_history (node_internal_id smallint,
  transaction_internal_id bigint, validated_at timestamp, replaced_at timestamp,
  PRIMARY KEY (node_internal_id, transaction_internal_id, validated_at));
CREATE TEMP TABLE block_transaction (block_internal_id bigint,
  transaction_internal_id bigint,
  PRIMARY KEY (transaction_internal_id, block_internal_id));
CREATE TEMP TABLE node_block (node_internal_id smallint,
  block_internal_id bigint, accepted_at timestamp NOT NULL,
  PRIMARY KEY (node_internal_id, block_internal_id));
`);

  const cascade = readMigration(
    '1778151011521_cascade_invalidate_mempool_descendants',
    'up'
  )
    .split('CREATE TRIGGER')[0]!
    .replace(
      'FUNCTION trigger_node_transaction_history_insert()',
      'FUNCTION pg_temp.trigger_node_transaction_history_insert()'
    );
  const blockConfirmation = readMigration(
    '1790950000000_bound_block_confirmation_mempool_cleanup',
    'up'
  ).replace(
    'FUNCTION trigger_node_block_insert()',
    'FUNCTION pg_temp.trigger_node_block_insert()'
  );
  await t.context.db.pool.query(`${cascade}
${blockConfirmation}
CREATE TRIGGER cascade_history AFTER INSERT ON node_transaction_history
  REFERENCING NEW TABLE AS new_table FOR EACH STATEMENT
  EXECUTE FUNCTION pg_temp.trigger_node_transaction_history_insert();
CREATE TRIGGER confirm_blocks AFTER INSERT ON node_block
  REFERENCING NEW TABLE AS new_table FOR EACH STATEMENT
  EXECUTE FUNCTION pg_temp.trigger_node_block_insert();
`);
});

test.afterEach.always(async (t) => {
  await t.context.db.pool.query('ROLLBACK;');
});

test.after.always(async (t) => {
  await t.context.db.pool.end();
});

const rows = /* sql */ `
SELECT node_internal_id AS node, transaction_internal_id::integer AS tx,
       replaced_at::text AS replaced
  FROM node_transaction_history ORDER BY node, tx;
`;

test.serial(
  '[e2e] block confirmation archives confirmations, conflicts, and descendants per node',
  async (t) => {
    await t.context.db.pool.query(/* sql */ `
INSERT INTO transaction VALUES
  (1, decode(repeat('01', 32), 'hex')),
  (2, decode(repeat('02', 32), 'hex')),
  (3, decode(repeat('03', 32), 'hex')),
  (10, decode(repeat('10', 32), 'hex')),
  (11, decode(repeat('11', 32), 'hex'));
INSERT INTO output VALUES
  (decode(repeat('01', 32), 'hex'), 0),
  (decode(repeat('03', 32), 'hex'), 0);
INSERT INTO input VALUES
  (1, 0, decode(repeat('aa', 32), 'hex'), 0),
  (2, 0, decode(repeat('01', 32), 'hex'), 0),
  (3, 0, decode(repeat('bb', 32), 'hex'), 1),
  (10, 0, decode(repeat('aa', 32), 'hex'), 0),
  (10, 1, decode(repeat('00', 32), 'hex'), 4294967295),
  (11, 0, decode(repeat('bb', 32), 'hex'), 1);
INSERT INTO node_transaction (node_internal_id, transaction_internal_id) VALUES
  (1, 1), (1, 2), (1, 10),
  (2, 1), (2, 3), (2, 11);
INSERT INTO block_transaction VALUES (100, 10), (101, 11);
`);

    const legacyDirectRows = await t.context.db.pool.query(/* sql */ `
WITH new_rows(node_internal_id, block_internal_id, accepted_at) AS (
  VALUES
    (1::smallint, 100::bigint, '2026-02-01'::timestamp),
    (2::smallint, 101::bigint, '2026-02-02'::timestamp)
),
accepted_transactions AS (
  SELECT new_rows.node_internal_id, block_transaction.transaction_internal_id,
         new_rows.accepted_at
    FROM block_transaction JOIN new_rows USING (block_internal_id)
),
newly_spent AS (
  SELECT accepted_transactions.*, input.outpoint_transaction_hash,
         input.outpoint_index
    FROM input JOIN accepted_transactions USING (transaction_internal_id)
),
legacy_candidates AS (
  SELECT newly_spent.node_internal_id,
         input.transaction_internal_id,
         CASE WHEN input.transaction_internal_id != newly_spent.transaction_internal_id
           THEN newly_spent.accepted_at ELSE NULL
         END AS replaced_at
    FROM input JOIN newly_spent USING (outpoint_transaction_hash, outpoint_index)
    WHERE input.outpoint_transaction_hash != decode(repeat('00', 32), 'hex')
)
SELECT node_internal_id AS node, transaction_internal_id::integer AS tx,
       CASE WHEN bool_or(replaced_at IS NULL) THEN NULL ELSE min(replaced_at)::text END AS replaced
  FROM legacy_candidates
  GROUP BY node_internal_id, transaction_internal_id
  ORDER BY node, tx;
`);

    // One statement covers multiple blocks and nodes, matching transition-table use.
    await t.context.db.pool.query(/* sql */ `
INSERT INTO node_block VALUES (1, 100, '2026-02-01'), (2, 101, '2026-02-02');
`);

    const directHistory = await t.context.db.pool.query(/* sql */ `
SELECT node_internal_id AS node, transaction_internal_id::integer AS tx,
       replaced_at::text AS replaced
  FROM node_transaction_history
  WHERE transaction_internal_id IN (1, 3, 10, 11)
  ORDER BY node, tx;
`);
    t.deepEqual(directHistory.rows, legacyDirectRows.rows);

    t.deepEqual((await t.context.db.pool.query(rows)).rows, [
      { node: 1, replaced: '2026-02-01 00:00:00', tx: 1 },
      { node: 1, replaced: '2026-02-01 00:00:00', tx: 2 },
      { node: 1, replaced: null, tx: 10 },
      { node: 2, replaced: '2026-02-02 00:00:00', tx: 3 },
      { node: 2, replaced: null, tx: 11 },
    ]);
    t.deepEqual(
      (
        await t.context.db.pool.query(/* sql */ `
SELECT node_internal_id AS node, transaction_internal_id::integer AS tx
  FROM node_transaction ORDER BY node, tx;
`)
      ).rows,
      [{ node: 2, tx: 1 }]
    );
  }
);

test.serial(
  '[e2e] migration down restores the previous trigger function',
  async (t) => {
    const downMigration = readMigration(
      '1790950000000_bound_block_confirmation_mempool_cleanup',
      'down'
    );
    const down = downMigration.replace(
      'FUNCTION trigger_node_block_insert()',
      'FUNCTION pg_temp.trigger_node_block_insert()'
    );
    const expected = downMigration.replace(
      'FUNCTION trigger_node_block_insert()',
      'FUNCTION pg_temp.expected_legacy_node_block_insert()'
    );
    await t.context.db.pool.query(`${expected}\n${down}`);
    const definitions = await t.context.db.pool.query<{
      actual: string;
      expected: string;
    }>(/* sql */ `
SELECT actual.prosrc AS actual, expected.prosrc AS expected
  FROM pg_proc actual, pg_proc expected
  WHERE actual.oid = 'pg_temp.trigger_node_block_insert()'::regprocedure
    AND expected.oid = 'pg_temp.expected_legacy_node_block_insert()'::regprocedure;
`);
    t.is(definitions.rows[0]!.actual, definitions.rows[0]!.expected);
  }
);
