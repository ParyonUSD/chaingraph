import { readFileSync } from 'fs';

import type { TestFn } from 'ava';
import baseTest from 'ava';

import type * as database from '../db.js';

// cspell:words lpad
const test = baseTest as TestFn<{ db: typeof database }>;
const host = process.env.CHAINGRAPH_E2E_POSTGRES_HOST ?? 'localhost';
const port = process.env.CHAINGRAPH_E2E_POSTGRES_PORT ?? '5432';

/*
 * A single connection keeps every operation inside the same temporary schema
 * and rollback transaction. No persistent tables are created or modified.
 */
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
CREATE TEMP TABLE node (internal_id smallint PRIMARY KEY, name text);
CREATE TEMP TABLE transaction (internal_id bigint PRIMARY KEY, hash bytea UNIQUE);
CREATE TEMP TABLE output (transaction_hash bytea, output_index integer,
  PRIMARY KEY (transaction_hash, output_index));
CREATE TEMP TABLE input (transaction_internal_id bigint, input_index integer,
  outpoint_transaction_hash bytea, outpoint_index integer,
  PRIMARY KEY (transaction_internal_id, input_index));
CREATE INDEX input_outpoint ON input (outpoint_transaction_hash, outpoint_index);
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
INSERT INTO node VALUES (1, 'alpha'), (2, 'beta');
INSERT INTO transaction
  SELECT id, decode(repeat(lpad(to_hex(id), 2, '0'), 32), 'hex')
    FROM generate_series(1, 20) id;
INSERT INTO output SELECT hash, 0 FROM transaction;
`);
  const migration = readFileSync(
    new URL(
      '../../images/hasura/hasura-data/migrations/default/1778151011521_cascade_invalidate_mempool_descendants/up.sql',
      import.meta.url
    ),
    'utf8'
  );
  const cascadeFunction = migration
    .split('CREATE TRIGGER')[0]!
    .replace(
      'FUNCTION trigger_node_transaction_history_insert()',
      'FUNCTION pg_temp.trigger_node_transaction_history_insert()'
    );
  await t.context.db.pool.query(`${cascadeFunction}
CREATE TRIGGER cascade_history AFTER INSERT ON node_transaction_history
  REFERENCING NEW TABLE AS new_table FOR EACH STATEMENT
  EXECUTE FUNCTION pg_temp.trigger_node_transaction_history_insert();
`);
});

test.afterEach.always(async (t) => {
  await t.context.db.pool.query('ROLLBACK;');
});

test.after.always(async (t) => {
  await t.context.db.pool.end();
});

const repeatedHashBytes = 32;
const hash = (byte: string) => byte.repeat(repeatedHashBytes);

const membershipQuery = /* sql */ `
SELECT node_internal_id AS node, transaction_internal_id::integer AS tx
  FROM node_transaction ORDER BY node, tx;
`;
const historyQuery = /* sql */ `
SELECT node_internal_id AS node, transaction_internal_id::integer AS tx,
       replaced_at::text AS replaced
  FROM node_transaction_history ORDER BY node, tx;
`;

test.serial(
  '[e2e] cleanup confirms a parent without invalidating descendants or another node',
  async (t) => {
    const { pool, archiveMempoolTransactionsAcceptedByBlocks: archive } =
      t.context.db;
    await pool.query(/* sql */ `
INSERT INTO input
  SELECT 2, 0, hash, 0 FROM transaction WHERE internal_id = 1
  UNION ALL SELECT 3, 0, hash, 0 FROM transaction WHERE internal_id = 2;
INSERT INTO node_transaction (node_internal_id, transaction_internal_id)
  SELECT node, tx FROM generate_series(1, 2) node, generate_series(1, 3) tx;
-- Duplicate accepted inclusions must still produce only one history row.
INSERT INTO block_transaction VALUES (1, 1), (2, 1);
INSERT INTO node_block VALUES (1, 1, '2026-01-02'), (1, 2, '2026-01-03');
`);
    t.deepEqual(await archive(), [
      { hash: hash('01'), nodeName: 'alpha', replacedAt: null },
    ]);
    t.deepEqual((await pool.query(membershipQuery)).rows, [
      { node: 1, tx: 2 },
      { node: 1, tx: 3 },
      { node: 2, tx: 1 },
      { node: 2, tx: 2 },
      { node: 2, tx: 3 },
    ]);
    t.deepEqual((await pool.query(historyQuery)).rows, [
      { node: 1, replaced: null, tx: 1 },
    ]);
    t.deepEqual(await archive(), []);
  }
);

test.serial(
  '[e2e] cleanup invalidates conflicts and descendants only for the accepting node',
  async (t) => {
    const { pool, archiveMempoolTransactionsAcceptedByBlocks: archive } =
      t.context.db;
    await pool.query(/* sql */ `
INSERT INTO input
  SELECT 1, 0, hash, 0 FROM transaction WHERE internal_id = 10
  UNION ALL SELECT 4, 0, hash, 0 FROM transaction WHERE internal_id = 10
  UNION ALL SELECT 2, 0, hash, 0 FROM transaction WHERE internal_id = 1
  UNION ALL SELECT 3, 0, hash, 0 FROM transaction WHERE internal_id = 2;
INSERT INTO node_transaction (node_internal_id, transaction_internal_id)
  SELECT node, tx FROM generate_series(1, 2) node, generate_series(1, 3) tx;
INSERT INTO block_transaction VALUES (1, 4), (2, 4);
INSERT INTO node_block VALUES (1, 1, '2026-01-05'), (1, 2, '2026-01-02');
`);
    t.deepEqual(await archive(), [
      {
        hash: hash('01'),
        nodeName: 'alpha',
        replacedAt: new Date('2026-01-02T00:00:00.000Z'),
      },
    ]);
    t.deepEqual((await pool.query(membershipQuery)).rows, [
      { node: 2, tx: 1 },
      { node: 2, tx: 2 },
      { node: 2, tx: 3 },
    ]);
    t.deepEqual((await pool.query(historyQuery)).rows, [
      { node: 1, replaced: '2026-01-02 00:00:00', tx: 1 },
      { node: 1, replaced: '2026-01-02 00:00:00', tx: 2 },
      { node: 1, replaced: '2026-01-02 00:00:00', tx: 3 },
    ]);
    t.deepEqual(await archive(), []);
  }
);

test.serial(
  '[e2e] cleanup gives confirmation precedence in mixed confirmation and invalidation batches',
  async (t) => {
    const { pool, archiveMempoolTransactionsAcceptedByBlocks: archive } =
      t.context.db;
    await pool.query(/* sql */ `
INSERT INTO input
  SELECT 1, 0, hash, 0 FROM transaction WHERE internal_id = 10
  UNION ALL SELECT 4, 0, hash, 0 FROM transaction WHERE internal_id = 10
  UNION ALL SELECT 2, 0, hash, 0 FROM transaction WHERE internal_id = 1
  UNION ALL SELECT 5, 0, hash, 0 FROM transaction WHERE internal_id = 11
  UNION ALL SELECT 8, 0, hash, 0 FROM transaction WHERE internal_id = 11
  UNION ALL SELECT 6, 0, hash, 0 FROM transaction WHERE internal_id = 5
  UNION ALL SELECT 7, 0, hash, 0 FROM transaction WHERE internal_id = 6;
INSERT INTO node_transaction (node_internal_id, transaction_internal_id)
  VALUES (1, 1), (1, 2), (1, 5), (1, 6), (1, 7);
-- Deliberately inconsistent historical acceptance tests NULL precedence.
INSERT INTO block_transaction VALUES (1, 1), (2, 4), (3, 8);
INSERT INTO node_block VALUES
  (1, 1, '2026-01-02'), (1, 2, '2026-01-03'), (1, 3, '2026-01-04');
`);
    t.deepEqual(await archive(), [
      { hash: hash('01'), nodeName: 'alpha', replacedAt: null },
      {
        hash: hash('05'),
        nodeName: 'alpha',
        replacedAt: new Date('2026-01-04T00:00:00.000Z'),
      },
    ]);
    t.deepEqual((await pool.query(membershipQuery)).rows, [{ node: 1, tx: 2 }]);
    t.deepEqual((await pool.query(historyQuery)).rows, [
      { node: 1, replaced: null, tx: 1 },
      { node: 1, replaced: '2026-01-04 00:00:00', tx: 5 },
      { node: 1, replaced: '2026-01-04 00:00:00', tx: 6 },
      { node: 1, replaced: '2026-01-04 00:00:00', tx: 7 },
    ]);
  }
);

test.serial(
  '[e2e] cleanup ignores other-node confirmations, self, coinbase and unconfirmed conflicts',
  async (t) => {
    const { pool, archiveMempoolTransactionsAcceptedByBlocks: archive } =
      t.context.db;
    await pool.query(/* sql */ `
INSERT INTO input
  SELECT 1, 0, hash, 0 FROM transaction WHERE internal_id = 10
  UNION ALL SELECT 2, 0, decode(repeat('00', 32), 'hex'), 0
  UNION ALL SELECT 3, 0, decode(repeat('00', 32), 'hex'), 0
  UNION ALL SELECT 4, 0, hash, 0 FROM transaction WHERE internal_id = 11
  UNION ALL SELECT 5, 0, hash, 0 FROM transaction WHERE internal_id = 11
  UNION ALL SELECT 6, 0, hash, 0 FROM transaction WHERE internal_id = 12
  UNION ALL SELECT 7, 0, hash, 0 FROM transaction WHERE internal_id = 12
  UNION ALL SELECT 8, 0, hash, 1 FROM transaction WHERE internal_id = 13
  UNION ALL SELECT 9, 0, hash, 0 FROM transaction WHERE internal_id = 13;
INSERT INTO node_transaction (node_internal_id, transaction_internal_id)
  VALUES (1, 1), (1, 2), (1, 4), (1, 6), (1, 8);
INSERT INTO block_transaction VALUES (1, 1), (2, 3), (3, 5), (4, 9);
INSERT INTO node_block VALUES
  (2, 1, '2026-01-02'), (1, 2, '2026-01-02'),
  (2, 3, '2026-01-02'), (1, 4, '2026-01-02');
`);
    t.deepEqual(await archive(), []);
    t.deepEqual((await pool.query(historyQuery)).rows, []);
    t.deepEqual((await pool.query(membershipQuery)).rows, [
      { node: 1, tx: 1 },
      { node: 1, tx: 2 },
      { node: 1, tx: 4 },
      { node: 1, tx: 6 },
      { node: 1, tx: 8 },
    ]);
  }
);

test.serial('[e2e] cleanup accepts an empty mempool', async (t) => {
  t.deepEqual(
    await t.context.db.archiveMempoolTransactionsAcceptedByBlocks(),
    []
  );
});
