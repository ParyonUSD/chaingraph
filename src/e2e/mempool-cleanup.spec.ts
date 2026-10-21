import { readFileSync } from 'fs';

import type { TestFn } from 'ava';
import baseTest from 'ava';

import type * as database from '../db.js';

// cspell:words lpad
const test = baseTest as TestFn<{ db: typeof database }>;
const host = process.env.CHAINGRAPH_E2E_POSTGRES_HOST ?? 'localhost';
const port = process.env.CHAINGRAPH_E2E_POSTGRES_PORT ?? '5432';

/*
 * A single connection keeps every operation inside the same temporary schema,
 * discarded after each test (not a rollback transaction: expiry runs its own
 * transaction). No persistent tables are created or modified.
 */
test.before(async (t) => {
  process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING = `postgres://chaingraph:very_insecure_postgres_password@${host}:${port}/postgres`;
  process.env.CHAINGRAPH_POSTGRES_MAX_CONNECTIONS = '1';
  t.context.db = await import('../db.js');
});

test.beforeEach(async (t) => {
  await t.context.db.pool.query(/* sql */ `
SET search_path = pg_temp;
SET statement_timeout = '5s';
CREATE TEMP TABLE node (internal_id integer PRIMARY KEY, name text);
CREATE TEMP TABLE transaction (internal_id bigint PRIMARY KEY, hash bytea UNIQUE);
CREATE TEMP TABLE output (transaction_hash bytea, output_index integer,
  PRIMARY KEY (transaction_hash, output_index));
CREATE TEMP TABLE input (transaction_internal_id bigint, input_index integer,
  outpoint_transaction_hash bytea, outpoint_index integer,
  PRIMARY KEY (transaction_internal_id, input_index));
CREATE INDEX input_outpoint ON input (outpoint_transaction_hash, outpoint_index);
CREATE TEMP TABLE node_transaction (node_internal_id integer,
  transaction_internal_id bigint, validated_at timestamp DEFAULT '2026-01-01',
  PRIMARY KEY (node_internal_id, transaction_internal_id));
CREATE TEMP TABLE node_transaction_history (node_internal_id integer,
  transaction_internal_id bigint, validated_at timestamp, replaced_at timestamp,
  PRIMARY KEY (node_internal_id, transaction_internal_id, validated_at));
CREATE TEMP TABLE block_transaction (block_internal_id bigint,
  transaction_internal_id bigint,
  PRIMARY KEY (transaction_internal_id, block_internal_id));
CREATE TEMP TABLE node_block (node_internal_id integer,
  block_internal_id bigint, accepted_at timestamp,
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
  await t.context.db.pool.query('DISCARD TEMP;');
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

/*
 * Regression: the agent's mempool scan runs archiveMempoolTransactionsAcceptedByBlocks
 * and then getMempoolTransactionsExpiringBefore as two separate statements, and
 * expires the rows returned by the second via archiveMempoolTransaction. A row
 * inserted between the two (e.g. an announcement of an already-confirmed
 * transaction with an old validated_at) used to be archived as expired, with
 * replaced_at = expiry, although the node had accepted it in a block. Expiry
 * must leave such rows for the accepted-block archive.
 */
test.serial(
  '[e2e] expiry never archives a transaction accepted or replaced by a block for that node',
  async (t) => {
    const {
      pool,
      archiveMempoolTransaction,
      archiveMempoolTransactionsAcceptedByBlocks: archive,
      getMempoolTransactionsExpiringBefore,
    } = t.context.db;
    await pool.query(/* sql */ `
INSERT INTO input
  SELECT 4, 0, hash, 0 FROM transaction WHERE internal_id = 10
  UNION ALL SELECT 5, 0, hash, 0 FROM transaction WHERE internal_id = 10;
-- tx 1 is confirmed and tx 5 (conflicting with tx 4) is mined, for alpha only.
INSERT INTO block_transaction VALUES (1, 1), (2, 5);
INSERT INTO node_block VALUES (1, 1, '2026-01-02'), (1, 2, '2026-01-03');
INSERT INTO node_transaction (node_internal_id, transaction_internal_id)
  VALUES (1, 3);
`);
    // Scan step 1: the accepted-block archive finds nothing to do yet.
    t.deepEqual(await archive(), []);
    // A concurrent insert lands between the two scan steps.
    await pool.query(/* sql */ `
INSERT INTO node_transaction (node_internal_id, transaction_internal_id)
  VALUES (1, 1), (1, 4), (2, 1), (2, 4);
`);
    // Scan step 2: every row is already past expiry, so all get timers.
    const expiring = await getMempoolTransactionsExpiringBefore({
      expirationMs: 3_600_000,
      expiresBefore: new Date('2026-01-10T00:00:00.000Z'),
    });
    t.deepEqual(
      expiring.map((row) => ({
        node: row.nodeName,
        tx: row.transactionInternalId,
      })),
      [
        { node: 'alpha', tx: 1 },
        { node: 'alpha', tx: 3 },
        { node: 'alpha', tx: 4 },
        { node: 'beta', tx: 1 },
        { node: 'beta', tx: 4 },
      ]
    );
    // Timers fire: only rows not resolved by a block accepted by that node expire.
    const archivedCounts = await expiring.reduce<Promise<number[]>>(
      async (previous, row) => [
        ...(await previous),
        await archiveMempoolTransaction({
          nodeInternalId: row.nodeInternalId,
          replacedAt: row.expiresAt,
          transactionInternalId: row.transactionInternalId,
        }),
      ],
      Promise.resolve([])
    );
    t.deepEqual(archivedCounts, [0, 1, 0, 1, 1]);
    t.deepEqual((await pool.query(membershipQuery)).rows, [
      { node: 1, tx: 1 },
      { node: 1, tx: 4 },
    ]);
    // The next scan archives them with the block-derived replaced_at.
    t.deepEqual(await archive(), [
      { hash: hash('01'), nodeName: 'alpha', replacedAt: null },
      {
        hash: hash('04'),
        nodeName: 'alpha',
        replacedAt: new Date('2026-01-03T00:00:00.000Z'),
      },
    ]);
    t.deepEqual((await pool.query(membershipQuery)).rows, []);
    t.deepEqual((await pool.query(historyQuery)).rows, [
      { node: 1, replaced: null, tx: 1 },
      { node: 1, replaced: '2026-01-01 01:00:00', tx: 3 },
      { node: 1, replaced: '2026-01-03 00:00:00', tx: 4 },
      { node: 2, replaced: '2026-01-01 01:00:00', tx: 1 },
      { node: 2, replaced: '2026-01-01 01:00:00', tx: 4 },
    ]);
  }
);

/*
 * Regression: a batch holding an expired parent and an expired descendant
 * deleted both directly, so the descendant (and, via the history trigger,
 * everything below it) recorded its own, later expiry instead of the
 * parent's earlier one.
 */
test.serial(
  '[e2e] a batched expiry records the earliest ancestor expiry for every descendant',
  async (t) => {
    const { pool, archiveMempoolTransactions } = t.context.db;
    // chain 1 <- 2 <- 3 <- 4; only 1 and 3 are in the batch
    await pool.query(/* sql */ `
INSERT INTO input
  SELECT 2, 0, hash, 0 FROM transaction WHERE internal_id = 1
  UNION ALL SELECT 3, 0, hash, 0 FROM transaction WHERE internal_id = 2
  UNION ALL SELECT 4, 0, hash, 0 FROM transaction WHERE internal_id = 3;
INSERT INTO node_transaction (node_internal_id, transaction_internal_id)
  SELECT 1, tx FROM generate_series(1, 4) tx;
`);
    const archived = await archiveMempoolTransactions([
      {
        nodeInternalId: 1,
        replacedAt: new Date('2026-01-01T02:00:00.000Z'),
        transactionInternalId: 3,
      },
      {
        nodeInternalId: 1,
        replacedAt: new Date('2026-01-01T01:00:00.000Z'),
        transactionInternalId: 1,
      },
    ]);
    t.deepEqual(
      archived.map((expiry) => ({ tx: expiry.transactionInternalId })),
      [{ tx: 1 }, { tx: 3 }]
    );
    t.deepEqual((await pool.query(membershipQuery)).rows, []);
    t.deepEqual((await pool.query(historyQuery)).rows, [
      { node: 1, replaced: '2026-01-01 01:00:00', tx: 1 },
      { node: 1, replaced: '2026-01-01 01:00:00', tx: 2 },
      { node: 1, replaced: '2026-01-01 01:00:00', tx: 3 },
      { node: 1, replaced: '2026-01-01 01:00:00', tx: 4 },
    ]);
  }
);

/*
 * Regression: the block guard only covered the expired rows themselves. The
 * history trigger then archived a descendant also replaced by a block accepted
 * by the node (and the descendant's own descendants) with the parent's expiry,
 * so the accepted-block archive could no longer record the block's accepted_at.
 */
test.serial(
  '[e2e] expiry archives a descendant replaced by an accepted block with the block time',
  async (t) => {
    const { pool, archiveMempoolTransactions } = t.context.db;
    // 2 spends 1 and 10; 5 (mined for alpha only) also spends 10; 3 spends 2
    await pool.query(/* sql */ `
INSERT INTO input
  SELECT 2, 0, hash, 0 FROM transaction WHERE internal_id = 1
  UNION ALL SELECT 2, 1, hash, 0 FROM transaction WHERE internal_id = 10
  UNION ALL SELECT 5, 0, hash, 0 FROM transaction WHERE internal_id = 10
  UNION ALL SELECT 3, 0, hash, 0 FROM transaction WHERE internal_id = 2;
INSERT INTO block_transaction VALUES (1, 5);
INSERT INTO node_block VALUES (1, 1, '2026-01-03');
INSERT INTO node_transaction (node_internal_id, transaction_internal_id)
  SELECT node, tx FROM generate_series(1, 2) node, generate_series(1, 3) tx;
`);
    const replacedAt = new Date('2026-01-01T01:00:00.000Z');
    const archived = await archiveMempoolTransactions([
      { nodeInternalId: 1, replacedAt, transactionInternalId: 1 },
      { nodeInternalId: 2, replacedAt, transactionInternalId: 1 },
    ]);
    t.deepEqual(
      archived.map((expiry) => ({ node: expiry.nodeInternalId })),
      [{ node: 1 }, { node: 2 }]
    );
    t.deepEqual((await pool.query(membershipQuery)).rows, []);
    t.deepEqual((await pool.query(historyQuery)).rows, [
      { node: 1, replaced: '2026-01-01 01:00:00', tx: 1 },
      { node: 1, replaced: '2026-01-03 00:00:00', tx: 2 },
      { node: 1, replaced: '2026-01-03 00:00:00', tx: 3 },
      { node: 2, replaced: '2026-01-01 01:00:00', tx: 1 },
      { node: 2, replaced: '2026-01-01 01:00:00', tx: 2 },
      { node: 2, replaced: '2026-01-01 01:00:00', tx: 3 },
    ]);
  }
);

/*
 * Regression: blocks accepted before Chaingraph began monitoring have a NULL
 * accepted_at, which MIN(accepted_at) could not tell apart from no conflicting
 * block at all. Expiry then archived a root replaced by such a block with its
 * expiry, and gave such a descendant its ancestor's expiry, instead of leaving
 * the root for the accepted-block archive and giving the descendant the
 * accepted-block archive's NULL (which, as there, ends the cascade).
 */
test.serial(
  '[e2e] expiry treats a conflict in a block with a NULL accepted_at as replaced',
  async (t) => {
    const {
      pool,
      archiveMempoolTransactions,
      archiveMempoolTransactionsAcceptedByBlocks: archive,
    } = t.context.db;
    // 4 and 5 (mined) spend 10; chain 1 <- 2 <- 3, 2 and 6 (mined) spend 11
    await pool.query(/* sql */ `
INSERT INTO input
  SELECT 4, 0, hash, 0 FROM transaction WHERE internal_id = 10
  UNION ALL SELECT 5, 0, hash, 0 FROM transaction WHERE internal_id = 10
  UNION ALL SELECT 2, 0, hash, 0 FROM transaction WHERE internal_id = 1
  UNION ALL SELECT 2, 1, hash, 0 FROM transaction WHERE internal_id = 11
  UNION ALL SELECT 6, 0, hash, 0 FROM transaction WHERE internal_id = 11
  UNION ALL SELECT 3, 0, hash, 0 FROM transaction WHERE internal_id = 2;
INSERT INTO block_transaction VALUES (1, 5), (2, 6);
INSERT INTO node_block VALUES (1, 1, NULL), (1, 2, NULL);
INSERT INTO node_transaction (node_internal_id, transaction_internal_id)
  VALUES (1, 1), (1, 2), (1, 3), (1, 4);
`);
    const replacedAt = new Date('2026-01-01T01:00:00.000Z');
    const archived = await archiveMempoolTransactions([
      { nodeInternalId: 1, replacedAt, transactionInternalId: 1 },
      { nodeInternalId: 1, replacedAt, transactionInternalId: 4 },
    ]);
    t.deepEqual(
      archived.map((expiry) => ({ tx: expiry.transactionInternalId })),
      [{ tx: 1 }]
    );
    t.deepEqual((await pool.query(historyQuery)).rows, [
      { node: 1, replaced: '2026-01-01 01:00:00', tx: 1 },
      { node: 1, replaced: null, tx: 2 },
    ]);
    t.deepEqual(await archive(), [
      { hash: hash('04'), nodeName: 'alpha', replacedAt: null },
    ]);
    t.deepEqual((await pool.query(membershipQuery)).rows, [{ node: 1, tx: 3 }]);
  }
);

/*
 * Regression: an expiry scheduled for one validation also archived a later
 * validation of the same transaction (e.g. one restored by a mempool resync
 * after the first was confirmed and a re-org removed the block), and through the
 * history trigger that validation's descendants.
 */
test.serial(
  '[e2e] expiry leaves a transaction validated again after it was scheduled',
  async (t) => {
    const { pool, archiveMempoolTransactions } = t.context.db;
    // 2 spends 1; node 1 re-validated 1 after the expiry was scheduled
    await pool.query(/* sql */ `
INSERT INTO input SELECT 2, 0, hash, 0 FROM transaction WHERE internal_id = 1;
INSERT INTO node_transaction VALUES
  (1, 1, '2026-01-02 00:00:00.123456'), (1, 2, '2026-01-02'),
  (2, 1, '2026-01-01 00:00:00.123456');
`);
    const replacedAt = new Date('2026-01-01T01:00:00.000Z');
    const archived = await archiveMempoolTransactions([
      {
        nodeInternalId: 1,
        replacedAt,
        transactionInternalId: 1,
        validatedAt: new Date('2026-01-01T00:00:00.123Z'),
      },
      {
        nodeInternalId: 2,
        replacedAt,
        transactionInternalId: 1,
        validatedAt: new Date('2026-01-01T00:00:00.123Z'),
      },
    ]);
    t.deepEqual(
      archived.map((expiry) => ({ node: expiry.nodeInternalId })),
      [{ node: 2 }]
    );
    t.deepEqual((await pool.query(membershipQuery)).rows, [
      { node: 1, tx: 1 },
      { node: 1, tx: 2 },
    ]);
    t.deepEqual((await pool.query(historyQuery)).rows, [
      { node: 2, replaced: '2026-01-01 01:00:00', tx: 1 },
    ]);
  }
);
