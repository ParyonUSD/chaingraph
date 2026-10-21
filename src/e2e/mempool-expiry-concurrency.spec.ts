/* eslint-disable @typescript-eslint/no-magic-numbers */
import { readFileSync } from 'fs';

import type { ExecutionContext, TestFn } from 'ava';
import baseTest from 'ava';

import type * as database from '../db.js';

// cspell:words lpad
const test = baseTest as TestFn<{ db: typeof database; schema: string }>;
const host = process.env.CHAINGRAPH_E2E_POSTGRES_HOST ?? 'localhost';
const port = process.env.CHAINGRAPH_E2E_POSTGRES_PORT ?? '5432';
const schema = `expiry_concurrency_${process.pid}`;
const poolSize = 16;

/*
 * Real (committed) tables in a scratch schema, so many pool connections see the
 * same rows and take real row locks: the stale-mempool expiry of a restarted
 * agent fires one timer per expired transaction at once.
 */
test.before(async (t) => {
  process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING = `postgres://chaingraph:very_insecure_postgres_password@${host}:${port}/postgres?options=${encodeURIComponent(
    `-c search_path=${schema} -c deadlock_timeout=50ms`
  )}`;
  process.env.CHAINGRAPH_POSTGRES_MAX_CONNECTIONS = `${poolSize}`;
  t.context.db = await import('../db.js');
  t.context.schema = schema;
  const migration = readFileSync(
    new URL(
      '../../images/hasura/hasura-data/migrations/default/1778151011521_cascade_invalidate_mempool_descendants/up.sql',
      import.meta.url
    ),
    'utf8'
  );
  const cascadeFunction = migration.split('CREATE TRIGGER')[0]!;
  await t.context.db.pool.query(/* sql */ `
DROP SCHEMA IF EXISTS ${schema} CASCADE;
CREATE SCHEMA ${schema};
SET search_path = ${schema};
CREATE TABLE node (internal_id integer PRIMARY KEY, name text);
CREATE TABLE transaction (internal_id bigint PRIMARY KEY, hash bytea UNIQUE);
CREATE TABLE output (transaction_hash bytea, output_index integer,
  PRIMARY KEY (transaction_hash, output_index));
CREATE TABLE input (transaction_internal_id bigint, input_index integer,
  outpoint_transaction_hash bytea, outpoint_index integer,
  PRIMARY KEY (transaction_internal_id, input_index));
CREATE INDEX input_outpoint ON input (outpoint_transaction_hash, outpoint_index);
CREATE TABLE node_transaction (node_internal_id integer,
  transaction_internal_id bigint, validated_at timestamp NOT NULL,
  PRIMARY KEY (node_internal_id, transaction_internal_id));
CREATE TABLE node_transaction_history (node_internal_id integer,
  transaction_internal_id bigint, validated_at timestamp, replaced_at timestamp,
  PRIMARY KEY (node_internal_id, transaction_internal_id, validated_at));
CREATE TABLE block_transaction (block_internal_id bigint,
  transaction_internal_id bigint,
  PRIMARY KEY (transaction_internal_id, block_internal_id));
CREATE TABLE node_block (node_internal_id integer,
  block_internal_id bigint, accepted_at timestamp NOT NULL,
  PRIMARY KEY (node_internal_id, block_internal_id));
${cascadeFunction}
CREATE TRIGGER cascade_history AFTER INSERT ON node_transaction_history
  REFERENCING NEW TABLE AS new_table FOR EACH STATEMENT
  EXECUTE FUNCTION trigger_node_transaction_history_insert();
`);
});

test.after.always(async (t) => {
  await t.context.db.pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE;`);
  await t.context.db.pool.end();
});

const chains = 60;
const depth = 40;

/**
 * `chains` chains of `depth` mempool transactions (each spends output 0 of the
 * previous one), every transaction also fanning out to the next chain's
 * transaction at the same depth, all validated at the same moment for two
 * nodes: a restarted agent's stale mempool, all expiring at once.
 */
const createStaleMempool = async (db: typeof database) => {
  await db.pool.query(/* sql */ `
TRUNCATE node, transaction, output, input, node_transaction,
  node_transaction_history, block_transaction, node_block;
INSERT INTO node VALUES (1, 'alpha'), (2, 'beta');
INSERT INTO transaction
  SELECT id, sha256(int8send(id)) FROM generate_series(1, ${chains * depth}) id;
INSERT INTO output SELECT hash, o FROM transaction, generate_series(0, 1) o;
-- tx (c, d) has internal id c * depth + d + 1
INSERT INTO input
  SELECT c * ${depth} + d + 1, 0, parent.hash, 0
    FROM generate_series(0, ${chains - 1}) c CROSS JOIN generate_series(1, ${
    depth - 1
  }) d
    JOIN transaction parent ON parent.internal_id = c * ${depth} + d
  UNION ALL
  SELECT c * ${depth} + d + 1, 1, parent.hash, 1
    FROM generate_series(1, ${chains - 1}) c CROSS JOIN generate_series(0, ${
    depth - 1
  }) d
    JOIN transaction parent ON parent.internal_id = (c - 1) * ${depth} + d + 1;
INSERT INTO node_transaction
  SELECT node.internal_id, transaction.internal_id, '2026-01-01'
    FROM node CROSS JOIN transaction;
`);
};

const countRows = async (db: typeof database) => {
  const result = await db.pool.query<{
    current: string;
    history: string;
  }>(/* sql */ `
SELECT (SELECT count(*) FROM node_transaction)::text AS current,
       (SELECT count(*) FROM node_transaction_history)::text AS history;`);
  return {
    current: Number(result.rows[0]!.current),
    history: Number(result.rows[0]!.history),
  };
};

const shuffled = <T>(items: T[], seed: number) => {
  const copy = [...items];
  // eslint-disable-next-line functional/no-let
  let state = seed;
  // eslint-disable-next-line functional/no-loop-statement, functional/no-let
  for (let i = copy.length - 1; i > 0; i -= 1) {
    // eslint-disable-next-line no-bitwise
    state = (state * 1103515245 + 12345) & 0x7fffffff;
    const j = state % (i + 1);
    [copy[i], copy[j]] = [copy[j]!, copy[i]!];
  }
  return copy;
};

const expectAllArchived = async (
  t: ExecutionContext<{ db: typeof database; schema: string }>,
  errors: unknown[]
) => {
  t.deepEqual(
    errors.map((err) => (err as { code?: string }).code ?? err),
    [],
    'no expiry fails (40P01 = deadlock)'
  );
  t.deepEqual(await countRows(t.context.db), {
    current: 0,
    history: chains * depth * 2,
  });
  const replacedAt = await t.context.db.pool.query<{ replaced: string }>(
    /* sql */ `SELECT DISTINCT replaced_at::text AS replaced FROM node_transaction_history;`
  );
  t.deepEqual(replacedAt.rows, [{ replaced: '2026-01-01 01:00:00' }]);
};

const staleExpiries = async (db: typeof database) => {
  await createStaleMempool(db);
  const expiring = await db.getMempoolTransactionsExpiringBefore({
    expirationMs: 3_600_000,
    expiresBefore: new Date('2026-01-10T00:00:00.000Z'),
  });
  return shuffled(expiring, 7).map((row) => ({
    nodeInternalId: row.nodeInternalId,
    replacedAt: row.expiresAt,
    transactionInternalId: row.transactionInternalId,
  }));
};

/*
 * A restarted agent's stale mempool expires all at once, one timer per
 * transaction. Archiving a transaction cascades to its descendants (history
 * trigger), so racing single-row expiries of a parent and a descendant would
 * lock the same rows in opposite orders and deadlock (40P01) without net
 * progress; every row must be archived without a deadlock.
 */
test.serial(
  '[e2e] concurrent single-row expiries of a stale mempool archive every row without deadlocks',
  async (t) => {
    const { db } = t.context;
    const expiries = await staleExpiries(db);
    t.is(expiries.length, chains * depth * 2);
    const errors: unknown[] = [];
    await Promise.all(
      expiries.map(async (expiry) =>
        db.archiveMempoolTransaction(expiry).catch((err: unknown) => {
          errors.push(err);
        })
      )
    );
    await expectAllArchived(t, errors);
  }
);

test.serial(
  '[e2e] concurrent expiry batches serialize and archive every row',
  async (t) => {
    const { db } = t.context;
    const expiries = await staleExpiries(db);
    const batchCount = poolSize;
    const errors: unknown[] = [];
    await Promise.all(
      Array.from({ length: batchCount }, async (_, batch) =>
        db
          .archiveMempoolTransactions(
            expiries.filter((_expiry, index) => index % batchCount === batch)
          )
          .catch((err: unknown) => {
            errors.push(err);
          })
      )
    );
    await expectAllArchived(t, errors);
  }
);

test.serial(
  '[e2e] the expiry queue archives a stale mempool firing at once in a few ordered batches',
  async (t) => {
    const { db } = t.context;
    const expiries = await staleExpiries(db);
    const errors: unknown[] = [];
    const results = await Promise.all(
      expiries.map(async (expiry) =>
        db.expireMempoolTransaction(expiry).catch((err: unknown) => {
          errors.push(err);
          return undefined;
        })
      )
    );
    await expectAllArchived(t, errors);
    const archivedDirectly = results.filter((archived) => archived === true);
    t.true(archivedDirectly.length > 0);
    t.is(
      archivedDirectly.length +
        results.filter((archived) => archived === false).length,
      expiries.length
    );
    // a duplicate request for an already-archived row resolves false
    t.false(await db.expireMempoolTransaction(expiries[0]!));
  }
);

/**
 * Node 40000 (node ids are `integer`) has a mempool chain (parent, child,
 * grandchild); the parent and the child expire in one batch at different
 * times, the grandchild is not in the batch.
 */
const createExpiringChain = async (db: typeof database) => {
  await db.pool.query(/* sql */ `
TRUNCATE node, transaction, output, input, node_transaction,
  node_transaction_history, block_transaction, node_block;
INSERT INTO node VALUES (40000, 'large-id');
INSERT INTO transaction
  SELECT id, sha256(int8send(id)) FROM generate_series(1, 3) id;
INSERT INTO output SELECT hash, 0 FROM transaction;
INSERT INTO input
  SELECT child.internal_id, 0, parent.hash, 0
    FROM transaction child
    JOIN transaction parent ON parent.internal_id = child.internal_id - 1
    WHERE child.internal_id > 1;
INSERT INTO node_transaction
  SELECT 40000, internal_id, '2026-01-01' FROM transaction;
`);
};

test.serial(
  '[e2e] expiry archives rows of nodes with ids above the smallint range',
  async (t) => {
    const { db } = t.context;
    await createExpiringChain(db);
    const archived = await db.archiveMempoolTransactions([
      {
        nodeInternalId: 40000,
        replacedAt: new Date('2026-01-01T01:00:00.000Z'),
        transactionInternalId: 1,
      },
    ]);
    t.is(archived.length, 1);
    t.deepEqual(await countRows(db), { current: 0, history: 3 });
  }
);

test.serial(
  '[e2e] a descendant expiring in the same batch as its ancestor inherits the earlier replaced_at',
  async (t) => {
    const { db } = t.context;
    await createExpiringChain(db);
    const archived = await db.archiveMempoolTransactions([
      {
        nodeInternalId: 40000,
        replacedAt: new Date('2026-01-01T02:00:00.000Z'),
        transactionInternalId: 2,
      },
      {
        nodeInternalId: 40000,
        replacedAt: new Date('2026-01-01T01:00:00.000Z'),
        transactionInternalId: 1,
      },
    ]);
    t.is(archived.length, 2);
    const history = await db.pool.query<{
      replaced: string;
      tx: string;
    }>(/* sql */ `SELECT transaction_internal_id::text AS tx, replaced_at::text AS replaced
        FROM node_transaction_history ORDER BY transaction_internal_id;`);
    t.deepEqual(history.rows, [
      { replaced: '2026-01-01 01:00:00', tx: '1' },
      { replaced: '2026-01-01 01:00:00', tx: '2' },
      { replaced: '2026-01-01 01:00:00', tx: '3' },
    ]);
  }
);
