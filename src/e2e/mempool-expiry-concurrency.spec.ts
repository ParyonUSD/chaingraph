/* eslint-disable @typescript-eslint/no-magic-numbers */
import { readFileSync } from 'fs';

import type { ExecutionContext, TestFn } from 'ava';
import baseTest from 'ava';

import type * as database from '../db.js';

// cspell:words autovacuum lpad xact
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
  block_internal_id bigint, accepted_at timestamp,
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

/** The expiry queue's batch size (`expiryBatchSize` in db.ts). */
const expiryQueueBatchSize = 500;
interface MempoolShape {
  chains: number;
  depth: number;
}
/*
 * Big enough for several expiry queue batches and many concurrent single-row
 * expiries of related rows; small enough that the single-row test, which
 * serializes one transaction per row, stays fast on a slow CI runner.
 */
const mempoolShape: MempoolShape = { chains: 20, depth: 15 };
const mempoolRowCount = ({ chains, depth }: MempoolShape) => chains * depth * 2;

/**
 * `chains` chains of `depth` mempool transactions (each spends output 0 of the
 * previous one), every transaction also fanning out to the next chain's
 * transaction at the same depth, all validated at the same moment for two
 * nodes: a restarted agent's stale mempool, all expiring at once.
 */
const createStaleMempool = async (
  db: typeof database,
  validatedAt = `'2026-01-01'::timestamp`,
  { chains, depth } = mempoolShape
) => {
  await db.pool.query(/* sql */ `
TRUNCATE node, transaction, output, input, node_transaction,
  node_transaction_history, block_transaction, node_block;
-- production node ids are integers; node registration consumes sequence
-- values even for known nodes, so ids above the smallint range are routine
INSERT INTO node VALUES (40001, 'alpha'), (40002, 'beta');
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
  SELECT node.internal_id, transaction.internal_id, ${validatedAt}
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
  errors: unknown[],
  replaced = '2026-01-01 01:00:00'
) => {
  t.deepEqual(
    errors.map((err) => (err as { code?: string }).code ?? err),
    [],
    'no expiry fails (40P01 = deadlock)'
  );
  t.deepEqual(await countRows(t.context.db), {
    current: 0,
    history: mempoolRowCount(mempoolShape),
  });
  const replacedAt = await t.context.db.pool.query<{ replaced: string }>(
    /* sql */ `SELECT DISTINCT replaced_at::text AS replaced FROM node_transaction_history;`
  );
  t.deepEqual(replacedAt.rows, [{ replaced }]);
};

const staleExpiries = async (
  db: typeof database,
  validatedAt?: string,
  shape = mempoolShape
) => {
  await createStaleMempool(db, validatedAt, shape);
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
 * Regression (2026-10-13 backfill lab): a restarted agent's 6-day-stale mempool
 * expired all at once, one archive statement per timer on every pool
 * connection. Archiving a transaction cascades to its descendants (history
 * trigger), so a parent's and a descendant's expiry locked the same rows in
 * opposite orders: >1,300 deadlocks and no net progress. Before the fix this
 * test failed with dozens of 40P01 errors (or timed out).
 */
test.serial(
  '[e2e] concurrent single-row expiries of a stale mempool archive every row without deadlocks',
  async (t) => {
    const { db } = t.context;
    const expiries = await staleExpiries(db);
    t.is(expiries.length, mempoolRowCount(mempoolShape));
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

/*
 * Every transaction descends from transaction 1, the first validated, so every
 * row must record its expiry, also when a descendant's expiry is queued before
 * its ancestors' and the queue needs several batches.
 */
test.serial(
  '[e2e] the expiry queue archives every descendant with its earliest ancestor expiry',
  async (t) => {
    const { db } = t.context;
    const expiries = await staleExpiries(
      db,
      `'2026-01-01'::timestamp + transaction.internal_id * interval '1 second'`
    );
    const errors: unknown[] = [];
    await Promise.all(
      expiries.map(async (expiry) =>
        db.expireMempoolTransaction(expiry).catch((err: unknown) => {
          errors.push(err);
        })
      )
    );
    await expectAllArchived(t, errors, '2026-01-01 01:00:01');
  }
);

/* eslint-disable @typescript-eslint/naming-convention */
interface PlanNode {
  'Actual Loops': number;
  'Actual Rows': number;
  'Node Type': string;
  Plans?: PlanNode[];
  'Relation Name'?: string;
  'Rows Removed by Filter'?: number;
  'Rows Removed by Index Recheck'?: number;
}
/* eslint-enable @typescript-eslint/naming-convention */
const rowsReadPerLoop = (node: PlanNode) =>
  node['Actual Rows'] +
  (node['Rows Removed by Filter'] ?? 0) +
  (node['Rows Removed by Index Recheck'] ?? 0);
/**
 * Rows of `relation` read by the scans of an EXPLAIN ANALYZE plan: those
 * emitted plus those discarded by the scan's filter or index recheck (all
 * reported per loop).
 */
const rowsScanned = (node: PlanNode, relation: string): number =>
  (node['Relation Name'] === relation && node['Node Type'].endsWith('Scan')
    ? rowsReadPerLoop(node) * node['Actual Loops']
    : 0) +
  (node.Plans ?? []).reduce(
    (total, child) => total + rowsScanned(child, relation),
    0
  );

/*
 * Regression: with node_transaction statistics taken when the mempool was
 * nearly empty (as autovacuum may analyze it just after a block), Postgres
 * joined each walked descendant to node_transaction by node alone, reading the
 * node's whole mempool per walked row: archiving a stale mempool took time
 * quadratic in its size (here ~485,000 node_transaction rows read rather than
 * ~20,000; tens of seconds per batch on CI).
 */
test.serial(
  '[e2e] archiving a stale mempool reads each node_transaction row a few times, also with outdated statistics',
  async (t) => {
    const { db } = t.context;
    // larger, so the quadratic plan reads far more rows than the bound below
    const largeMempool = { chains: 60, depth: 40 };
    const mempoolRows = mempoolRowCount(largeMempool);
    // statistics of a nearly empty mempool, then the stale mempool
    await createStaleMempool(db, undefined, largeMempool);
    await db.pool.query(/* sql */ `
DELETE FROM node_transaction WHERE transaction_internal_id > 3;
ANALYZE node_transaction;`);
    // one expiry queue batch, including transaction 1: the ancestor of all
    const expiries = (await staleExpiries(db, undefined, largeMempool))
      .sort((a, b) => a.transactionInternalId - b.transactionInternalId)
      .slice(0, expiryQueueBatchSize);
    const client = await db.pool.connect();
    // eslint-disable-next-line functional/no-try-statement
    try {
      await client.query('BEGIN;');
      const explained = await client.query<{
        // eslint-disable-next-line @typescript-eslint/naming-convention
        'QUERY PLAN': [{ Plan: PlanNode }];
      }>(
        `EXPLAIN (ANALYZE, FORMAT JSON) ${db.mempoolExpiryArchivalStatement}`,
        [
          expiries.map((expiry) => expiry.nodeInternalId),
          expiries.map((expiry) => expiry.transactionInternalId),
          expiries.map((expiry) => expiry.replacedAt.toISOString()),
          expiries.map(() => null),
        ]
      );
      const scanned = rowsScanned(
        explained.rows[0]!['QUERY PLAN'][0].Plan,
        'node_transaction'
      );
      t.log(`node_transaction rows scanned: ${scanned}`);
      t.true(scanned <= mempoolRows * 10, `scanned ${scanned} rows`);
      const archived = await client.query<{ count: string }>(
        /* sql */ `SELECT count(*)::text FROM node_transaction_history;`
      );
      t.is(Number(archived.rows[0]!.count), mempoolRows);
    } finally {
      await client.query('ROLLBACK;');
      client.release();
    }
  }
);

/*
 * Expire transaction 1 of a two-transaction mempool for alpha (2 spends 1, and
 * `prepare` runs first) while another connection's transaction, `blocker`,
 * holds a lock the expiry waits for. Once the expiry waits, `concurrently` is
 * committed on a third connection, then `blocker`.
 */
const expireWhileBlocked = async (
  db: typeof database,
  {
    blocker,
    concurrently = '',
    prepare = '',
  }: { blocker: string; concurrently?: string; prepare?: string }
) => {
  await db.pool.query(/* sql */ `
TRUNCATE node, transaction, output, input, node_transaction,
  node_transaction_history, block_transaction, node_block;
INSERT INTO node VALUES (40001, 'alpha');
INSERT INTO transaction
  SELECT id, sha256(int8send(id)) FROM generate_series(1, 2) id;
INSERT INTO output SELECT hash, 0 FROM transaction;
INSERT INTO input SELECT 2, 0, hash, 0 FROM transaction WHERE internal_id = 1;
INSERT INTO node_transaction
  SELECT 40001, internal_id, '2026-01-01' FROM transaction;
${prepare}
`);
  const blocking = await db.pool.connect();
  const concurrent = await db.pool.connect();
  // eslint-disable-next-line functional/no-try-statement
  try {
    await blocking.query(`BEGIN; ${blocker}`);
    const expiry = db.archiveMempoolTransactions([
      {
        nodeInternalId: 40001,
        replacedAt: new Date('2026-01-01T01:00:00.000Z'),
        transactionInternalId: 1,
      },
    ]);
    const waitForExpiryToBlock = async (): Promise<void> => {
      const waiting = await db.pool.query(
        /* sql */ `SELECT 1 FROM pg_locks WHERE NOT granted;`
      );
      if (waiting.rowCount === 0) {
        await new Promise((resolve) => {
          setTimeout(resolve, 10);
        });
        return waitForExpiryToBlock();
      }
      return undefined;
    };
    await waitForExpiryToBlock();
    if (concurrently !== '') {
      await concurrent.query(concurrently);
    }
    await blocking.query('COMMIT;');
    return await expiry;
  } finally {
    blocking.release();
    concurrent.release();
  }
};

const currentAndHistory = async (db: typeof database) =>
  (
    await db.pool.query<{
      replaced: string | null;
      table: string;
      tx: number;
    }>(/* sql */ `
SELECT 'current' AS table, transaction_internal_id::integer AS tx,
       NULL AS replaced
  FROM node_transaction
UNION ALL
SELECT 'history', transaction_internal_id::integer, replaced_at::text
  FROM node_transaction_history
ORDER BY 1, 2;`)
  ).rows;

/*
 * Regression: the advisory lock was taken inside the archival statement, whose
 * snapshot predates the wait, so a block inclusion committed while it waited
 * (here, block_transaction repaired for a block the node already accepted)
 * was invisible to the accepted-block guard and the confirmed transaction was
 * archived with its expiry.
 */
test.serial(
  '[e2e] expiry sees a block inclusion committed while it waits for the expiry lock',
  async (t) => {
    const { db } = t.context;
    const archived = await expireWhileBlocked(db, {
      // another agent's expiry batch
      blocker: `SELECT pg_advisory_xact_lock(${db.expiryAdvisoryLockKey});`,
      concurrently: `INSERT INTO block_transaction VALUES (1, 1);`,
      prepare: `INSERT INTO node_block VALUES (40001, 1, '2026-01-02');`,
    });
    t.deepEqual(archived, []);
    t.deepEqual(await currentAndHistory(db), [
      { replaced: null, table: 'current', tx: 1 },
      { replaced: null, table: 'current', tx: 2 },
    ]);
  }
);

/*
 * Regression: an expiring parent confirmed concurrently (deleted by block
 * acceptance, as by the node_block trigger) was skipped by the archival
 * statement's row locks, but its snapshot still had the parent, so its
 * unconfirmed child was archived with the parent's expiry.
 */
test.serial(
  '[e2e] expiry leaves the child of a parent confirmed while the expiry waits',
  async (t) => {
    const { db } = t.context;
    const archived = await expireWhileBlocked(db, {
      blocker: `INSERT INTO block_transaction VALUES (1, 1);
INSERT INTO node_block VALUES (40001, 1, '2026-01-02');
WITH confirmed AS (
  DELETE FROM node_transaction
    WHERE node_internal_id = 40001 AND transaction_internal_id = 1
    RETURNING node_internal_id, transaction_internal_id, validated_at)
INSERT INTO node_transaction_history
  SELECT node_internal_id, transaction_internal_id, validated_at, NULL
    FROM confirmed;`,
    });
    t.deepEqual(archived, []);
    t.deepEqual(await currentAndHistory(db), [
      { replaced: null, table: 'current', tx: 2 },
      { replaced: null, table: 'history', tx: 1 },
    ]);
  }
);

/*
 * Regression: the archival statement walked descendants in a snapshot taken
 * before it waited for their row locks. For P (1) → C (2) → G (3) with only P
 * expiring, C confirmed concurrently (deleted with replaced_at NULL, ending
 * the cascade) was skipped by the row locks, but the walk had already reached
 * G through the old C, so G was archived with P's expiry.
 */
test.serial(
  '[e2e] expiry leaves the grandchild of a child confirmed while the expiry waits',
  async (t) => {
    const { db } = t.context;
    const archived = await expireWhileBlocked(db, {
      blocker: `INSERT INTO block_transaction VALUES (1, 2);
INSERT INTO node_block VALUES (40001, 1, '2026-01-02');
WITH confirmed AS (
  DELETE FROM node_transaction
    WHERE node_internal_id = 40001 AND transaction_internal_id = 2
    RETURNING node_internal_id, transaction_internal_id, validated_at)
INSERT INTO node_transaction_history
  SELECT node_internal_id, transaction_internal_id, validated_at, NULL
    FROM confirmed;`,
      prepare: `INSERT INTO transaction VALUES (3, sha256(int8send(3::bigint)));
INSERT INTO output VALUES (sha256(int8send(3::bigint)), 0);
INSERT INTO input SELECT 3, 0, hash, 0 FROM transaction WHERE internal_id = 2;
INSERT INTO node_transaction VALUES (40001, 3, '2026-01-01');`,
    });
    t.deepEqual(
      archived.map((expiry) => expiry.transactionInternalId),
      [1]
    );
    t.deepEqual(await currentAndHistory(db), [
      { replaced: null, table: 'current', tx: 3 },
      { replaced: '2026-01-01 01:00:00', table: 'history', tx: 1 },
      { replaced: null, table: 'history', tx: 2 },
    ]);
  }
);
