import { readFileSync } from 'fs';

import type { ExecutionContext, TestFn } from 'ava';
import baseTest from 'ava';

import type * as database from '../db.js';
import type { MempoolCleanupVector } from '../store/mempool-cleanup.vectors.js';
import {
  defaultValidatedAt,
  mempoolCleanupVectors,
  randomVector,
  timestampToDate,
  vectorInputs,
  vectorNodeMempool,
  vectorTxHash,
  vectorTxId,
} from '../store/mempool-cleanup.vectors.js';
import { planMempoolExpiry } from '../store/mempool-graph.js';

// cspell:words lpad unnest savepoint
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
  await t.context.db.pool.query('ROLLBACK;');
});

test.after.always(async (t) => {
  await t.context.db.pool.end();
});

const membershipQuery = /* sql */ `
SELECT node_internal_id AS node, transaction_internal_id::integer AS tx
  FROM node_transaction ORDER BY node, tx;
`;
const historyQuery = /* sql */ `
SELECT node_internal_id AS node, transaction_internal_id::integer AS tx,
       replaced_at::text AS "replacedAt"
  FROM node_transaction_history ORDER BY node, tx;
`;

type Pool = typeof database.pool;

/*
 * The shared vectors (src/store/mempool-cleanup.vectors.ts) also run through
 * the pure planner in src/store/mempool-graph.spec.ts, so these tests are the
 * SQL half of a differential test.
 */
const loadVector = async (pool: Pool, vector: MempoolCleanupVector) => {
  const inputs = vectorInputs(vector);
  const outpointHashes = inputs.map((input) => vectorTxHash(input.outpointTx));
  const outpointIndexes = inputs.map((input) => input.outpointIndex);
  await pool.query(
    /* sql */ `
INSERT INTO input
  SELECT tx, input_index, decode(outpoint_hash, 'hex'), outpoint_index
    FROM unnest($1::bigint[], $2::integer[], $3::text[], $4::integer[])
      AS i (tx, input_index, outpoint_hash, outpoint_index);`,
    [
      inputs.map((input) => input.tx),
      inputs.map((input) => input.inputIndex),
      outpointHashes,
      outpointIndexes,
    ]
  );
  // Output 0 of every transaction exists; add any other spent output.
  await pool.query(
    /* sql */ `
INSERT INTO output
  SELECT DISTINCT decode(outpoint_hash, 'hex'), outpoint_index
    FROM unnest($1::text[], $2::integer[]) AS o (outpoint_hash, outpoint_index)
    WHERE decode(outpoint_hash, 'hex') IN (SELECT hash FROM transaction)
  ON CONFLICT DO NOTHING;`,
    [outpointHashes, outpointIndexes]
  );
  await pool.query(
    /* sql */ `
INSERT INTO node_transaction
  SELECT node, tx, validated_at::timestamp
    FROM unnest($1::smallint[], $2::bigint[], $3::text[])
      AS n (node, tx, validated_at);`,
    [
      vector.nodeTransactions.map((row) => row.node),
      vector.nodeTransactions.map((row) => row.tx),
      vector.nodeTransactions.map(
        (row) => row.validatedAt ?? defaultValidatedAt
      ),
    ]
  );
  await pool.query(
    /* sql */ `
INSERT INTO block_transaction SELECT * FROM unnest($1::bigint[], $2::bigint[]);`,
    [
      vector.blockTransactions.map((row) => row.block),
      vector.blockTransactions.map((row) => row.tx),
    ]
  );
  await pool.query(
    /* sql */ `
INSERT INTO node_block
  SELECT node, block, accepted_at::timestamp
    FROM unnest($1::smallint[], $2::bigint[], $3::text[])
      AS b (node, block, accepted_at);`,
    [
      vector.nodeBlocks.map((row) => row.node),
      vector.nodeBlocks.map((row) => row.block),
      vector.nodeBlocks.map((row) => row.acceptedAt),
    ]
  );
};

const nodeName = (vector: MempoolCleanupVector, node: number) =>
  vector.nodes.find((candidate) => candidate.internalId === node)!.name;

/**
 * The vector's expected direct archive rows in the shape returned by
 * `archiveMempoolTransactionsAcceptedByBlocks`.
 */
const expectedArchiveResult = (vector: MempoolCleanupVector) =>
  vector.expected.archived.map((row) => ({
    hash: vectorTxHash(row.tx),
    nodeName: nodeName(vector, row.node),
    replacedAt:
      row.replacedAt === null ? null : timestampToDate(row.replacedAt),
  }));

const sweepAndCompare = async (
  t: ExecutionContext<{ db: typeof database }>,
  vector: MempoolCleanupVector
) => {
  const { pool, archiveMempoolTransactionsAcceptedByBlocks: archive } =
    t.context.db;
  await loadVector(pool, vector);
  t.deepEqual(await archive(), expectedArchiveResult(vector), vector.name);
  t.deepEqual(
    (await pool.query(membershipQuery)).rows,
    vector.expected.remaining,
    vector.name
  );
  t.deepEqual(
    (await pool.query(historyQuery)).rows,
    vector.expected.history,
    vector.name
  );
  t.deepEqual(await archive(), [], `${vector.name}: second sweep`);
};

mempoolCleanupVectors.forEach((vector) => {
  test.serial(
    `[e2e] [postgres] ${vector.description} (vector ${vector.name})`,
    async (t) => {
      await sweepAndCompare(t, vector);
    }
  );
});

/* eslint-disable no-await-in-loop */
const randomSeeds = Array.from({ length: 60 }, (_, i) => i + 1);

test.serial(
  '[e2e] [postgres] cleanup and expiry match the planner on random vectors',
  async (t) => {
    const { pool, archiveMempoolTransaction } = t.context.db;
    // eslint-disable-next-line functional/no-loop-statement
    for (const seed of randomSeeds) {
      const vector = randomVector(seed);
      await pool.query('SAVEPOINT random_vector;');
      await sweepAndCompare(t, vector);
      /*
       * Expire the first remaining transaction of each node, one node at a
       * time, and compare the new history rows with the planner's expiry.
       */
      const replacedAt = timestampToDate('2026-02-01 00:00:00');
      // eslint-disable-next-line functional/no-loop-statement
      for (const { internalId: node } of vector.nodes) {
        const remaining = vector.expected.remaining.filter(
          (row) => row.node === node
        );
        // eslint-disable-next-line no-continue
        if (remaining.length === 0) continue;
        const remainingMempool = {
          txs: new Map(
            [...vectorNodeMempool(vector, node).txs].filter(([tx]) =>
              remaining.some((row) => row.tx === vectorTxId(tx))
            )
          ),
        };
        const expired = remaining[0]!.tx;
        const plan = planMempoolExpiry(remainingMempool, {
          replacedAt,
          tx: vectorTxHash(expired),
        });
        const before = (await pool.query(historyQuery)).rows.length;
        t.is(
          await archiveMempoolTransaction({
            nodeInternalId: node,
            replacedAt,
            transactionInternalId: expired,
          }),
          1
        );
        const history = (
          await pool.query<{ node: number; tx: number; replacedAt: string }>(
            historyQuery
          )
        ).rows;
        t.is(history.length - before, plan.length, `seed ${seed} expiry`);
        t.deepEqual(
          history
            .filter(
              (row) =>
                row.node === node && row.replacedAt === '2026-02-01 00:00:00'
            )
            .map((row) => row.tx),
          plan.map((archive) => vectorTxId(archive.tx)).sort((a, b) => a - b),
          `seed ${seed} expiry`
        );
      }
      await pool.query(
        'ROLLBACK TO SAVEPOINT random_vector; RELEASE SAVEPOINT random_vector;'
      );
    }
  }
);
/* eslint-enable no-await-in-loop */
