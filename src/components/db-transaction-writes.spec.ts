// cspell:ignore intrablock tgname tgrelid tgisinternal unnest ctid
/* eslint-disable functional/no-loop-statement, no-await-in-loop, functional/no-try-statement, @typescript-eslint/no-magic-numbers */
import { readFileSync } from 'fs';

import test from 'ava';
import pg from 'pg';

import type { ChaingraphTransaction } from '../types/chaingraph.js';

import {
  type QueryParameter,
  runMembershipTransaction,
} from './db-membership.js';
import { insertTransactions } from './db-transaction-writes.js';

const transaction = (id: number, scripts = ['51']): ChaingraphTransaction => ({
  hash: id.toString(16).padStart(64, '0'),
  inputs: [],
  isCoinbase: false,
  locktime: 0,
  outputs: scripts.map((lockingBytecode) => ({
    lockingBytecode,
    valueSatoshis: 1n,
  })),
  sizeBytes: 100,
  version: 1,
});

const captureInserts = (
  transactions: ChaingraphTransaction[],
  replay = false
) => {
  const queries: { parameters: QueryParameter[]; sql: string }[] = [];
  const client = {
    query: async (sql: string, parameters: QueryParameter[]) => {
      queries.push({ parameters, sql });
      return {
        rows:
          sql.startsWith('INSERT INTO transaction') && !replay
            ? transactions.map((tx, index) => ({
                hash: Buffer.from(tx.hash, 'hex'),
                internalId: String(index + 1),
              }))
            : [],
      };
    },
  } as unknown as pg.PoolClient;
  return { client, queries };
};

test('new output initialization uses unique sorted acceptance IDs and excludes OP_RETURN from unspent', async (t) => {
  const tx = transaction(1, ['51', '6A', '']);
  const capture = captureInserts([tx]);
  const incoming = [2, 1, 2];
  await insertTransactions(capture.client, [tx], incoming);
  const output = capture.queries.find((query) =>
    query.sql.startsWith('INSERT INTO output')
  )!;
  t.true(output.sql.includes('accepted_node_ids, unspent_node_ids'));
  t.deepEqual(output.parameters.slice(8, 10), [
    [1, 2],
    [1, 2],
  ]);
  t.deepEqual(output.parameters.slice(18, 20), [[1, 2], []]);
  t.deepEqual(output.parameters.slice(28, 30), [
    [1, 2],
    [1, 2],
  ]);
  t.deepEqual(incoming, [2, 1, 2]);
});

test('baseline and deferred output inserts retain original columns and parameters', async (t) => {
  const tx = transaction(1);
  const capture = captureInserts([tx]);
  await insertTransactions(capture.client, [tx]);
  const output = capture.queries.find((query) =>
    query.sql.startsWith('INSERT INTO output')
  )!;
  t.false(output.sql.includes('accepted_node_ids'));
  t.is(output.parameters.length, 8);
});

test('transaction replay never initializes or overwrites existing outputs', async (t) => {
  const tx = transaction(1);
  const capture = captureInserts([tx], true);
  const saved = await insertTransactions(capture.client, [tx], [2]);
  t.is(saved.size, 0);
  t.is(capture.queries.length, 1);
});

test('initialized output batches keep array parameters separate and stay below protocol limits', async (t) => {
  const tx = transaction(
    1,
    Array.from({ length: 20_000 }, () => '51')
  );
  const capture = captureInserts([tx]);
  await insertTransactions(capture.client, [tx], [2, 1]);
  const outputs = capture.queries.filter((query) =>
    query.sql.startsWith('INSERT INTO output')
  );
  t.true(outputs.length > 1);
  t.is(
    outputs.reduce((count, query) => count + query.parameters.length / 10, 0),
    20_000
  );
  for (const output of outputs) {
    t.true(output.parameters.length <= 60_000);
    t.true(output.sql.length < 2_097_152);
    t.deepEqual(output.parameters.slice(8, 10), [
      [1, 2],
      [1, 2],
    ]);
  }
});

// Explicit local socket only; the ordinary unit suite never connects to a DB.
const socket = process.env.CHAINGRAPH_INSERT_TEST_SOCKET;
const localTest = socket === undefined ? test.serial.skip : test.serial;
localTest(
  '[e2e] initialized arrays are corrected for latent children, another node, and intrablock spends',
  async (t) => {
    const pool = new pg.Pool({
      database: 'postgres',
      host: socket,
      max: 2,
      port: 55489,
      user: 'membership_insert_test',
    });
    const client = await pool.connect();
    const root = new URL('../../', import.meta.url);
    const sql = (relative: string) =>
      readFileSync(new URL(relative, root), 'utf8');
    try {
      for (const migration of [
        '1616195337538_init',
        '1673124945608_tokens',
        '1790852400000_add_output_node_membership',
        '1791280000000_defer_output_membership_maintenance',
      ]) {
        await client.query(
          sql(
            `images/hasura/hasura-data/migrations/default/${migration}/up.sql`
          )
        );
      }
      await client.query(`DO $$ DECLARE t record; BEGIN
      FOR t IN SELECT c.oid::regclass AS relation, tg.tgname FROM pg_trigger tg
        JOIN pg_class c ON c.oid=tg.tgrelid WHERE NOT tg.tgisinternal
          AND tg.tgname <> 'trigger_output_membership_reject_truncate'
      LOOP EXECUTE format('ALTER TABLE %s DISABLE TRIGGER %I',t.relation,t.tgname); END LOOP; END $$;
      CREATE INDEX spent_by_index ON input(outpoint_transaction_hash,outpoint_index);
      CREATE INDEX block_inclusions_index ON block_transaction(transaction_internal_id);
      INSERT INTO node(internal_id,name,protocol_version,user_agent) VALUES(1,'node-1',1,'test'),(2,'node-2',1,'test');`);
      await client.query(sql('scripts/output-node-membership/incremental.sql'));
      const publish = async (
        transactions: ChaingraphTransaction[],
        accepted: number[]
      ) =>
        runMembershipTransaction(
          pool,
          'incremental',
          'all',
          async (writer, nodes) => {
            const saved = await insertTransactions(
              writer,
              transactions,
              accepted
            );
            const hashes = transactions.map((tx) =>
              Buffer.from(tx.hash, 'hex')
            );
            await writer.query(
              `INSERT INTO node_transaction(node_internal_id,transaction_internal_id,validated_at)
        SELECT n,tx.internal_id,now() FROM unnest($1::integer[]) n CROSS JOIN transaction tx
          WHERE tx.hash=ANY($2::bytea[]) ON CONFLICT DO NOTHING;`,
              [accepted, hashes]
            );
            await writer.query(
              `SELECT output_membership.note_membership_changes(n,
        ARRAY(SELECT internal_id FROM transaction WHERE hash=ANY($2::bytea[])))
        FROM unnest($1::integer[]) n;`,
              [nodes, hashes]
            );
            const tupleIds = await writer.query<{ tupleId: string }>(
              'SELECT ctid::text AS "tupleId" FROM output WHERE transaction_hash=ANY($1::bytea[]) ORDER BY transaction_hash,output_index;',
              [hashes]
            );
            return { saved, tupleIds: tupleIds.rows };
          }
        );
      const seed = transaction(100);
      const initialized = await publish([seed], [2, 1]);
      const afterFinish = await client.query<{ tupleId: string }>(
        'SELECT ctid::text AS "tupleId" FROM output WHERE transaction_hash=$1 ORDER BY output_index;',
        [Buffer.from(seed.hash, 'hex')]
      );
      t.deepEqual(afterFinish.rows, initialized.tupleIds);
      const parent = transaction(101);
      const child = transaction(102);
      child.inputs.push({
        outpointIndex: 0,
        outpointTransactionHash: parent.hash,
        sequenceNumber: 0,
        unlockingBytecode: '',
      });
      await publish([child], [1, 2]);
      await publish([parent], [1, 1]);
      const arrays = async (tx: ChaingraphTransaction) =>
        (
          await client.query<{ accepted: number[]; unspent: number[] }>(
            'SELECT accepted_node_ids AS accepted, unspent_node_ids AS unspent FROM output WHERE transaction_hash=$1 AND output_index=0;',
            [Buffer.from(tx.hash, 'hex')]
          )
        ).rows[0]!;
      t.deepEqual(await arrays(parent), { accepted: [1], unspent: [] });
      await publish([parent], [2]);
      t.deepEqual(await arrays(parent), { accepted: [1, 2], unspent: [] });
      t.deepEqual(await arrays(child), { accepted: [1, 2], unspent: [1, 2] });
      const intrablockParent = transaction(103);
      const intrablockChild = transaction(104);
      intrablockChild.inputs.push({
        outpointIndex: 0,
        outpointTransactionHash: intrablockParent.hash,
        sequenceNumber: 0,
        unlockingBytecode: '',
      });
      await publish([intrablockParent, intrablockChild], [2, 1, 2]);
      t.deepEqual(await arrays(intrablockParent), {
        accepted: [1, 2],
        unspent: [],
      });
      t.deepEqual(await arrays(intrablockChild), {
        accepted: [1, 2],
        unspent: [1, 2],
      });
      /*
       * A second node's existing block acceptance adds support not present in
       * the initializer of a late transaction body.
       */
      const lateBody = transaction(105);
      await client.query(
        `INSERT INTO block(internal_id,height,version,timestamp,hash,previous_block_hash,merkle_root,bits,nonce,size_bytes)
      VALUES(1,0,1,0,$1,$1,$1,0,0,100);`,
        [Buffer.from(transaction(999).hash, 'hex')]
      );
      await client.query('INSERT INTO node_block VALUES(2,1,now());');
      await runMembershipTransaction(
        pool,
        'incremental',
        'all',
        async (writer, nodes) => {
          const saved = await insertTransactions(writer, [lateBody], [1]);
          const id = [...saved.values()][0]!;
          await writer.query('INSERT INTO block_transaction VALUES(1,$1,0);', [
            id,
          ]);
          await writer.query(
            'INSERT INTO node_transaction VALUES(1,$1,now());',
            [id]
          );
          await writer.query(
            'SELECT output_membership.note_membership_changes(n,$2::bigint[]) FROM unnest($1::integer[]) n;',
            [nodes, [id]]
          );
        }
      );
      t.deepEqual(await arrays(lateBody), {
        accepted: [1, 2],
        unspent: [1, 2],
      });
    } finally {
      client.release();
      await pool.end();
    }
  }
);
