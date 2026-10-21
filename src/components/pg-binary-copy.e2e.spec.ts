/* eslint-disable @typescript-eslint/no-magic-numbers */
import type { TestFn } from 'ava';
import baseTest from 'ava';
import pg from 'pg';

import { BinaryCopyWriter, copyFromBuffers } from './pg-binary-copy.js';

const test = baseTest as TestFn<{ client: pg.Client }>;
const host = process.env.CHAINGRAPH_E2E_POSTGRES_HOST ?? 'localhost';
const port = process.env.CHAINGRAPH_E2E_POSTGRES_PORT ?? '5432';

/*
 * Runs against a real server, using only temporary tables (no persistent
 * tables are created or modified).
 */
test.before(async (t) => {
  t.context.client = new pg.Client({
    connectionString: `postgres://chaingraph:very_insecure_postgres_password@${host}:${port}/postgres`,
  });
  await t.context.client.connect();
  await t.context.client.query(/* sql */ `
SET statement_timeout = '5s';
CREATE TEMP TABLE stage_a (id bigint NOT NULL);
CREATE TEMP TABLE stage_b (id bigint NOT NULL);
`);
});

test.after.always(async (t) => {
  await t.context.client.end();
});

const rows = (values: (number | null)[]) =>
  values
    .reduce(
      (writer, value) =>
        value === null
          ? writer.startRow(1).null()
          : writer.startRow(1).int8(value),
      new BinaryCopyWriter()
    )
    .finish();
const copyA = 'COPY pg_temp.stage_a (id) FROM STDIN WITH (FORMAT binary)';
const copyB = 'COPY pg_temp.stage_b (id) FROM STDIN WITH (FORMAT binary)';
const ids = async (client: pg.Client, table: string) =>
  (
    await client.query<{ id: string }>(
      `SELECT id FROM pg_temp.${table} ORDER BY id;`
    )
  ).rows.map((row) => Number(row.id));

test.serial(
  '[e2e] copyFromBuffers: a failed later COPY rolls back and the connection stays usable',
  async (t) => {
    const { client } = t.context;
    await client.query('BEGIN;');
    const error = await t.throwsAsync(
      copyFromBuffers(client, [
        { messages: rows([1, 2]), statement: copyA },
        { messages: rows([3, null]), statement: copyB },
        { messages: rows([5]), statement: copyA },
      ])
    );
    t.regex(error?.message ?? '', /null value/u);
    await client.query('ROLLBACK;');
    t.deepEqual(await ids(client, 'stage_a'), []);
    t.deepEqual(await ids(client, 'stage_b'), []);

    await client.query('BEGIN;');
    t.deepEqual(
      await copyFromBuffers(client, [
        { messages: rows([10, 11]), statement: copyA },
        { messages: rows([12]), statement: copyB },
      ]),
      [2, 1]
    );
    await client.query('COMMIT;');
    t.deepEqual(await ids(client, 'stage_a'), [10, 11]);
    t.deepEqual(await ids(client, 'stage_b'), [12]);
  }
);
