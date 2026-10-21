import { readFileSync } from 'fs';

import type { TestFn } from 'ava';
import baseTest from 'ava';
import pg from 'pg';

// cspell:words attname attnum attrelid lpad stxkeys stxkind stxname stxrelid stxstattarget vals
const test = baseTest as TestFn<{ client: pg.Client }>;
const host = process.env.CHAINGRAPH_E2E_POSTGRES_HOST ?? 'localhost';
const port = process.env.CHAINGRAPH_E2E_POSTGRES_PORT ?? '5432';
const migrationDirectory = new URL(
  '../../images/hasura/hasura-data/migrations/default/1791100003000_output_category_capability_statistics/',
  import.meta.url
);
const readMigration = (direction: 'down' | 'up') =>
  readFileSync(new URL(`${direction}.sql`, migrationDirectory), 'utf8');

const statisticsQuery = /* sql */ `
SELECT s.stxkind::text AS kinds, s.stxstattarget::int AS target,
  array_agg(a.attname::text ORDER BY a.attnum) AS columns
FROM pg_statistic_ext s
JOIN pg_attribute a ON a.attrelid = s.stxrelid AND a.attnum = ANY (s.stxkeys)
WHERE s.stxname = 'output_category_capability_stats'
  AND s.stxrelid = 'output'::regclass
GROUP BY s.stxkind, s.stxstattarget;`;

test.before(async (t) => {
  t.context.client = new pg.Client({
    connectionString: `postgres://chaingraph:very_insecure_postgres_password@${host}:${port}/postgres`,
  });
  await t.context.client.connect();
  /**
   * Minimal stand-in for the output table inside a transaction-scoped schema;
   * everything is rolled back in `after.always`.
   */
  await t.context.client.query(/* sql */ `
BEGIN;
SET LOCAL statement_timeout = '10s';
CREATE SCHEMA output_statistics_test;
SET LOCAL search_path = output_statistics_test;
CREATE TYPE enum_nonfungible_token_capability AS ENUM ('none', 'mutable', 'minting');
CREATE TABLE output (transaction_hash bytea, output_index bigint,
  token_category bytea,
  nonfungible_token_capability enum_nonfungible_token_capability);
`);
});

test.after.always(async (t) => {
  await t.context.client.query('ROLLBACK;');
  await t.context.client.end();
});

test.serial(
  '[e2e] output_category_capability_statistics: up creates the statistics object',
  async (t) => {
    const { client } = t.context;
    await client.query(readMigration('up'));
    // re-applying is a no-op thanks to IF NOT EXISTS
    await client.query(readMigration('up'));
    const created = await client.query<{
      columns: string[];
      kinds: string;
      target: number;
    }>(statisticsQuery);
    t.deepEqual(created.rows, [
      {
        columns: ['token_category', 'nonfungible_token_capability'],
        kinds: '{d,f,m}',
        target: 10000,
      },
    ]);
  }
);

test.serial(
  '[e2e] output_category_capability_statistics: ANALYZE builds the MCV list',
  async (t) => {
    const { client } = t.context;
    await client.query(/* sql */ `
INSERT INTO output
SELECT decode(lpad(to_hex(i), 64, '0'), 'hex'), 0,
  decode(lpad(to_hex(i % 3), 64, '0'), 'hex'),
  (ARRAY['none', 'mutable', 'minting'])[i % 3 + 1]::enum_nonfungible_token_capability
FROM generate_series(1, 300) i;
ANALYZE output;`);
    const analyzed = await client.query<{ hasMcv: boolean }>(/* sql */ `
SELECT most_common_vals IS NOT NULL AS "hasMcv" FROM pg_stats_ext
WHERE statistics_name = 'output_category_capability_stats'
  AND statistics_schemaname = 'output_statistics_test';`);
    t.deepEqual(analyzed.rows, [{ hasMcv: true }]);
  }
);

test.serial(
  '[e2e] output_category_capability_statistics: down removes the statistics object',
  async (t) => {
    const { client } = t.context;
    await client.query(readMigration('down'));
    const dropped = await client.query(statisticsQuery);
    t.is(dropped.rowCount, 0);
    // down is idempotent too
    await client.query(readMigration('down'));
    t.pass();
  }
);
