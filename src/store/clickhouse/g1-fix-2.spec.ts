/* eslint-disable @typescript-eslint/no-magic-numbers, functional/no-let, camelcase, @typescript-eslint/naming-convention, functional/no-try-statement */
// cspell:ignore clickhouse unhex
/**
 * G1 fix pass 2 (docs/clickhouse-port/g1-fix-pass-2.md §2): the spent-output
 * lookup reads few rows per outpoint (output granularity 128, base table, not
 * a projection), chunks run concurrently, and the DDL CLI notices a table
 * whose settings differ from the DDL.
 */
import { createHash } from 'node:crypto';

import test from 'ava';

import {
  lookupStoredOutputs,
  mapChunksConcurrently,
  sortedOutpoints,
} from './block-commit.js';
import { checkTableSettings, ddlTableSettings } from './ddl-apply.js';
import { createScratchDatabase, e2eClickHouseUrl } from './test-support.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

test('mapChunksConcurrently: chunk order kept, at most `concurrency` in flight', async (t) => {
  let inFlight = 0;
  let peak = 0;
  const results = await mapChunksConcurrently(
    Array.from({ length: 23 }, (_, index) => index),
    5,
    2,
    async (chunk) => {
      inFlight += 1;
      peak = Math.max(peak, inFlight);
      await new Promise((resolve) => {
        setTimeout(resolve, 5 * (6 - chunk.length));
      });
      inFlight -= 1;
      return chunk[0];
    }
  );
  t.deepEqual(results, [0, 5, 10, 15, 20]);
  t.is(peak, 2);
  t.deepEqual(await mapChunksConcurrently([], 5, 2, async () => 1), []);
});

test('sortedOutpoints: distinct, in (hash, index) order', (t) => {
  t.deepEqual(
    sortedOutpoints([
      { hash: 'bb', index: 1 },
      { hash: 'aa', index: 2 },
      { hash: 'bb', index: 0 },
      { hash: 'aa', index: 2 },
    ]),
    [
      { hash: 'aa', index: 2 },
      { hash: 'bb', index: 0 },
      { hash: 'bb', index: 1 },
    ]
  );
});

test('the DDL pins output to granularity 128 with 4 KiB blocks', (t) => {
  const settings = ddlTableSettings();
  t.deepEqual(Object.fromEntries(settings.get('output')!), {
    index_granularity: '128',
    max_compress_block_size: '4096',
    min_compress_block_size: '4096',
  });
  t.is(settings.get('input')!.get('index_granularity'), '1024');
});

e2e(
  '[e2e] checkTableSettings: a table created with other settings is reported (IF NOT EXISTS keeps it)',
  async (t) => {
    const scratch = await createScratchDatabase('fix2_settings');
    try {
      const server = { url: e2eClickHouseUrl! };
      t.deepEqual(
        (await checkTableSettings(server, scratch.name)).mismatches,
        []
      );
      await scratch.client.command(`DROP TABLE input_at`);
      await scratch.client.command(`DROP TABLE input_v`);
      await scratch.client.command(`DROP TABLE input`);
      await scratch.client.command(
        `CREATE TABLE input (transaction_hash FixedString(32), input_index UInt32, commit_seq UInt64)
         ENGINE = MergeTree ORDER BY (transaction_hash, input_index) SETTINGS index_granularity = 8192`
      );
      t.deepEqual((await checkTableSettings(server, scratch.name)).mismatches, [
        {
          actual: '8192',
          expected: '1024',
          setting: 'index_granularity',
          table: 'input',
        },
      ]);
    } finally {
      await scratch.drop();
    }
  }
);

e2e(
  '[e2e] spent-output lookup: base table, about one 128-row granule per outpoint, all found',
  async (t) => {
    const scratch = await createScratchDatabase('fix2_lookup');
    try {
      const { client } = scratch;
      const transactions = 40_000;
      await client.command(
        `INSERT INTO output (transaction_hash, output_index, transaction_internal_id, value_satoshis,
           locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability,
           nonfungible_token_commitment, commit_seq)
         SELECT toFixedString(SHA256(toString(intDiv(number, 2))), 32), toUInt32(number % 2), intDiv(number, 2) + 1,
           toInt64(number), concat(unhex('76a914'), substring(SHA256(toString(number)), 1, 20), unhex('88ac')),
           toFixedString(repeat(unhex('00'), 32), 32), NULL, NULL, NULL, toUInt64(1099511627777)
         FROM numbers(${transactions * 2})`
      );
      await client.command('OPTIMIZE TABLE output FINAL');
      const sha256 = (text: string) =>
        createHash('sha256').update(text).digest('hex');
      const expected = new Map<string, bigint>();
      const outpoints = Array.from({ length: 300 }, (_, item) => {
        const tx = (item * 7919) % transactions;
        const index = item % 2;
        const hash = sha256(String(tx));
        expected.set(`${hash}:${index}`, BigInt(2 * tx + index));
        return { hash, index };
      });
      const tag = `fix2_lookup_${Date.now()}`;
      const tagged = new Proxy(client, {
        get: (target, property: keyof typeof client) =>
          property === 'query'
            ? async (
                sql: string,
                params: { [key: string]: unknown },
                settings: { [key: string]: unknown } = {}
              ) => target.query(sql, params, { ...settings, log_comment: tag })
            : target[property],
      });
      const found = await lookupStoredOutputs(
        { client: tagged, fence: () => [], lookupChunkSize: 100 },
        outpoints
      );
      t.is(found.length, 300);
      t.true(
        found.every(
          ({ output }) =>
            expected.get(`${output.transactionHash}:${output.outputIndex}`) ===
            output.valueSatoshis
        )
      );
      await client.command('SYSTEM FLUSH LOGS');
      const [log] = await client.query<{
        queries: string;
        read_rows: string;
        projections: number;
      }>(
        `SELECT count() AS queries, sum(read_rows) AS read_rows, sum(length(projections)) AS projections
         FROM system.query_log
         WHERE type = 'QueryFinish' AND log_comment = {tag:String} AND query LIKE '%FROM output%'`,
        { tag }
      );
      t.is(Number(log!.queries), 3);
      t.is(Number(log!.projections), 0);
      // 300 outpoints in one part: at most 2 granules of 128 rows each (plus the commit_void set)
      t.true(
        Number(log!.read_rows) <= 300 * 2 * 128,
        `read ${log!.read_rows} rows`
      );
    } finally {
      await scratch.drop();
    }
  }
);
