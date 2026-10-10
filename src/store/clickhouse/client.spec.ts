/* eslint-disable @typescript-eslint/no-magic-numbers, @typescript-eslint/naming-convention, functional/no-let, functional/no-throw-statement */
// cspell:ignore clickhouse dedup urlsecret retryable
import { inspect } from 'node:util';

import { ClickHouseError } from '@clickhouse/client';
import test from 'ava';

import {
  ClickHouseClient,
  clickHouseConfigFromEnv,
  isRetryable,
  quoteIdentifier,
  splitCredentials,
} from './client.js';
import { RowBinaryWriter } from './row-binary.js';
import { createScratchDatabase, e2eClickHouseUrl } from './test-support.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

test('clickHouseConfigFromEnv: reads CHAINGRAPH_CLICKHOUSE_*', (t) => {
  t.deepEqual(
    clickHouseConfigFromEnv({
      CHAINGRAPH_CLICKHOUSE_DATABASE: 'cg2',
      CHAINGRAPH_CLICKHOUSE_PASSWORD: 'secret',
      CHAINGRAPH_CLICKHOUSE_URL: 'http://localhost:18123',
      CHAINGRAPH_CLICKHOUSE_USER: 'writer',
    }),
    {
      database: 'cg2',
      password: 'secret',
      requestTimeoutMs: 300_000,
      url: 'http://localhost:18123',
      username: 'writer',
    }
  );
  t.is(
    clickHouseConfigFromEnv({ CHAINGRAPH_CLICKHOUSE_URL: 'http://h:1' })
      .database,
    'cg'
  );
  t.throws(() => clickHouseConfigFromEnv({}), {
    message: /CHAINGRAPH_CLICKHOUSE_URL/u,
  });
});

test('splitCredentials: moves URL credentials out of the URL', (t) => {
  const split = splitCredentials({
    database: 'cg',
    password: '',
    requestTimeoutMs: 1,
    url: 'https://alice:p%40ss@example.com:8443/',
    username: '',
  });
  t.is(split.url, 'https://example.com:8443');
  t.is(split.username, 'alice');
  t.is(split.password, 'p@ss');
  const explicit = splitCredentials({
    database: 'cg',
    password: 'explicit',
    requestTimeoutMs: 1,
    url: 'https://alice:urlpass@example.com',
    username: 'bob',
  });
  t.is(explicit.username, 'bob');
  t.is(explicit.password, 'explicit');
  t.is(
    splitCredentials({ ...explicit, url: 'http://h:1', username: '' }).username,
    'default'
  );
});

test('isRetryable: transport errors and transient server codes (209 socket timeout, 210, 3, 32, 202, 252), not statement errors', (t) => {
  t.true(
    isRetryable(
      Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' })
    )
  );
  t.true(
    isRetryable(
      new ClickHouseError({
        code: '209',
        message: 'Timeout exceeded while reading from socket (30000 ms).',
        type: 'SOCKET_TIMEOUT',
      })
    )
  );
  ['3', '32', '202', '210', '252'].forEach((code) => {
    t.true(isRetryable(new ClickHouseError({ code, message: code })), code);
  });
  t.false(
    isRetryable(new ClickHouseError({ code: '62', message: 'Syntax error' }))
  );
  t.false(
    isRetryable(new ClickHouseError({ code: '60', message: 'Unknown table' }))
  );
});

test('quoteIdentifier: accepts plain identifiers only', (t) => {
  t.is(quoteIdentifier('utxo_by_script'), '`utxo_by_script`');
  t.throws(() => quoteIdentifier('utxo; DROP TABLE x'));
  t.throws(() => quoteIdentifier('a`b'));
  t.throws(() => quoteIdentifier(''));
});

test('ClickHouseClient: never exposes the password', (t) => {
  const client = new ClickHouseClient({
    database: 'cg',
    password: 'hunter2',
    requestTimeoutMs: 1000,
    url: 'http://user:urlsecret@localhost:1',
    username: '',
  });
  const shown = [
    String(client),
    JSON.stringify(client),
    inspect(client),
    client.endpoint,
  ].join(' ');
  t.false(shown.includes('hunter2'));
  t.false(shown.includes('urlsecret'));
  t.true(shown.includes('localhost:1'));
});

e2e(
  '[e2e] ClickHouseClient: ping, bound parameters, 64-bit integers',
  async (t) => {
    const scratch = await createScratchDatabase('client');
    t.teardown(scratch.drop);
    const { client } = scratch;
    t.true(await client.ping());
    const hostile = "x'); DROP DATABASE cg; --";
    const rows = await client.query<{ text: string; big: string }>(
      'SELECT {text:String} AS text, {big:UInt64} AS big',
      { big: 2n ** 64n - 1n, text: hostile }
    );
    t.deepEqual(rows, [{ big: '18446744073709551615', text: hostile }]);
    await t.throwsAsync(client.query('SELECT definitely_not_a_function()'));
    t.false(
      await new ClickHouseClient({
        database: 'cg',
        password: '',
        requestTimeoutMs: 500,
        url: 'http://127.0.0.1:1',
        username: '',
      }).ping()
    );
  }
);

e2e(
  '[e2e] ClickHouseClient: insertRowBinary is deduplicated by token',
  async (t) => {
    const scratch = await createScratchDatabase('client_dedup');
    t.teardown(scratch.drop);
    const { client } = scratch;
    const columns = [
      'commit_seq',
      'reason',
      'writer_epoch',
      'voided_at',
    ] as const;
    const rows = new RowBinaryWriter()
      .uint64(7n)
      .string('first')
      .uint64(1)
      .dateTime64(Date.UTC(2026, 9, 9))
      .endRow()
      .uint64(8n)
      .string('second')
      .uint64(1)
      .dateTime64(Date.UTC(2026, 9, 9))
      .endRow()
      .finish();
    await client.insertRowBinary('commit_void', columns, rows, {
      deduplicationToken: 'test:commit_void:0',
    });
    // a retry of the same step (same token) is dropped
    await client.insertRowBinary('commit_void', columns, rows, {
      deduplicationToken: 'test:commit_void:0',
    });
    // the same token with different data is dropped too (token wins)
    await client.insertRowBinary(
      'commit_void',
      columns,
      new RowBinaryWriter()
        .uint64(9n)
        .string('other')
        .uint64(1)
        .dateTime64(0)
        .endRow()
        .finish(),
      { deduplicationToken: 'test:commit_void:0' }
    );
    // a new token inserts
    await client.insertRowBinary('commit_void', columns, rows, {
      deduplicationToken: 'test:commit_void:1',
    });
    const counted = await client.query<{ seq: string; c: string }>(
      'SELECT commit_seq AS seq, count() AS c FROM commit_void GROUP BY seq ORDER BY seq'
    );
    t.deepEqual(counted, [
      { c: '2', seq: '7' },
      { c: '2', seq: '8' },
    ]);
    await client.insertRowBinary('commit_void', columns, new Uint8Array(), {
      deduplicationToken: 'empty',
    });
    await t.throwsAsync(
      client.insertRowBinary('commit_void', columns, rows, {
        deduplicationToken: '',
      }),
      { message: /deduplication token/u }
    );
    await t.throwsAsync(
      client.insertRowBinary('commit_void; DROP TABLE x', columns, rows, {
        deduplicationToken: 't',
      }),
      { message: /Invalid ClickHouse identifier/u }
    );
  }
);

e2e(
  '[e2e] ClickHouseClient: a read is retried after a socket reset and a 209; a statement error is not',
  async (t) => {
    const url = e2eClickHouseUrl!;
    const failures = ['reset', '209'];
    let attempts = 0;
    const client = new ClickHouseClient({
      database: 'default',
      faultBeforeRequest: (request) => {
        if (request.kind !== 'query') return;
        attempts += 1;
        const failure = failures.shift();
        if (failure === 'reset') {
          throw Object.assign(new Error('socket hang up'), {
            code: 'ECONNRESET',
          });
        }
        if (failure === '209') {
          throw new ClickHouseError({
            code: '209',
            message: 'Timeout exceeded while reading from socket (test).',
            type: 'SOCKET_TIMEOUT',
          });
        }
      },
      password: process.env.CHAINGRAPH_E2E_CLICKHOUSE_PASSWORD ?? '',
      requestTimeoutMs: 30_000,
      url,
      username: process.env.CHAINGRAPH_E2E_CLICKHOUSE_USER ?? '',
    });
    t.teardown(async () => client.close());
    t.deepEqual(await client.query<{ one: number }>('SELECT 1 AS one'), [
      { one: 1 },
    ]);
    t.is(attempts, 3);
    attempts = 0;
    await t.throwsAsync(client.query('SELECT * FROM no_such_table_g1'), {
      instanceOf: ClickHouseError,
    });
    t.is(attempts, 1, 'a statement error is not retried');
  }
);
