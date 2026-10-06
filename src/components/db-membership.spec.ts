/* eslint-disable functional/no-loop-statement, functional/no-let, functional/no-throw-statement, @typescript-eslint/no-magic-numbers */
import test from 'ava';
import type pg from 'pg';

import {
  boundedValueRows,
  runMembershipTransaction,
  sqlChunkTargetBytes,
} from './db-membership.js';

const fakePool = (onQuery?: (sql: string, parameters: unknown[]) => void) => {
  const events: { sql: string; parameters: unknown[] }[] = [];
  const releases: boolean[] = [];
  let connections = 0;
  const pool = {
    connect: async () => {
      connections += 1;
      return {
        query: async (sql: string, parameters: unknown[] = []) => {
          events.push({ parameters, sql });
          onQuery?.(sql, parameters);
          return {
            rows: sql.startsWith('SELECT internal_id')
              ? [{ internalId: 7 }, { internalId: 2 }]
              : [],
          };
        },
        release: (discard: boolean) => releases.push(discard),
      };
    },
  } as unknown as Pick<pg.Pool, 'connect'>;
  return { connectionCount: () => connections, events, pool, releases };
};

test('publication locks unique nodes in ascending order before writes and finishes before commit', async (t) => {
  const fake = fakePool();
  const result = await runMembershipTransaction(
    fake.pool,
    'incremental',
    [7, 2, 7],
    async (client, nodes) => {
      t.deepEqual(nodes, [2, 7]);
      await client.query('NORMALIZED WRITE');
      return 'published';
    }
  );
  t.is(result, 'published');
  t.deepEqual(
    fake.events.map((event) => event.sql),
    [
      'BEGIN ISOLATION LEVEL READ COMMITTED;',
      'SELECT output_membership.lock_node($1::integer);',
      'SELECT output_membership.lock_node($1::integer);',
      'SELECT output_membership.begin_membership_changes();',
      'NORMALIZED WRITE',
      'SELECT output_membership.finish_membership_changes($1::integer[]);',
      'COMMIT;',
    ]
  );
  t.deepEqual(
    fake.events.slice(1, 3).map((event) => event.parameters),
    [[2], [7]]
  );
  t.deepEqual(fake.releases, [false]);
});

for (const code of ['40001', '40P01']) {
  test(`publication retries the entire normalized write on ${code}`, async (t) => {
    let failures = 0;
    const fake = fakePool((sql) => {
      if (sql.includes('finish_membership_changes')) {
        const shouldFail = failures === 0;
        failures += 1;
        if (shouldFail) {
          throw Object.assign(new Error('retry publication'), { code });
        }
      }
    });
    let writes = 0;
    await runMembershipTransaction(
      fake.pool,
      'incremental',
      [2],
      async (client) => {
        writes += 1;
        await client.query('NORMALIZED WRITE');
      }
    );
    t.is(writes, 2);
    t.is(fake.connectionCount(), 2);
    t.is(fake.events.filter((event) => event.sql === 'ROLLBACK;').length, 1);
    t.is(fake.events.filter((event) => event.sql === 'COMMIT;').length, 1);
    t.deepEqual(fake.releases, [false, false]);
  });
}

test('publication retry is bounded and failures never commit', async (t) => {
  const fake = fakePool((sql) => {
    if (sql === 'NORMALIZED WRITE')
      throw Object.assign(new Error('deadlock'), { code: '40P01' });
  });
  await t.throwsAsync(
    runMembershipTransaction(fake.pool, 'incremental', [2], async (client) => {
      await client.query('NORMALIZED WRITE');
    })
  );
  t.is(fake.connectionCount(), 4);
  t.is(fake.events.filter((event) => event.sql === 'ROLLBACK;').length, 4);
  t.false(fake.events.some((event) => event.sql === 'COMMIT;'));
});

test('publication releases broken clients when rollback fails', async (t) => {
  const fake = fakePool((sql) => {
    if (sql === 'NORMALIZED WRITE' || sql === 'ROLLBACK;')
      throw new Error('connection failed');
  });
  await t.throwsAsync(
    runMembershipTransaction(fake.pool, 'incremental', [2], async (client) => {
      await client.query('NORMALIZED WRITE');
    })
  );
  t.deepEqual(fake.releases, [true]);
});

test('late body fences registration before taking a snapshot and locking the node universe', async (t) => {
  const fake = fakePool();
  await runMembershipTransaction(
    fake.pool,
    'incremental',
    'all',
    async (_, nodes) => {
      t.deepEqual(nodes, [2, 7]);
    }
  );
  t.is(fake.events[1]!.sql, 'LOCK TABLE node IN SHARE MODE;');
  t.true(fake.events[2]!.sql.startsWith('SELECT internal_id'));
  t.deepEqual(
    fake.events.slice(3, 5).map((event) => event.parameters),
    [[2], [7]]
  );
});

test('deferred writes opt out of collectors without invoking installed incremental APIs', async (t) => {
  const fake = fakePool();
  await runMembershipTransaction(
    fake.pool,
    'deferred',
    'all',
    async (client) => {
      await client.query('NORMALIZED WRITE');
    }
  );
  t.deepEqual(
    fake.events.map((event) => event.sql),
    [
      'BEGIN ISOLATION LEVEL READ COMMITTED;',
      "SET LOCAL output_membership.collect_changes = 'deferred';",
      'NORMALIZED WRITE',
      'COMMIT;',
    ]
  );
});

test('baseline writes require no membership SQL APIs', async (t) => {
  const fake = fakePool();
  await runMembershipTransaction(fake.pool, 'baseline', [2], async (client) => {
    await client.query('NORMALIZED WRITE');
  });
  t.deepEqual(
    fake.events.map((event) => event.sql),
    ['BEGIN ISOLATION LEVEL READ COMMITTED;', 'NORMALIZED WRITE', 'COMMIT;']
  );
});

test('32 MiB of bytea rows are consumed lazily in bounded chunks with small SQL', (t) => {
  const script = Buffer.alloc(32 * 1024);
  let consumed = 0;
  const rows = function* rows() {
    for (let index = 0; index < 1024; index += 1) {
      consumed += 1;
      yield [index, script];
    }
  };
  const chunks = boundedValueRows(rows());
  const next = chunks.next();
  if (next.done === true) {
    t.fail('Expected first chunk');
    return;
  }
  const first = next.value;
  t.true(consumed < 1024);
  t.true(first.values.length < 1024);
  let totalRows = first.parameters.length / 2;
  for (const chunk of chunks) {
    t.true(
      (chunk.parameters.length / 2) * script.length * 2 <= sqlChunkTargetBytes
    );
    t.true(chunk.values.length < 1024);
    totalRows += chunk.parameters.length / 2;
  }
  t.is(totalRows, 1024);
});

test('large single script stays a parameter and empty rows produce no SQL', (t) => {
  const buffer = Buffer.alloc(32 * 1024 * 1024);
  const result = [...boundedValueRows([[buffer]])];
  t.is(result.length, 1);
  t.is(result[0]!.values, '($1)');
  t.is(result[0]!.parameters[0], buffer);
  t.deepEqual([...boundedValueRows([])], []);
});

test('VALUES batching reserves prefix parameters and never exceeds protocol limits', (t) => {
  const result = [
    ...boundedValueRows(
      Array.from({ length: 70_000 }, () => [1]),
      3
    ),
  ];
  t.true(result.length >= 2);
  for (const chunk of result) t.true(chunk.parameters.length + 3 <= 60_000);
  t.true(result[0]!.values.startsWith('($4),($5)'));
  t.true(result[1]!.values.startsWith('($4),($5)'));
});

test('standalone VALUES binds bytea types inside tuples before PostgreSQL resolves unknown columns', (t) => {
  const bytes = Buffer.from('ff80', 'hex');
  const chunk = [...boundedValueRows([[bytes, 4]], 1, ['bytea', 'bigint'])][0]!;
  t.is(chunk.values, '($2::bytea,$3::bigint)');
  t.deepEqual(chunk.parameters, [bytes, 4]);
});
