/* eslint-disable @typescript-eslint/no-magic-numbers, @typescript-eslint/naming-convention, functional/no-throw-statement, functional/no-let, no-promise-executor-return */
/**
 * Start-up warm-up (unit; fix-pass-11.md §5): entries run in order and per
 * round, failures are counted and logged without throwing, the deadline
 * skips the rest, and a malformed file is refused.
 */
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import test from 'ava';

import { loadWarmupEntries, runWarmup } from './warmup.js';

const response = (body: unknown) =>
  new Response(JSON.stringify(body), {
    headers: { 'content-type': 'application/json' },
  });

test('runWarmup: every entry per round, in order; errors counted, never thrown', async (t) => {
  const seen: string[] = [];
  const logged: string[] = [];
  const result = await runWarmup(
    async (_url, init) => {
      const body = JSON.parse(String(init.body)) as {
        query?: string;
        documentId?: string;
      };
      const name = body.query ?? body.documentId ?? '';
      seen.push(name);
      if (name === 'boom') throw new Error('socket hang up');
      return response(
        name === 'bad' ? { errors: [{ message: 'no' }] } : { data: {} }
      );
    },
    [{ query: 'a' }, { documentId: 'bad' }, { query: 'boom' }],
    { log: (line) => logged.push(line), rounds: 2 }
  );
  t.deepEqual(seen, ['a', 'bad', 'boom', 'a', 'bad', 'boom']);
  t.like(result, { entries: 3, failed: 4, ok: 2, skipped: 0 });
  t.is(logged.length, 4);
});

test('runWarmup: entries after the deadline are skipped', async (t) => {
  let calls = 0;
  const result = await runWarmup(
    async () => {
      calls += 1;
      await new Promise((resolve) => setTimeout(resolve, 30));
      return response({ data: {} });
    },
    [{ query: 'a' }, { query: 'b' }, { query: 'c' }],
    { timeoutMs: 10 }
  );
  t.is(calls, 1);
  t.like(result, { failed: 0, ok: 1, skipped: 2 });
});

test('loadWarmupEntries: an array of requests with a query or a documentId', (t) => {
  const dir = mkdtempSync(join(tmpdir(), 'warmup-'));
  const good = join(dir, 'good.json');
  writeFileSync(
    good,
    JSON.stringify([
      { query: '{ a }', variables: { n: 1 } },
      { documentId: 'abc' },
    ])
  );
  t.is(loadWarmupEntries(good).length, 2);
  const notArray = join(dir, 'object.json');
  writeFileSync(notArray, JSON.stringify({ query: '{ a }' }));
  t.throws(() => loadWarmupEntries(notArray), { message: /JSON array/u });
  const noQuery = join(dir, 'empty.json');
  writeFileSync(noQuery, JSON.stringify([{ variables: {} }]));
  t.throws(() => loadWarmupEntries(noQuery), { message: /entry 0/u });
});
