// cspell:ignore clickhouse
/**
 * R23: the committed SDL equals the schema. A failure means the schema
 * changed without the committed file: review the diff (breaking changes need
 * release notes), then `yarn api:schema:update`.
 */
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import test from 'ava';

import { schemaSdl } from './sdl.js';

const committed = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../docs/clickhouse-port/v2-schema.graphql'
);

test('R23: the committed v2 SDL (docs/clickhouse-port/v2-schema.graphql) equals the schema', (t) => {
  const expected = readFileSync(committed, 'utf8');
  const actual = schemaSdl();
  if (actual !== expected) {
    const a = actual.split('\n');
    const e = expected.split('\n');
    const line = a.findIndex((text, index) => text !== e[index]);
    t.fail(
      `The schema changed (first difference at line ${line + 1}: committed "${
        e[line] ?? ''
      }", now "${
        a[line] ?? ''
      }"). Review it, then run yarn api:schema:update to commit the new SDL.`
    );
    return;
  }
  t.pass();
});
