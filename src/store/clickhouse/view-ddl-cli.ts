// cspell:ignore clickhouse
/**
 * `yarn clickhouse:views:generate`: rewrite `src/store/clickhouse/ddl/050_views.sql`
 * from visibility.ts's view definitions (view-ddl.ts). `--check` exits 1
 * when the committed file differs instead of writing it.
 */
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { viewsDdl } from './view-ddl.js';

const target = fileURLToPath(
  new URL('../../../src/store/clickhouse/ddl/050_views.sql', import.meta.url)
);
const generated = viewsDdl();
if (process.argv.includes('--check')) {
  const same = readFileSync(target, 'utf8') === generated;
  // eslint-disable-next-line no-console
  console.log(same ? `${target}: up to date` : `${target}: differs`);
  process.exitCode = same ? 0 : 1;
} else {
  writeFileSync(target, generated);
  // eslint-disable-next-line no-console
  console.log(`wrote ${target}`);
}
