#!/usr/bin/env node
/**
 * The spend index against the join (docs/clickhouse-port/spend-index.md): the
 * checker's `spendIndexCheck` on one node-agnostic snapshot. Pairs with both
 * commits at or below the spend watermark must be in the index exactly once,
 * with the joined output's attributes; nothing else may be. Also prints the
 * lag (visible(0) vs the spend watermark).
 *
 *   node ch1-chipnet-spend-check.mjs <agent dir> <clickhouse url> <database>
 *
 * Prints one JSON object; exit 0 if consistent, 1 if not. Credentials: CH_USER / CH_PASSWORD (env only).
 */
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [agentDirectory, url, database] = process.argv.slice(2);
if (database === undefined) {
  console.error('usage: ch1-chipnet-spend-check.mjs <agent dir> <clickhouse url> <database>');
  process.exit(2);
}
const load = (file) =>
  import(pathToFileURL(join(agentDirectory, 'build/store/clickhouse', file)).href);
const [clientModule, checkerModule] = await Promise.all([load('client.js'), load('checker.js')]);
const client = new clientModule.ClickHouseClient({
  database,
  password: process.env.CH_PASSWORD ?? '',
  requestTimeoutMs: 600_000,
  url,
  username: process.env.CH_USER ?? '',
});
try {
  const started = Date.now();
  const report = await new checkerModule.ClickHouseChecker(client, database).spendIndexCheck();
  const consistent =
    report.missing.length === 0 &&
    report.extra.length === 0 &&
    report.duplicated.length === 0 &&
    report.mismatched.length === 0 &&
    report.indexRows === report.joinRows;
  console.log(
    JSON.stringify(
      {
        ...report,
        consistent,
        database,
        ms: Date.now() - started,
        visible0: report.visible0.toString(),
        watermark: report.watermark.toString(),
      },
      null,
      2
    )
  );
  process.exitCode = consistent ? 0 : 1;
} finally {
  await client.close();
}
