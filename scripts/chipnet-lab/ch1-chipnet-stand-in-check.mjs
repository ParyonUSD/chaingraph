#!/usr/bin/env node
/**
 * The stand-in check on a ClickHouse Chaingraph database
 * (docs/clickhouse-port/mempool-fill-fix.md), through the agent's own checker
 * (`ClickHouseChecker.standInCheck`, one node-agnostic snapshot): visible
 * non-coinbase inputs whose visible spent output differs in value, locking
 * bytecode or token fields (`mismatched`), and inputs with more than one
 * visible row (`duplicated`). Both must be empty.
 *
 *   node ch1-chipnet-stand-in-check.mjs <agent dir> <clickhouse url> <database> [limit]
 *
 * Prints one JSON object; exit 0 if clean, 1 if not. Uses the compiled store
 * of <agent dir> (build/store/clickhouse). Credentials: CH_USER / CH_PASSWORD
 * (env only).
 */
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [agentDirectory, url, database, limit = '100'] = process.argv.slice(2);
if (database === undefined) {
  console.error('usage: ch1-chipnet-stand-in-check.mjs <agent dir> <clickhouse url> <database> [limit]');
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
  const checker = new checkerModule.ClickHouseChecker(client, database, {});
  const result = await checker.standInCheck({ limit: Number(limit) });
  const clean = result.mismatched.length === 0 && result.duplicated.length === 0;
  console.log(JSON.stringify({ clean, database, ms: Date.now() - started, ...result }, null, 2));
  process.exitCode = clean ? 0 : 1;
} finally {
  await client.close();
}
