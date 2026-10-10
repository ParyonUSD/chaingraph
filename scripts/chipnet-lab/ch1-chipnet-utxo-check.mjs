#!/usr/bin/env node
/**
 * UTXO-on database: the stored per-node UTXO set (`utxo_at`) equals the
 * Chaingraph v1 definition computed at query time (outputs of transactions the
 * node accepts, not spent by an input of a transaction it accepts; the
 * checker's `utxo: 'off'` path, docs/clickhouse-port/utxo-off.md). Run with
 * the agent stopped (each side takes its own snapshot).
 *
 *   node ch1-chipnet-utxo-check.mjs <agent dir> <clickhouse url> <database> <node name>
 *
 * Prints one JSON object; exit 0 if equal, 1 if not. Credentials: CH_USER / CH_PASSWORD (env only).
 */
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [agentDirectory, url, database, nodeName] = process.argv.slice(2);
if (nodeName === undefined) {
  console.error('usage: ch1-chipnet-utxo-check.mjs <agent dir> <clickhouse url> <database> <node name>');
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
const key = (row) => `${row.transactionHash}:${row.outputIndex}`;
try {
  const started = Date.now();
  const stored = (await new checkerModule.ClickHouseChecker(client, database, { utxo: 'on' }).unspent(nodeName, {})).map(key);
  const v1 = (await new checkerModule.ClickHouseChecker(client, database, { utxo: 'off' }).unspent(nodeName, {})).map(key);
  const storedSet = new Set(stored);
  const v1Set = new Set(v1);
  const onlyStored = stored.filter((outpoint) => !v1Set.has(outpoint));
  const onlyV1 = v1.filter((outpoint) => !storedSet.has(outpoint));
  const equal = onlyStored.length === 0 && onlyV1.length === 0;
  console.log(
    JSON.stringify(
      { database, equal, ms: Date.now() - started, node: nodeName, onlyStored: onlyStored.slice(0, 20), onlyV1: onlyV1.slice(0, 20), stored: stored.length, v1: v1.length },
      null,
      2
    )
  );
  process.exitCode = equal ? 0 : 1;
} finally {
  await client.close();
}
