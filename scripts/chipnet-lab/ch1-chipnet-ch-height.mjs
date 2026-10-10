#!/usr/bin/env node
/**
 * Visible height of one node in a ClickHouse Chaingraph database, through the
 * agent's own pinned view (`node_block_at` on one gated snapshot).
 *
 *   node ch1-chipnet-ch-height.mjs <agent dir> <clickhouse url> <database> <node name>
 *
 * Prints "<max height>\t<accepted block count>" ("-1\t0" if the node or its
 * blocks are not there yet). Uses the compiled store of <agent dir>
 * (build/store/clickhouse). Credentials: CH_USER / CH_PASSWORD (env only).
 */
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [agentDirectory, url, database, nodeName] = process.argv.slice(2);
if (nodeName === undefined) {
  console.error('usage: ch1-chipnet-ch-height.mjs <agent dir> <clickhouse url> <database> <node name>');
  process.exit(2);
}
const load = (file) =>
  import(pathToFileURL(join(agentDirectory, 'build/store/clickhouse', file)).href);
const [clientModule, checkerModule, visibility] = await Promise.all([
  load('client.js'),
  load('checker.js'),
  load('visibility.js'),
]);
const client = new clientModule.ClickHouseClient({
  database,
  password: process.env.CH_PASSWORD ?? '',
  requestTimeoutMs: 120_000,
  url,
  username: process.env.CH_USER ?? '',
});
try {
  const checker = checkerModule.createClickHouseChecker(client, database);
  const nodeId = await checker.nodeInternalId(nodeName);
  if (nodeId === undefined) {
    console.log('-1\t0');
  } else {
    const snapshot = await visibility.readSnapshot(client, nodeId);
    const [row] = await client.query(
      `SELECT toString(if(count() = 0, -1, toInt64(max(height)))) AS h, toString(count()) AS c FROM ${visibility.pinnedView('node_block_at')}`,
      visibility.nodeViewParams(snapshot)
    );
    console.log(`${row.h}\t${row.c}`);
  }
} finally {
  await client.close();
}
