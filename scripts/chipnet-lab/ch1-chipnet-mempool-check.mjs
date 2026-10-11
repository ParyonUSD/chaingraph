#!/usr/bin/env node
/**
 * Compare one node's mempool as the store records it (`node_transaction`)
 * with the node's own mempool (`getrawmempool` over RPC), for the mempool
 * fetch check (docs/clickhouse-port/pre-phase2-concurrency-mempool.md §2).
 *
 *   node ch1-chipnet-mempool-check.mjs <agent dir> pg|ch <store url> <database> <node name>
 *
 * Node RPC: CHIPNET_NODE_RPC_URL, credentials CHIPNET_RPC_USER /
 * CHIPNET_RPC_PASSWORD (environment only; never printed). ClickHouse
 * credentials: CH_USER / CH_PASSWORD. Uses the compiled checker of
 * <agent dir>. Prints JSON: counts, the hashes only the node has and only the
 * store has. Exit code 1 if they differ.
 *
 * The node's mempool moves (new announcements, a block): run it twice a few
 * seconds apart and read a difference against both.
 */
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [agentDirectory, store, url, database, nodeName] = process.argv.slice(2);
if (nodeName === undefined || !['pg', 'ch'].includes(store)) {
  console.error(
    'usage: ch1-chipnet-mempool-check.mjs <agent dir> pg|ch <store url> <database> <node name>'
  );
  process.exit(2);
}
const rpcUrl = process.env.CHIPNET_NODE_RPC_URL ?? 'http://127.0.0.1:48332/';
const rpc = async (method, params = []) => {
  const response = await fetch(rpcUrl, {
    body: JSON.stringify({
      id: 'ch1-mempool-check',
      jsonrpc: '1.0',
      method,
      params,
    }),
    headers: {
      authorization: `Basic ${Buffer.from(
        `${process.env.CHIPNET_RPC_USER ?? ''}:${
          process.env.CHIPNET_RPC_PASSWORD ?? ''
        }`
      ).toString('base64')}`,
      'content-type': 'text/plain',
    },
    method: 'POST',
  });
  const body = await response.json();
  if (body.error) throw new Error(`${method}: ${JSON.stringify(body.error)}`);
  return body.result;
};

const load = (file) =>
  import(pathToFileURL(join(agentDirectory, 'build', file)).href);

const storeMempool = async () => {
  if (store === 'ch') {
    const [clientModule, checkerModule] = await Promise.all([
      load('store/clickhouse/client.js'),
      load('store/clickhouse/checker.js'),
    ]);
    const client = new clientModule.ClickHouseClient({
      database,
      password: process.env.CH_PASSWORD ?? '',
      requestTimeoutMs: 120_000,
      url,
      username: process.env.CH_USER ?? '',
    });
    try {
      return await checkerModule
        .createClickHouseChecker(client, database)
        .mempool(nodeName);
    } finally {
      await client.close();
    }
  }
  const { default: pg } = await import(
    pathToFileURL(join(agentDirectory, 'node_modules/pg/lib/index.js')).href
  );
  const client = new pg.Client({ connectionString: `${url}/${database}` });
  await client.connect();
  try {
    const { rows } = await client.query(
      `SELECT encode(t.hash, 'hex') AS hash, nt.validated_at AS "validatedAt"
         FROM node_transaction nt
         JOIN node n ON n.internal_id = nt.node_internal_id
         JOIN transaction t ON t.internal_id = nt.transaction_internal_id
        WHERE n.name = $1`,
      [nodeName]
    );
    return rows;
  } finally {
    await client.end();
  }
};

const [nodeHashes, entries] = await Promise.all([
  rpc('getrawmempool'),
  storeMempool(),
]);
const nodeSet = new Set(nodeHashes);
const storeSet = new Set(entries.map((entry) => entry.hash));
const onlyNode = [...nodeSet].filter((hash) => !storeSet.has(hash)).sort();
const onlyStore = [...storeSet].filter((hash) => !nodeSet.has(hash)).sort();
console.log(
  JSON.stringify(
    {
      at: new Date().toISOString(),
      node: nodeName,
      nodeMempool: nodeSet.size,
      onlyNode,
      onlyStore,
      store,
      storeMempool: storeSet.size,
    },
    null,
    1
  )
);
process.exit(onlyNode.length === 0 && onlyStore.length === 0 ? 0 : 1);
