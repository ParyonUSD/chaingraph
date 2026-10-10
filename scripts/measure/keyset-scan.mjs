#!/usr/bin/env node
/**
 * WP6 keyset scanner (plan risk R7): id-ordered keyset pagination through the gated views.
 *
 *   node scripts/measure/keyset-scan.mjs --agent-dir <built checkout> [--ch-url http://localhost:18123]
 *     [--database chaingraph_ingestion_gate] [--page 1000] --out <file.json>
 *
 * With ONE pinned snapshot (node-agnostic: visible0 + committed tail), pages
 *   - transaction_at by internal_id                         (… WHERE internal_id > {last} ORDER BY internal_id LIMIT page)
 *   - output_at by (transaction_internal_id, output_index)  (tuple keyset)
 * to the end, then compares the paged keys with one full aggregate over the same snapshot:
 * count, uniqExact and an order-independent fingerprint (sum and xor of cityHash64(key)). Any gap or
 * duplicate shows up as a mismatch. Also reports, per commit, whether its transaction ids interleave
 * with an earlier commit's (ids are allocated before commit, so commit order != id order): the number
 * of commits whose min id is below an earlier-committed commit's max id. A cursor that is NOT pinned
 * to one snapshot can skip such rows (they become visible behind the cursor).
 */
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';

const { values: options } = parseArgs({
  options: {
    'agent-dir': { type: 'string' },
    'ch-url': { default: 'http://localhost:18123', type: 'string' },
    database: { default: 'chaingraph_ingestion_gate', type: 'string' },
    out: { type: 'string' },
    page: { default: '1000', type: 'string' },
  },
});
if (options['agent-dir'] === undefined || options.out === undefined) {
  console.error('usage: keyset-scan.mjs --agent-dir <dir> --out <file> [--ch-url] [--database] [--page]');
  process.exit(2);
}
const load = (path) => import(pathToFileURL(join(options['agent-dir'], 'build/store/clickhouse', path)).href);
const [{ ClickHouseClient }, visibility] = await Promise.all([load('client.js'), load('visibility.js')]);
const client = new ClickHouseClient({ database: options.database, password: '', requestTimeoutMs: 120_000, url: options['ch-url'], username: '' });
const pageSize = Number(options.page);
const snapshot = await visibility.readSnapshot(client, visibility.nodeAgnosticId);
const params = visibility.agnosticViewParams(snapshot);
// WP6b builds export pinnedView (views take fence/void too); older builds take the WP4 arguments.
const view = (name) =>
  visibility.pinnedView?.(`${name}_at`) ?? `${name}_at(visible0 = {visible0:UInt64}, tail = {tail:Array(UInt64)})`;

const mask64 = (1n << 64n) - 1n;
const scan = async ({ name, keyColumns, pageQuery, fullQuery }) => {
  const started = Date.now();
  const seen = new Set();
  let duplicates = 0;
  let outOfOrder = 0;
  let pages = 0;
  let rows = 0;
  let sum = 0n;
  let xor = 0n;
  let last;
  for (;;) {
    const page = await client.query(pageQuery(last), { ...params, ...(last ?? {}), page: pageSize });
    pages += 1;
    for (const row of page) {
      const key = keyColumns.map((column) => row[column]).join(':');
      if (seen.has(key)) duplicates += 1;
      seen.add(key);
      const current = keyColumns.map((column) => BigInt(row[column]));
      if (last !== undefined) {
        const previous = keyColumns.map((column) => BigInt(last[column]));
        const greater = current[0] > previous[0] || (current[0] === previous[0] && (current[1] ?? 0n) > (previous[1] ?? 0n));
        if (!greater) outOfOrder += 1;
      }
      last = Object.fromEntries(keyColumns.map((column) => [column, row[column]]));
      sum = (sum + BigInt(row.h)) & mask64;
      xor ^= BigInt(row.h);
      rows += 1;
    }
    if (page.length < pageSize) break;
  }
  const [full] = await client.query(fullQuery, params);
  const result = {
    duplicates,
    full: { count: Number(full.c), unique: Number(full.u), fingerprintSum: full.s, fingerprintXor: full.x },
    name,
    outOfOrder,
    paged: { fingerprintSum: sum.toString(), fingerprintXor: xor.toString(), pages, rows, unique: seen.size },
    seconds: (Date.now() - started) / 1000,
  };
  result.ok = duplicates === 0 && outOfOrder === 0 && result.full.count === rows && result.full.unique === seen.size && full.s === sum.toString() && full.x === xor.toString();
  console.log(`${name}: ${rows} rows in ${pages} pages, duplicates ${duplicates}, out-of-order ${outOfOrder}, full count ${result.full.count}, fingerprints ${result.ok ? 'match' : 'MISMATCH'} (${result.seconds.toFixed(1)} s)`);
  return result;
};

const transactionScan = await scan({
  fullQuery: `SELECT toString(count()) AS c, toString(uniqExact(internal_id)) AS u, toString(sum(cityHash64(internal_id))) AS s,
                     toString(groupBitXor(cityHash64(internal_id))) AS x FROM ${view('transaction')}`,
  keyColumns: ['k_internal_id'],
  name: 'transaction_at by internal_id',
  pageQuery: (last) => `SELECT toString(internal_id) AS k_internal_id, toString(cityHash64(internal_id)) AS h FROM ${view('transaction')}
     ${last === undefined ? '' : 'WHERE internal_id > {k_internal_id:UInt64}'} ORDER BY internal_id LIMIT {page:UInt32}`,
});
const outputScan = await scan({
  fullQuery: `SELECT toString(count()) AS c, toString(uniqExact(transaction_internal_id, output_index)) AS u,
                     toString(sum(cityHash64(transaction_internal_id, output_index))) AS s,
                     toString(groupBitXor(cityHash64(transaction_internal_id, output_index))) AS x FROM ${view('output')}`,
  keyColumns: ['k_transaction_internal_id', 'k_output_index'],
  name: 'output_at by (transaction_internal_id, output_index)',
  pageQuery: (last) => `SELECT toString(transaction_internal_id) AS k_transaction_internal_id, toString(output_index) AS k_output_index,
       toString(cityHash64(transaction_internal_id, output_index)) AS h FROM ${view('output')}
     ${last === undefined ? '' : 'WHERE (transaction_internal_id, output_index) > ({k_transaction_internal_id:UInt64}, {k_output_index:UInt32})'}
     ORDER BY transaction_internal_id, output_index LIMIT {page:UInt32}`,
});

/* commits whose transaction ids interleave with an earlier-committed commit's ids */
const [interleave] = await client.query(
  `SELECT toString(count()) AS commits, toString(countIf(min_id < prior_max)) AS interleaved FROM (
     SELECT commit_seq, min_id, max(max_id) OVER (ORDER BY commit_seq ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_max
     FROM (SELECT commit_seq, min(internal_id) AS min_id, max(internal_id) AS max_id FROM ${view('transaction')} GROUP BY commit_seq))`,
  params
);
const report = {
  database: options.database,
  idInterleaving: { commitsWithTransactions: Number(interleave.commits), commitsBelowAnEarlierCommitsMaxId: Number(interleave.interleaved) },
  pageSize,
  scans: [transactionScan, outputScan],
  snapshot: { tail: params.tail.map(String), visible0: String(params.visible0) },
};
console.log(`id interleaving: ${report.idInterleaving.commitsBelowAnEarlierCommitsMaxId} of ${report.idInterleaving.commitsWithTransactions} commits have ids below an earlier commit's max id`);
writeFileSync(options.out, `${JSON.stringify(report, null, 2)}\n`);
await client.close();
