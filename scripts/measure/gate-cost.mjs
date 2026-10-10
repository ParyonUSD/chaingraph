#!/usr/bin/env node
/**
 * WP6 gate cost on reads (plan kill criterion 3: gate cost > 20 %).
 *
 *   node scripts/measure/gate-cost.mjs --agent-dir <built checkout> [--ch-url http://localhost:18123]
 *     [--database chaingraph_ingestion_gate] [--runs 41] [--warmup 5] [--void-pad N | --void-overflow] --out <file.json>
 *
 * On a populated database (e.g. left by `run.mjs --store clickhouse --scenarios max-block --keep-pg`)
 * runs representative API-shaped reads twice each:
 *   gated  – through the pinned views (`*_at`) with the parameters of one readSnapshot, as the API will;
 *   base   – the same query on the base tables with the watermark as a literal
 *            (`commit_seq <= W`) and the same GROUP BY … HAVING sum(sign) > 0 collapse, i.e. without
 *            the gate's visibility / commit_void / epoch_fence / committed-tail subqueries.
 * WP6b: the `*_at` views take every gate input as a parameter (`pinnedView`, `snapshotParams`); with a
 * pre-WP6b build (no `pinnedView`) the WP4 two-parameter form is used, so the same script measures both.
 * `--legacy-views` forces the WP4 form (for a database whose DDL predates WP6b).
 * `--void-pad N` adds N fake aborted seqs (epoch 0, never a data seq) to the `void` parameter, to price a
 * large inline void set; `--void-overflow` passes the overflow sentinel, to price the `commit_void` fallback.
 * Runs alternate gated/base; reports server elapsed (X-ClickHouse-Summary elapsed_ns) and client wall
 * medians, read rows, and overhead = median(gated) / median(base) − 1. EXPLAIN indexes=1 of both
 * variants is saved for every query.
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
    runs: { default: '41', type: 'string' },
    'legacy-views': { default: false, type: 'boolean' },
    'void-overflow': { default: false, type: 'boolean' },
    'void-pad': { default: '0', type: 'string' },
    warmup: { default: '5', type: 'string' },
  },
});
if (options['agent-dir'] === undefined || options.out === undefined) {
  console.error('usage: gate-cost.mjs --agent-dir <dir> --out <file> [--ch-url] [--database] [--runs] [--warmup]');
  process.exit(2);
}

const load = (path) => import(pathToFileURL(join(options['agent-dir'], 'build/store/clickhouse', path)).href);
const [{ ClickHouseClient }, visibility] = await Promise.all([load('client.js'), load('visibility.js')]);
const client = new ClickHouseClient({ database: options.database, password: '', requestTimeoutMs: 120_000, url: options['ch-url'], username: '' });

/** One HTTP query with bound parameters; returns server elapsed, wall, read rows and the rows. */
const run = async (sql, params) => {
  const url = new URL(options['ch-url']);
  url.searchParams.set('database', options.database);
  url.searchParams.set('use_query_cache', '0');
  Object.entries(params).forEach(([key, value]) => {
    url.searchParams.set(`param_${key}`, Array.isArray(value) ? `[${value.join(',')}]` : String(value));
  });
  const started = process.hrtime.bigint();
  const response = await fetch(url, { body: `${sql} FORMAT JSONEachRow`, method: 'POST' });
  const text = await response.text();
  const wallMs = Number(process.hrtime.bigint() - started) / 1e6;
  if (!response.ok) throw new Error(`${response.status}: ${text.slice(0, 500)}\n${sql}`);
  const summary = JSON.parse(response.headers.get('x-clickhouse-summary') ?? '{}');
  return { readRows: Number(summary.read_rows ?? 0), resultRows: text.split('\n').filter(Boolean).length, serverMs: Number(summary.elapsed_ns ?? 0) / 1e6, text, wallMs };
};

const median = (values) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
};

// ---- parameters from the database --------------------------------------------------------
const [nodeRow] = await client.query('SELECT min(internal_id) AS id FROM node FINAL WHERE internal_id > 0');
const node = Number(nodeRow.id);
const snapshot = await visibility.readSnapshot(client, node);
const pinned = { ...visibility.nodeViewParams(snapshot), ...visibility.agnosticViewParams(snapshot) };
const wp6b = typeof visibility.pinnedView === 'function' && !options['legacy-views'];
if (wp6b && Number(options['void-pad']) > 0) {
  pinned.void = [...pinned.void, ...Array.from({ length: Number(options['void-pad']) }, (_, index) => BigInt(index + 1))];
}
if (wp6b && options['void-overflow']) pinned.void = [visibility.voidOverflowSentinel];
/** `name_at(…)` with the parameter placeholders of the build under test. */
const at = (name) =>
  wp6b
    ? visibility.pinnedView(`${name}_at`)
    : ['block', 'block_transaction', 'input', 'output', 'transaction'].includes(name)
      ? `${name}_at(visible0 = {visible0:UInt64}, tail = {tail:Array(UInt64)})`
      : `${name}_at(node = {node:UInt32}, visible = {visible:UInt64})`;
const W = snapshot.visible.toString();
const V0 = snapshot.visible0 > snapshot.visible ? snapshot.visible0.toString() : snapshot.visible.toString();
const [sample] = await client.query(
  `SELECT lower(hex(locking_bytecode_prefix)) AS prefix, lower(hex(transaction_hash)) AS tx, output_index AS idx
   FROM utxo_by_script WHERE node_internal_id = {node:UInt32}
   GROUP BY locking_bytecode_prefix, transaction_hash, output_index HAVING sum(sign) > 0
   ORDER BY cityHash64(transaction_hash) LIMIT 1`,
  { node }
);
const [blockRow] = await client.query('SELECT internal_id AS id FROM block ORDER BY transaction_count DESC LIMIT 1');
const [spentRow] = await client.query(
  'SELECT lower(hex(outpoint_transaction_hash)) AS tx, outpoint_index AS idx FROM input ORDER BY cityHash64(transaction_hash) LIMIT 1'
);
const params = {
  ...pinned,
  block: blockRow.id,
  node,
  outpointIndex: spentRow.idx,
  outpointTx: spentRow.tx,
  prefix: sample.prefix,
  tx: sample.tx,
};
const hash = (name) => `toFixedString(unhex({${name}:String}), 32)`;
// the column is String (substring(locking_bytecode, 1, 25)); a FixedString literal would keep the sort key unused
const fixedPrefix = 'unhex({prefix:String})';


const cases = [
  {
    name: 'utxo by locking bytecode (address balance)',
    gated: `SELECT transaction_hash, output_index, value_satoshis FROM ${at('utxo_by_script')}
            WHERE locking_bytecode_prefix = ${fixedPrefix} ORDER BY transaction_hash, output_index`,
    base: `SELECT transaction_hash, output_index, any(value_satoshis) AS value_satoshis FROM utxo_by_script
           WHERE node_internal_id = {node:UInt32} AND commit_seq <= ${W} AND locking_bytecode_prefix = ${fixedPrefix}
           GROUP BY node_internal_id, locking_bytecode_prefix, transaction_hash, output_index HAVING sum(sign) > 0
           ORDER BY transaction_hash, output_index`,
  },
  {
    name: 'transaction by hash with outputs',
    gated: `SELECT t.hash, t.size_bytes, o.output_index, o.value_satoshis
            FROM ${at('transaction')} AS t
            JOIN (SELECT transaction_hash, output_index, value_satoshis FROM ${at('output')}
                  WHERE transaction_hash = ${hash('tx')}) AS o ON o.transaction_hash = t.hash
            WHERE t.hash = ${hash('tx')} ORDER BY o.output_index`,
    base: `SELECT t.hash, t.size_bytes, o.output_index, o.value_satoshis
           FROM transaction AS t
           JOIN (SELECT transaction_hash, output_index, value_satoshis FROM output
                 WHERE transaction_hash = ${hash('tx')} AND commit_seq <= ${V0}) AS o ON o.transaction_hash = t.hash
           WHERE t.hash = ${hash('tx')} AND t.commit_seq <= ${V0} ORDER BY o.output_index`,
  },
  {
    name: 'block transactions, first page of 1000',
    gated: `SELECT bt.transaction_index, t.hash, t.size_bytes
            FROM (SELECT transaction_index, transaction_hash FROM ${at('block_transaction')}
                  WHERE block_internal_id = {block:UInt64} ORDER BY transaction_index LIMIT 1000) AS bt
            JOIN (SELECT hash, size_bytes FROM ${at('transaction')}
                  WHERE hash IN (SELECT transaction_hash FROM ${at('block_transaction')}
                                 WHERE block_internal_id = {block:UInt64} ORDER BY transaction_index LIMIT 1000)) AS t ON t.hash = bt.transaction_hash
            ORDER BY bt.transaction_index`,
    base: `SELECT bt.transaction_index, t.hash, t.size_bytes
           FROM (SELECT transaction_index, transaction_hash FROM block_transaction
                 WHERE block_internal_id = {block:UInt64} AND commit_seq <= ${V0} ORDER BY transaction_index LIMIT 1000) AS bt
           JOIN (SELECT hash, size_bytes FROM transaction
                 WHERE commit_seq <= ${V0} AND hash IN (SELECT transaction_hash FROM block_transaction
                                WHERE block_internal_id = {block:UInt64} AND commit_seq <= ${V0} ORDER BY transaction_index LIMIT 1000)) AS t ON t.hash = bt.transaction_hash
           ORDER BY bt.transaction_index`,
  },
  {
    name: 'acceptance of a transaction by the node',
    gated: `SELECT block_internal_id, height FROM ${at('tx_acceptance')}
            WHERE transaction_hash = ${hash('tx')}`,
    base: `SELECT block_internal_id, any(height) AS height FROM tx_acceptance
           WHERE node_internal_id = {node:UInt32} AND commit_seq <= ${W} AND transaction_hash = ${hash('tx')}
           GROUP BY transaction_hash, node_internal_id, block_internal_id HAVING sum(sign) > 0`,
  },
  {
    name: 'node UTXO set aggregate (full scan)',
    gated: `SELECT count() AS utxos, sum(value_satoshis) AS satoshis FROM ${at('utxo')}`,
    base: `SELECT count() AS utxos, sum(v) AS satoshis FROM (
             SELECT any(value_satoshis) AS v FROM utxo WHERE node_internal_id = {node:UInt32} AND commit_seq <= ${W}
             GROUP BY node_internal_id, token_category, nonfungible_token_commitment_key, transaction_hash, output_index HAVING sum(sign) > 0)`,
  },
  {
    name: 'spender of an outpoint',
    gated: `SELECT transaction_hash, input_index FROM ${at('input')}
            WHERE outpoint_transaction_hash = ${hash('outpointTx')} AND outpoint_index = {outpointIndex:UInt32}`,
    base: `SELECT transaction_hash, input_index FROM input
           WHERE outpoint_transaction_hash = ${hash('outpointTx')} AND outpoint_index = {outpointIndex:UInt32} AND commit_seq <= ${V0}`,
  },
];

const runs = Number(options.runs);
const warmup = Number(options.warmup);
const results = [];
for (const testCase of cases) {
  const samples = { base: [], gated: [] };
  let check;
  for (let index = 0; index < warmup + runs; index += 1) {
    for (const variant of index % 2 === 0 ? ['gated', 'base'] : ['base', 'gated']) {
      const result = await run(testCase[variant], params);
      if (index >= warmup) samples[variant].push(result);
      if (index === 0) (check ??= {})[variant] = result;
    }
  }
  const stats = (list) => ({
    readRows: median(list.map((item) => item.readRows)),
    resultRows: list[0].resultRows,
    serverMsMedian: median(list.map((item) => item.serverMs)),
    wallMsMedian: median(list.map((item) => item.wallMs)),
  });
  const gated = stats(samples.gated);
  const base = stats(samples.base);
  const entry = {
    base,
    gated,
    name: testCase.name,
    overheadServer: gated.serverMsMedian / base.serverMsMedian - 1,
    overheadWall: gated.wallMsMedian / base.wallMsMedian - 1,
    sameResult: check.gated.text === check.base.text,
  };
  entry.explain = {};
  for (const variant of ['gated', 'base']) {
    entry.explain[variant] = (await run(`EXPLAIN indexes = 1 ${testCase[variant]}`, params)).text.split('\n').map((line) => (line ? JSON.parse(line).explain : '')).join('\n');
  }
  results.push(entry);
  console.log(`${testCase.name}: gated ${gated.serverMsMedian.toFixed(2)} ms vs base ${base.serverMsMedian.toFixed(2)} ms server (${(entry.overheadServer * 100).toFixed(0)} %), wall ${gated.wallMsMedian.toFixed(2)} vs ${base.wallMsMedian.toFixed(2)} ms (${(entry.overheadWall * 100).toFixed(0)} %), rows ${gated.resultRows}/${base.resultRows}, same=${entry.sameResult}`);
}
writeFileSync(options.out, `${JSON.stringify({ database: options.database, params: Object.fromEntries(Object.entries(params).map(([key, value]) => [key, Array.isArray(value) ? (value.length > 20 ? `${value.length} values` : value.map(String)) : String(value)])), voidOverflow: options['void-overflow'], voidPad: Number(options['void-pad']), wp6b, results, runs, warmup }, null, 2)}\n`);
await client.close();
