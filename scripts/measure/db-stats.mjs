#!/usr/bin/env node
/**
 * WP6 database statistics after a gate scenario (plan risk R6 parts/merges; UTXO growth).
 *
 *   node scripts/measure/db-stats.mjs [--ch-url http://localhost:18123] [--database chaingraph_ingestion_gate] --out <file.json>
 *
 * Reports
 *   - per table: active parts, max active parts in one partition, rows, bytes on disk, bytes/row;
 *   - merges in flight for the database, server-wide DelayedInserts/RejectedInserts totals and the
 *     parts_to_delay_insert / parts_to_throw_insert settings;
 *   - per node, for each sign-collapsing table (utxo, utxo_by_script, tx_acceptance, node_transaction,
 *     node_block): stored rows (as merged so far), keys, live keys (sum(sign) = 1), dead keys
 *     (sum(sign) = 0, i.e. +1/−1 pairs from different commits that never collapse because
 *     version = own commit_seq), and rows / live key (the growth ratio). A second figure is taken
 *     after `OPTIMIZE TABLE … FINAL` when --optimize is passed (what background merges can reach).
 */
import { writeFileSync } from 'node:fs';
import { parseArgs } from 'node:util';

const { values: options } = parseArgs({
  options: {
    'ch-url': { default: 'http://localhost:18123', type: 'string' },
    database: { default: 'chaingraph_ingestion_gate', type: 'string' },
    optimize: { default: false, type: 'boolean' },
    out: { type: 'string' },
  },
});
if (options.out === undefined) {
  console.error('usage: db-stats.mjs --out <file> [--ch-url] [--database] [--optimize]');
  process.exit(2);
}

const query = async (sql, params = {}) => {
  const url = new URL(options['ch-url']);
  url.searchParams.set('database', options.database);
  Object.entries(params).forEach(([key, value]) => url.searchParams.set(`param_${key}`, String(value)));
  const response = await fetch(url, { body: `${sql}${/^\s*(SELECT|WITH)/i.test(sql) ? ' FORMAT JSONEachRow' : ''}`, method: 'POST' });
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status}: ${text.slice(0, 400)}`);
  return text.split('\n').filter(Boolean).map((line) => JSON.parse(line));
};

const collapsing = {
  node_block: 'node_internal_id, block_internal_id',
  node_transaction: 'node_internal_id, transaction_internal_id',
  tx_acceptance: 'transaction_hash, node_internal_id, block_internal_id',
  utxo: 'node_internal_id, token_category, nonfungible_token_commitment_key, transaction_hash, output_index',
  utxo_by_script: 'node_internal_id, locking_bytecode_prefix, transaction_hash, output_index',
};

const tables = await query(
  `SELECT table, toString(count()) AS parts, toString(sum(rows)) AS rows, toString(sum(bytes_on_disk)) AS bytes,
          toString(max(partition_parts)) AS max_partition_parts
   FROM (SELECT table, partition_id, rows, bytes_on_disk, count() OVER (PARTITION BY table, partition_id) AS partition_parts
         FROM system.parts WHERE database = {database:String} AND active)
   GROUP BY table ORDER BY table`,
  { database: options.database }
);
const [merges] = await query('SELECT toString(count()) AS c FROM system.merges WHERE database = {database:String}', { database: options.database });
const events = await query("SELECT event, toString(value) AS value FROM system.events WHERE event IN ('DelayedInserts', 'RejectedInserts', 'DelayedInsertsMilliseconds')");
const settings = await query("SELECT name, value FROM system.merge_tree_settings WHERE name IN ('parts_to_delay_insert', 'parts_to_throw_insert')");

const growth = async () => {
  const result = {};
  for (const [table, key] of Object.entries(collapsing)) {
    result[table] = (
      await query(`SELECT node_id AS node, toString(sum(rows)) AS rows, toString(count()) AS keys,
               toString(countIf(s = 1)) AS live, toString(countIf(s = 0)) AS dead, toString(countIf(s NOT IN (0, 1))) AS invalid
             FROM (SELECT any(node_internal_id) AS node_id, count() AS rows, sum(sign) AS s FROM ${table} GROUP BY ${key})
             GROUP BY node_id ORDER BY node_id`)
    ).map((row) => ({ ...row, rowsPerLiveKey: Number(row.live) === 0 ? null : Number(row.rows) / Number(row.live) }));
  }
  return result;
};
const report = {
  collapsing: { asMerged: await growth() },
  database: options.database,
  events: Object.fromEntries(events.map((row) => [row.event, Number(row.value)])),
  mergesInFlight: Number(merges.c),
  settings: Object.fromEntries(settings.map((row) => [row.name, Number(row.value)])),
  tables: tables.map((row) => ({ ...row, bytesPerRow: Number(row.rows) === 0 ? null : Number(row.bytes) / Number(row.rows) })),
};
if (options.optimize) {
  for (const table of Object.keys(collapsing)) await query(`OPTIMIZE TABLE ${table} FINAL`);
  report.collapsing.afterOptimizeFinal = await growth();
  report.tablesAfterOptimize = (
    await query(
      `SELECT table, toString(count()) AS parts, toString(sum(rows)) AS rows, toString(sum(bytes_on_disk)) AS bytes
       FROM system.parts WHERE database = {database:String} AND active GROUP BY table ORDER BY table`,
      { database: options.database }
    )
  ).map((row) => ({ ...row, bytesPerRow: Number(row.rows) === 0 ? null : Number(row.bytes) / Number(row.rows) }));
}
writeFileSync(options.out, `${JSON.stringify(report, null, 2)}\n`);
console.table(report.tables.map(({ table, parts, max_partition_parts, rows, bytes, bytesPerRow }) => ({ table, parts, maxPerPartition: max_partition_parts, rows, MB: (Number(bytes) / 1e6).toFixed(1), bytesPerRow: bytesPerRow?.toFixed(1) })));
for (const [phase, data] of Object.entries(report.collapsing)) {
  console.log(`-- collapsing tables (${phase})`);
  Object.entries(data).forEach(([table, rows]) => rows.forEach((row) => console.log(`${table} node ${row.node}: rows ${row.rows}, keys ${row.keys}, live ${row.live}, dead ${row.dead}, invalid ${row.invalid}, rows/live ${row.rowsPerLiveKey?.toFixed(3)}`)));
}
console.log(`merges in flight ${report.mergesInFlight}; events ${JSON.stringify(report.events)}; settings ${JSON.stringify(report.settings)}`);
