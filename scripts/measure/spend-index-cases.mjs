#!/usr/bin/env node
// cspell:ignore clickhouse unhex
/**
 * Spent-output filter cases timed three ways on one pinned snapshot
 * (docs/clickhouse-port/spend-index.md, "Measurements"):
 * - `join`: outputs matching the filter, then the inputs spending them by
 *   outpoint (input's p_outpoint), the input denormalisation kit's join form;
 * - `index`: the spend index alone (`spend_at` up to the snapshot's spend
 *   watermark): exact only when the index has caught up;
 * - `api`: what the API runs (`spendRelationSql`: index up to the spend
 *   watermark ∪ the join above it), exact at any lag.
 * Each case returns a fingerprint (count, hash sum, value sums); `join` and
 * `api` must be equal (and `index` when the watermark equals visible(0)).
 *
 *   node scripts/measure/spend-index-cases.mjs <agent dir> <clickhouse url> <database> <params.json> [warm reps] [cold reps] [lag batches]
 *
 * `lag batches` > 0 pins the spend watermark that many finished batches back (a lagging index: `api` then reads
 * the tail by join; `index` is then not exact and not compared).
 *
 * params.json: [{ "level": "typical", "lb": "76a9…88ac", "cat": "<64 hex>", "nftCat": "<64 hex>", "cmt": "00" }, …]
 * Prints JSON lines (one per level × case × form: median ms warm / cold, rows read, fingerprint).
 * Cold = mark, uncompressed, primary-index and query caches dropped before each run (not the OS page cache).
 */
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [agentDirectory, url, database, paramsFile, warmArg, coldArg, lagArg] = process.argv.slice(2);
if (paramsFile === undefined) {
  console.error('usage: spend-index-cases.mjs <agent dir> <clickhouse url> <database> <params.json> [warm] [cold]');
  process.exit(2);
}
const warmReps = Number(warmArg ?? 5);
const coldReps = Number(coldArg ?? 3);
const load = (file) => import(pathToFileURL(join(agentDirectory, 'build', file)).href);
const [clientModule, visibilityModule, sqlModule] = await Promise.all([
  load('store/clickhouse/client.js'),
  load('store/clickhouse/visibility.js'),
  load('api/sql.js'),
]);
const client = new clientModule.ClickHouseClient({
  database,
  password: process.env.CH_PASSWORD ?? '',
  requestTimeoutMs: 600_000,
  url,
  username: process.env.CH_USER ?? '',
});
const levels = JSON.parse(readFileSync(paramsFile, 'utf8'));
const lagBatches = Number(lagArg ?? 0);
const read = await visibilityModule.readSnapshot(client, 0);
const lagged =
  lagBatches > 0
    ? await client.query(
        `SELECT toString(watermark) AS w FROM spend_watermark ORDER BY watermark DESC LIMIT 1 OFFSET {lag:UInt32}`,
        { lag: lagBatches }
      )
    : [];
const snapshot = lagged[0] === undefined ? read : { ...read, spend: BigInt(lagged[0].w) };
const sql = new sqlModule.Sql(snapshot);
const view = (name) => visibilityModule.pinnedView(name);
const scriptFilter = (lb) =>
  `locking_bytecode_prefix = substring(unhex('${lb}'), 1, 25) AND locking_bytecode = unhex('${lb}')`;
const categoryFilter = (cat) => `token_category = toFixedString(unhex('${cat}'), 32)`;
const nftFilter = (cat, cmt) =>
  `${categoryFilter(cat)} AND nonfungible_token_capability = 'mutable' AND startsWith(nonfungible_token_commitment_key, unhex('${cmt}'))`;

/** The spend relation of a predicate in each form (columns of `spendRelationSql`). */
const relation = (form, predicate) => {
  if (form === 'api') return sqlModule.spendRelationSql(sql, predicate);
  if (form === 'index') return `(SELECT * FROM ${view('spend_at')} WHERE ${predicate})`;
  // join: outputs matching, then the inputs spending them by outpoint
  return `(SELECT i.transaction_hash AS transaction_hash, i.input_index AS input_index,
      o.value_satoshis AS value_satoshis, o.fungible_token_amount AS fungible_token_amount
    FROM (SELECT transaction_hash, input_index, outpoint_transaction_hash, outpoint_index FROM ${view('input_at')}
          WHERE (outpoint_transaction_hash, outpoint_index) IN
                (SELECT transaction_hash, output_index FROM ${view('output_at')} WHERE ${predicate})) AS i
    INNER JOIN (SELECT transaction_hash, output_index, value_satoshis, fungible_token_amount FROM ${view(
      'output_at'
    )} WHERE ${predicate}) AS o
      ON o.transaction_hash = i.outpoint_transaction_hash AND o.output_index = i.outpoint_index)`;
};
const semiRelation = (form, predicate) =>
  form === 'join'
    ? `(SELECT transaction_hash FROM ${view('input_at')}
        WHERE (outpoint_transaction_hash, outpoint_index) IN
              (SELECT transaction_hash, output_index FROM ${view('output_at')} WHERE ${predicate}))`
    : relation(form, predicate);

const cases = (level) => ({
  ...(level.lb === undefined
    ? {}
    : {
        'spends-addr': (form) =>
          `SELECT count() AS n, toString(sum(cityHash64(transaction_hash, input_index))) AS fp,
             toString(sum(value_satoshis)) AS sats FROM ${relation(form, scriptFilter(level.lb))}`,
        'txs-addr': (form) =>
          `SELECT count() AS n, toString(sum(cityHash64(h))) AS fp FROM (SELECT DISTINCT h FROM (
             SELECT transaction_hash AS h FROM ${view('output_at')} WHERE ${scriptFilter(level.lb)}
             UNION ALL SELECT transaction_hash AS h FROM ${semiRelation(form, scriptFilter(level.lb))}))`,
      }),
  ...(level.cat === undefined
    ? {}
    : {
        'spends-cat': (form) =>
          `SELECT count() AS n, toString(sum(cityHash64(transaction_hash, input_index))) AS fp,
             toString(sum(ifNull(fungible_token_amount, 0))) AS ft, toString(sum(value_satoshis)) AS sats
           FROM ${relation(form, categoryFilter(level.cat))}`,
      }),
  ...(level.nftCat === undefined
    ? {}
    : {
        'txs-nft': (form) =>
          `SELECT count() AS n, toString(sum(cityHash64(h))) AS fp FROM (SELECT DISTINCT transaction_hash AS h
             FROM ${semiRelation(form, nftFilter(level.nftCat, level.cmt ?? ''))})`,
      }),
});

const dropCaches = async () => {
  for (const statement of [
    'SYSTEM DROP MARK CACHE',
    'SYSTEM DROP UNCOMPRESSED CACHE',
    'SYSTEM DROP PRIMARY INDEX CACHE',
    'SYSTEM DROP QUERY CACHE',
    'SYSTEM DROP MMAP CACHE',
  ]) {
    await client.command(statement).catch(() => undefined);
  }
};

const run = async (statement) => {
  const id = `si-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  const started = performance.now();
  const rows = await client.query(statement, sql.params, { log_comment: id, use_query_cache: 0 });
  const ms = performance.now() - started;
  return { id, ms, row: rows[0] };
};

const median = (values) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor((sorted.length - 1) / 2)];
};

const results = [];
for (const level of levels) {
  for (const [name, build] of Object.entries(cases(level))) {
    for (const form of ['join', 'index', 'api']) {
      const statement = build(form);
      await run(statement); // warm-up
      const warm = [];
      for (let rep = 0; rep < warmReps; rep += 1) warm.push(await run(statement));
      const cold = [];
      for (let rep = 0; rep < coldReps; rep += 1) {
        await dropCaches();
        cold.push(await run(statement));
      }
      await client.command('SYSTEM FLUSH LOGS');
      const [stats] = await client.query(
        `SELECT median(query_duration_ms) AS server_ms, median(read_rows) AS read_rows
         FROM system.query_log WHERE type = 'QueryFinish' AND event_date = today()
           AND log_comment IN {ids:Array(String)}`,
        { ids: warm.map((item) => item.id) }
      );
      const result = {
        case: name,
        coldMs: Math.round(median(cold.map((item) => item.ms))),
        fingerprint: warm[0].row,
        form,
        level: level.level,
        readRows: Number(stats?.read_rows ?? 0),
        serverWarmMs: Number(stats?.server_ms ?? 0),
        warmMs: Math.round(median(warm.map((item) => item.ms))),
      };
      results.push(result);
      console.log(JSON.stringify(result));
    }
  }
}
const mismatches = results.filter((result) => {
  const join = results.find(
    (other) => other.level === result.level && other.case === result.case && other.form === 'join'
  );
  if (result.form === 'index' && snapshot.spend !== snapshot.visible0) return false;
  return JSON.stringify(join?.fingerprint) !== JSON.stringify(result.fingerprint);
});
console.log(
  JSON.stringify({
    equal: mismatches.length === 0,
    mismatches: mismatches.map((result) => `${result.level}/${result.case}/${result.form}`),
    spendWatermark: snapshot.spend.toString(),
    visible0: snapshot.visible0.toString(),
  })
);
await client.close();
process.exitCode = mismatches.length === 0 ? 0 : 1;
