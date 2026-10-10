#!/usr/bin/env node
/**
 * WP6 torn-read poller and head-of-line (HOL) wait probe (plan G1 item 5).
 *
 *   node scripts/measure/torn-read-poller.mjs --agent-dir <built checkout> \
 *     [--ch-url http://localhost:18123] [--database chaingraph_ingestion_gate] \
 *     [--interval-ms 50] [--utxo-sample 16] --out <summary.json>
 *
 * Runs until SIGINT/SIGTERM (or --duration-s), then writes a JSON summary.
 * Run it from a separate process while the ingestion gate (or any driver)
 * writes to the database. It tolerates the database not existing yet and
 * being dropped/recreated (each incarnation, by database UUID, is reported
 * separately).
 *
 * Every tick (default 50 ms):
 *   1. records max(visible_seq) per node from `visibility` with the host
 *      clock (first time each watermark value is seen), and the new
 *      `committed` rows of `commit_log` (seq, scope, started_at, finished_at);
 *   2. for every registered node n >= 1, takes ONE `readSnapshot(n)` and with
 *      those parameters checks
 *      a. for every block visible in node_block_at(n): block_at has the
 *         block, block_transaction_at links == block.transaction_count, and
 *         n's tx_acceptance_at rows for the block == transaction_count;
 *      b. every tx_acceptance_at(n) row with block_internal_id != 0 belongs
 *         to a block in node_block_at(n);
 *      c. a 1/<utxo-sample> sample (rotating) of utxo keys: sum(sign) over
 *         the gated base rows at the snapshot is 0 or 1 (never < 0 or > 1).
 *   Any violation is a torn read (recorded with the snapshot).
 *
 * HOL wait (reported per incarnation, for committed commits with a non-empty
 * node scope): visibleAt(seq, n) = first host time the poller saw
 * watermark(n) >= seq; hol = visibleAt - finished_at (the agent's commit
 * time, host clock); total = visibleAt - started_at (intent). Resolution is
 * the achieved tick interval (reported).
 */
import { join } from 'node:path';
import { writeFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';

const { values: options } = parseArgs({
  options: {
    'agent-dir': { type: 'string' },
    'ch-url': { default: 'http://localhost:18123', type: 'string' },
    database: { default: 'chaingraph_ingestion_gate', type: 'string' },
    'duration-s': { type: 'string' },
    'interval-ms': { default: '50', type: 'string' },
    out: { type: 'string' },
    'utxo-sample': { default: '16', type: 'string' },
  },
});
if (options['agent-dir'] === undefined || options.out === undefined) {
  console.error('usage: torn-read-poller.mjs --agent-dir <dir> --out <file> [--ch-url] [--database] [--interval-ms] [--duration-s]');
  process.exit(2);
}

const load = (path) => import(pathToFileURL(join(options['agent-dir'], 'build/store/clickhouse', path)).href);
const [{ ClickHouseClient }, visibility] = await Promise.all([load('client.js'), load('visibility.js')]);
const intervalMs = Number(options['interval-ms']);
const utxoSample = Number(options['utxo-sample']);
const database = options.database;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const admin = new ClickHouseClient({ database: 'system', password: '', requestTimeoutMs: 60_000, url: options['ch-url'], username: '' });

const quantile = (values, q) => {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
};

const fenceWith = visibility.gateSql.fenceWith;
const nodeGate = `node_internal_id = {node:UInt32}
  AND commit_seq <= least({visible:UInt64}, (SELECT max(visible_seq) FROM visibility WHERE node_internal_id = {node:UInt32}))
  AND commit_seq NOT IN (SELECT commit_seq FROM commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)))`;

// WP6b builds export pinnedView (views take fence/void too); older builds take the WP4 arguments.
const agnostic = (name) =>
  visibility.pinnedView?.(`${name}_at`) ?? `${name}_at(visible0 = {visible0:UInt64}, tail = {tail:Array(UInt64)})`;
const pinned = (name) =>
  visibility.pinnedView?.(`${name}_at`) ?? `${name}_at(node = {node:UInt32}, visible = {visible:UInt64})`;

const queries = {
  blockInvariant: `SELECT
      toString(nb.block_internal_id) AS block,
      b.internal_id = 0 AS block_missing,
      toString(b.transaction_count) AS expected,
      toString(ifNull(bt.c, 0)) AS links,
      toString(ifNull(acc.c, 0)) AS acceptances
    FROM ${pinned('node_block')} AS nb
    LEFT JOIN (SELECT internal_id, transaction_count FROM ${agnostic('block')}) AS b ON b.internal_id = nb.block_internal_id
    LEFT JOIN (SELECT block_internal_id, count() AS c FROM ${agnostic('block_transaction')} GROUP BY block_internal_id) AS bt
      ON bt.block_internal_id = nb.block_internal_id
    LEFT JOIN (SELECT block_internal_id, count() AS c FROM ${pinned('tx_acceptance')} WHERE block_internal_id != 0 GROUP BY block_internal_id) AS acc
      ON acc.block_internal_id = nb.block_internal_id
    WHERE block_missing OR links != expected OR acceptances != expected
    LIMIT 5`,
  orphanAcceptance: `SELECT toString(count()) AS c, toString(any(block_internal_id)) AS example
    FROM ${pinned('tx_acceptance')}
    WHERE block_internal_id != 0 AND block_internal_id NOT IN (SELECT block_internal_id FROM ${pinned('node_block')})`,
  utxoSums: `WITH ${fenceWith}
    SELECT toString(countIf(s NOT IN (0, 1))) AS bad, toString(anyIf(s, s NOT IN (0, 1))) AS example_sum, toString(count()) AS sampled FROM (
      SELECT sum(sign) AS s FROM utxo
      WHERE ${nodeGate} AND cityHash64(transaction_hash, output_index) % {mod:UInt64} = {slot:UInt64}
      GROUP BY token_category, nonfungible_token_commitment_key, transaction_hash, output_index)
    SETTINGS max_threads = 2`,
};

const incarnations = new Map();
let current;
let stopping = false;
const tickIntervals = [];
let lastTickStart;

const newIncarnation = (uuid) => ({
  checks: { blockChecks: 0, orphanChecks: 0, utxoKeysSampled: 0, utxoChecks: 0 },
  client: new ClickHouseClient({ database, password: '', requestTimeoutMs: 60_000, url: options['ch-url'], username: '' }),
  commits: new Map(),
  errors: 0,
  errorExamples: [],
  firstSeenAt: new Date().toISOString(),
  lastCommittedSeq: 0n,
  polls: 0,
  tornReads: [],
  uuid,
  watermarkLog: new Map(),
});

const recordError = (incarnation, error) => {
  incarnation.errors += 1;
  if (incarnation.errorExamples.length < 5) incarnation.errorExamples.push(String(error?.message ?? error).slice(0, 300));
};

const tick = async (slot) => {
  const [databaseRow] = await admin.query('SELECT toString(uuid) AS uuid FROM system.databases WHERE name = {name:String}', { name: database });
  if (databaseRow === undefined) return;
  if (current?.uuid !== databaseRow.uuid) {
    current = newIncarnation(databaseRow.uuid);
    incarnations.set(current.uuid, current);
  }
  const incarnation = current;
  const { client } = incarnation;
  try {
    const now = Date.now();
    const marks = await client.query('SELECT node_internal_id AS node, toString(max(visible_seq)) AS seq FROM visibility GROUP BY node_internal_id');
    marks.forEach(({ node, seq }) => {
      const log = incarnation.watermarkLog.get(Number(node)) ?? [];
      if (log.length === 0 || BigInt(log[log.length - 1].seq) < BigInt(seq)) log.push({ at: now, seq });
      incarnation.watermarkLog.set(Number(node), log);
    });
    const committed = await client.query(
      `SELECT toString(commit_seq) AS seq, node_scope AS scope, toString(kind) AS kind,
              toUnixTimestamp64Milli(started_at) AS started_ms, toUnixTimestamp64Milli(assumeNotNull(finished_at)) AS finished_ms
       FROM commit_log WHERE state = 'committed' AND commit_seq > {after:UInt64}`,
      { after: incarnation.lastCommittedSeq }
    );
    committed.forEach((row) => {
      incarnation.commits.set(row.seq, { finishedMs: Number(row.finished_ms), kind: row.kind, scope: row.scope.map(Number), startedMs: Number(row.started_ms) });
      if (BigInt(row.seq) > incarnation.lastCommittedSeq) incarnation.lastCommittedSeq = BigInt(row.seq);
    });
    const nodes = await client.query('SELECT DISTINCT internal_id AS id FROM node FINAL WHERE internal_id > 0');
    for (const { id } of nodes) {
      const snapshot = await visibility.readSnapshot(client, Number(id));
      const params = { ...visibility.nodeViewParams(snapshot), ...visibility.agnosticViewParams(snapshot) };
      const snapshotText = { node: Number(id), visible: snapshot.visible.toString(), visible0: snapshot.visible0.toString(), tail: snapshot.committedTail.length };
      const badBlocks = await client.query(queries.blockInvariant, params);
      incarnation.checks.blockChecks += 1;
      badBlocks.forEach((row) => incarnation.tornReads.push({ at: new Date().toISOString(), check: 'block links/acceptances != transaction_count', row, snapshot: snapshotText }));
      const [orphan] = await client.query(queries.orphanAcceptance, params);
      incarnation.checks.orphanChecks += 1;
      if (Number(orphan.c) > 0) incarnation.tornReads.push({ at: new Date().toISOString(), check: 'tx_acceptance row of an invisible block', row: orphan, snapshot: snapshotText });
      const [utxo] = await client.query(queries.utxoSums, { ...params, mod: utxoSample, slot: slot % utxoSample });
      incarnation.checks.utxoChecks += 1;
      incarnation.checks.utxoKeysSampled += Number(utxo.sampled);
      if (Number(utxo.bad) > 0) incarnation.tornReads.push({ at: new Date().toISOString(), check: 'utxo sum(sign) outside {0,1}', row: utxo, snapshot: snapshotText });
    }
    incarnation.polls += 1;
  } catch (error) {
    recordError(incarnation, error);
  }
};

const summarise = (incarnation) => {
  const holMs = [];
  const totalMs = [];
  const perKind = {};
  let unresolved = 0;
  const seenSince = Date.parse(incarnation.firstSeenAt);
  incarnation.commits.forEach((commit, seq) => {
    // commits finished before the poller saw this database have no observed publish time
    if (commit.finishedMs < seenSince) return;
    commit.scope.forEach((node) => {
      const log = incarnation.watermarkLog.get(node) ?? [];
      const seen = log.find((entry) => BigInt(entry.seq) >= BigInt(seq));
      if (seen === undefined) {
        unresolved += 1;
        return;
      }
      const hol = Math.max(0, seen.at - commit.finishedMs);
      holMs.push(hol);
      totalMs.push(Math.max(0, seen.at - commit.startedMs));
      (perKind[commit.kind] ??= []).push(hol);
    });
  });
  const stats = (values) => ({ count: values.length, max: values.length ? Math.max(...values) : null, p50: quantile(values, 0.5), p95: quantile(values, 0.95) });
  return {
    checks: incarnation.checks,
    commitsCommitted: incarnation.commits.size,
    errorExamples: incarnation.errorExamples,
    errors: incarnation.errors,
    firstSeenAt: incarnation.firstSeenAt,
    holMs: stats(holMs),
    holMsByKind: Object.fromEntries(Object.entries(perKind).map(([kind, values]) => [kind, stats(values)])),
    intentToVisibleMs: stats(totalMs),
    polls: incarnation.polls,
    tornReadCount: incarnation.tornReads.length,
    tornReads: incarnation.tornReads.slice(0, 20),
    unresolvedCommitNodePairs: unresolved,
    uuid: incarnation.uuid,
    watermarkChanges: Object.fromEntries([...incarnation.watermarkLog].map(([node, log]) => [node, log.length])),
  };
};

const finish = () => {
  const report = {
    database,
    intervalMs,
    tickMs: { p50: quantile(tickIntervals, 0.5), p95: quantile(tickIntervals, 0.95), ticks: tickIntervals.length },
    incarnations: [...incarnations.values()].map(summarise),
  };
  writeFileSync(options.out, `${JSON.stringify(report, null, 2)}\n`);
  const totals = report.incarnations.reduce((sum, item) => sum + item.tornReadCount, 0);
  console.log(`torn-read-poller: ${report.incarnations.length} incarnation(s), ${totals} torn read(s), tick p50 ${report.tickMs.p50} ms p95 ${report.tickMs.p95} ms -> ${options.out}`);
  process.exit(0);
};
process.on('SIGINT', () => (stopping = true));
process.on('SIGTERM', () => (stopping = true));
const deadline = options['duration-s'] ? Date.now() + Number(options['duration-s']) * 1000 : Infinity;

let slot = 0;
while (!stopping && Date.now() < deadline) {
  const started = Date.now();
  if (lastTickStart !== undefined) tickIntervals.push(started - lastTickStart);
  lastTickStart = started;
  try {
    await tick(slot);
  } catch (error) {
    if (current) recordError(current, error);
  }
  slot += 1;
  const elapsed = Date.now() - started;
  if (elapsed < intervalMs) await sleep(intervalMs - elapsed);
}
finish();
