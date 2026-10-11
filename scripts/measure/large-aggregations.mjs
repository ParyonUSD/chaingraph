#!/usr/bin/env node
// cspell:ignore clickhouse antijoin signsum denorm spendidx chipnet
/**
 * Large per-node aggregations, ClickHouse vs Postgres (Chaingraph v1 schema)
 * (docs/clickhouse-port/large-aggregations.md). Read-only.
 *
 *   node scripts/measure/large-aggregations.mjs <agent dir> <clickhouse url> <postgres url> \
 *     <utxo db> <spend-index db> [warm reps] [cold reps] [only: regexp over <query>:<form>]
 *
 * Queries (node 1), each at the database's tip (mempool included, v1
 * acceptance = mempool ∪ accepted blocks) and at a common height H = min of
 * the tips (accepted blocks at height <= H only):
 * - Q1 count + sum of the node's unspent outputs;
 * - Q2 count + sum of every output of a transaction in an accepted block;
 * - Q3 fees of the last 10,000 blocks (spent value of non-coinbase inputs
 *   minus outputs of non-coinbase transactions);
 * - Q4 unspent count + value per token category, top 20 by count.
 * ClickHouse reads the pinned `*_at` views with parameters from the compiled
 * store's `snapshotSql` (`readSnapshot` itself on the spend-index database;
 * the UTXO database predates `spend_watermark`, so the same statement runs
 * with an empty watermark source), and the bare base tables for reference.
 *
 * Prints JSON lines: one per query × scope × database × form (result, warm
 * median ms of `warm reps` after a warm-up, cold median ms with ClickHouse's
 * mark / uncompressed / primary-index / query / mmap caches dropped (not the
 * OS page cache), ClickHouse rows/bytes read from system.query_log, Postgres
 * EXPLAIN (ANALYZE, BUFFERS) totals), then one agreement line per check.
 */
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [agentDirectory, chUrl, pgUrl, utxoDb, spendDb, warmArg, coldArg, onlyArg] = process.argv.slice(2);
if (spendDb === undefined) {
  console.error(
    'usage: large-aggregations.mjs <agent dir> <clickhouse url> <postgres url> <utxo db> <spend-index db> [warm] [cold] [only]'
  );
  process.exit(2);
}
const warmReps = Number(warmArg ?? 5);
const coldReps = Number(coldArg ?? 3);
/** Regular expression over `<query>:<form>` (e.g. `^Q1:` or `hash`); default every statement. */
const only = new RegExp(onlyArg ?? '');
const node = 1;
const load = (file) => import(pathToFileURL(join(agentDirectory, 'build', file)).href);
const [clientModule, visibilityModule, sqlModule] = await Promise.all([
  load('store/clickhouse/client.js'),
  load('store/clickhouse/visibility.js'),
  load('api/sql.js'),
]);
const pgModule = await import(pathToFileURL(join(agentDirectory, 'node_modules', 'pg', 'lib', 'index.js')).href);
const PgClient = pgModule.default?.Client ?? pgModule.Client;

const median = (values) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor((sorted.length - 1) / 2)];
};
const round = (ms) => Math.round(ms * 10) / 10;

/* ---------------------------------------------------------------- ClickHouse */

const chClient = (database) =>
  new clientModule.ClickHouseClient({
    database,
    password: process.env.CH_PASSWORD ?? '',
    requestTimeoutMs: 1_800_000,
    url: chUrl,
    username: process.env.CH_USER ?? '',
  });

/**
 * `readSnapshot(client, node)` of the compiled store; for a database without
 * `spend_watermark` the same `snapshotSql` with an empty watermark source and
 * the same post-processing as `readSnapshotMulti`.
 */
const snapshotOf = async (client, hasSpendIndex) => {
  if (hasSpendIndex) return visibilityModule.readSnapshot(client, node);
  const statement = visibilityModule.snapshotSql({
    commitLog: 'commit_log',
    commitVoid: 'commit_void',
    epochFence: 'epoch_fence',
    spendWatermark: '(SELECT toUInt64(0) AS watermark WHERE 0)',
    visibility: 'visibility',
  });
  const [row] = await client.query(statement, {
    nodes: [node],
    voidLimit: visibilityModule.voidInlineLimit + 1,
  });
  const voidSeqs = (row?.void ?? []).map(BigInt);
  const voidOverflow = voidSeqs.length > visibilityModule.voidInlineLimit;
  return {
    committedTail: (row?.tail ?? []).map(BigInt),
    fence: (row?.fence ?? []).map(BigInt),
    nodeId: node,
    spend: BigInt(row?.spend ?? '0'),
    visible: BigInt(row?.visible?.[0] ?? '0'),
    visible0: BigInt(row?.visible0 ?? '0'),
    void: voidOverflow ? [visibilityModule.voidOverflowSentinel] : voidSeqs,
    voidOverflow,
  };
};

/** View sources: pinned (`gated`) or the bare base tables (`base`, no gate). */
const viewsFor = (mode) =>
  mode === 'gated'
    ? (name) => visibilityModule.pinnedView(name)
    : (name) =>
        ({
          input_at: 'input',
          output_at: 'output',
          spend_at: 'spend',
          transaction_at: 'transaction',
          tx_acceptance_at: `(SELECT transaction_hash, block_internal_id, any(height) AS height FROM tx_acceptance
             WHERE node_internal_id = ${node} GROUP BY transaction_hash, node_internal_id, block_internal_id
             HAVING sum(sign) > 0)`,
          utxo_at: `(SELECT token_category, transaction_hash, output_index, any(value_satoshis) AS value_satoshis
             FROM utxo WHERE node_internal_id = ${node}
             GROUP BY node_internal_id, token_category, nonfungible_token_commitment_key, transaction_hash, output_index
             HAVING sum(sign) > 0)`,
        })[name];

/** Accepted transactions: tip = every acceptance (mempool too); H = accepted blocks at height <= H. */
const chAccepted = (view, scope) =>
  `SELECT transaction_hash FROM ${view('tx_acceptance_at')}${
    scope.height === undefined ? '' : ` WHERE block_internal_id != 0 AND height <= ${scope.height}`
  }`;

/** Spent outpoints of the node, by form. `sql` is the request's `Sql` (gated spend form only). */
const chSpent = (view, scope, form, sql) => {
  const accepted = chAccepted(view, scope);
  if (form === 'spendidx') {
    const relation =
      sql === undefined ? `(SELECT transaction_hash, outpoint_transaction_hash, outpoint_index FROM spend)` : sqlModule.spendRelationSql(sql, '1');
    return `SELECT outpoint_transaction_hash, outpoint_index FROM ${relation} WHERE transaction_hash IN (${accepted})`;
  }
  return `SELECT outpoint_transaction_hash, outpoint_index FROM ${view('input_at')} WHERE transaction_hash IN (${accepted})`;
};

/** Unspent outputs (v1 anti-join), as a row source with token_category and value_satoshis. */
const chUnspent = (view, scope, form, sql) =>
  `(SELECT token_category, value_satoshis FROM ${view('output_at')}
    WHERE transaction_hash IN (${chAccepted(view, scope)})
      AND (transaction_hash, output_index) NOT IN (${chSpent(view, scope, form, sql)}))`;

/**
 * The same set by a hash anti-join (the mainnet UTXO check's form,
 * `ch1-utxo-check.sql`): add `hashJoinSettings` to the statement.
 */
const chUnspentHash = (view, scope, form, sql) =>
  `(SELECT o.token_category AS token_category, o.value_satoshis AS value_satoshis
    FROM (SELECT transaction_hash, output_index, token_category, value_satoshis FROM ${view('output_at')}
          WHERE transaction_hash IN (${chAccepted(view, scope)})) AS o
    LEFT ANTI JOIN (SELECT outpoint_transaction_hash AS transaction_hash, outpoint_index AS output_index
                    FROM (${chSpent(view, scope, form, sql)})) AS s
    USING (transaction_hash, output_index))`;
const hashJoinSettings = ` SETTINGS join_algorithm = 'parallel_hash'`;

const top20 = (source, category) =>
  `SELECT ${category} AS category, toString(count()) AS n, toString(sum(value_satoshis)) AS sats FROM ${source}
   GROUP BY category ORDER BY count() DESC, category LIMIT 20`;

/** Non-coinbase transactions of the node's accepted blocks in [lo, hi]. */
const chFeeTxs = (view, lo, hi) =>
  `SELECT hash FROM ${view('transaction_at')} WHERE NOT is_coinbase AND hash IN (
     SELECT transaction_hash FROM ${view('tx_acceptance_at')} WHERE block_internal_id != 0 AND height BETWEEN ${lo} AND ${hi})`;

const chFees = (view, lo, hi, form, sql) => {
  const txs = chFeeTxs(view, lo, hi);
  const spentIn =
    form === 'denorm'
      ? `SELECT sum(value_satoshis) FROM ${view('input_at')} WHERE transaction_hash IN (${txs})`
      : form === 'spendidx'
        ? `SELECT sum(value_satoshis) FROM ${
            sql === undefined ? 'spend' : sqlModule.spendRelationSql(sql, '1')
          } WHERE transaction_hash IN (${txs})`
        : `SELECT sum(o.value_satoshis) FROM
             (SELECT outpoint_transaction_hash, outpoint_index FROM ${view('input_at')} WHERE transaction_hash IN (${txs})) AS i
             INNER JOIN (SELECT transaction_hash, output_index, value_satoshis FROM ${view('output_at')}
                         WHERE (transaction_hash, output_index) IN (
                           SELECT outpoint_transaction_hash, outpoint_index FROM ${view('input_at')} WHERE transaction_hash IN (${txs}))) AS o
             ON o.transaction_hash = i.outpoint_transaction_hash AND o.output_index = i.outpoint_index`;
  return `SELECT (SELECT count() FROM (${txs})) AS txs,
    toString((${spentIn})) AS sats_in,
    toString((SELECT sum(value_satoshis) FROM ${view('output_at')} WHERE transaction_hash IN (${txs}))) AS sats_out,
    toString(toInt64(sats_in) - toInt64(sats_out)) AS fees`;
};

/** ClickHouse statements: [query, form, statement] for one database, mode and scope. */
const chStatements = ({ hasUtxo, hasSpend, hasDenorm }, mode, scope, sql) => {
  const view = viewsFor(mode);
  const gatedSql = mode === 'gated' ? sql : undefined;
  const out = [];
  const zeroCategory = `if(token_category = ${sqlModule.zeroCategory}, '', lower(hex(token_category)))`;
  const atTip = scope.height === undefined;
  // Q1
  if (hasUtxo && atTip) {
    out.push(['Q1', 'utxo', `SELECT count() AS n, toString(sum(value_satoshis)) AS sats FROM ${view('utxo_at')}`]);
    if (mode === 'gated') {
      out.push([
        'Q1',
        'utxo-signsum',
        `SELECT sum(sign) AS n, toString(sum(sign * value_satoshis)) AS sats FROM utxo
         WHERE node_internal_id = {node:UInt32} AND ${visibilityModule.gateSql.visibleAt()}`,
      ]);
    }
  }
  out.push(['Q1', 'antijoin', `SELECT count() AS n, toString(sum(value_satoshis)) AS sats FROM ${chUnspent(view, scope, 'input', gatedSql)}`]);
  out.push(['Q1', 'antijoin-hash', `SELECT count() AS n, toString(sum(value_satoshis)) AS sats FROM ${chUnspentHash(view, scope, 'input', gatedSql)}${hashJoinSettings}`]);
  if (hasSpend) {
    out.push(['Q1', 'spendidx-hash', `SELECT count() AS n, toString(sum(value_satoshis)) AS sats FROM ${chUnspentHash(view, scope, 'spendidx', gatedSql)}${hashJoinSettings}`]);
    out.push(['Q1', 'spendidx', `SELECT count() AS n, toString(sum(value_satoshis)) AS sats FROM ${chUnspent(view, scope, 'spendidx', gatedSql)}`]);
  }
  // Q2
  out.push([
    'Q2',
    'blocks',
    `SELECT count() AS n, toString(sum(value_satoshis)) AS sats FROM ${view('output_at')}
     WHERE transaction_hash IN (SELECT transaction_hash FROM ${view('tx_acceptance_at')} WHERE block_internal_id != 0${
       atTip ? '' : ` AND height <= ${scope.height}`
     })`,
  ]);
  // Q3
  out.push(['Q3', 'join', chFees(view, scope.feeLo, scope.feeHi, 'join', gatedSql)]);
  if (hasDenorm) out.push(['Q3', 'denorm', chFees(view, scope.feeLo, scope.feeHi, 'denorm', gatedSql)]);
  if (hasSpend) out.push(['Q3', 'spendidx', chFees(view, scope.feeLo, scope.feeHi, 'spendidx', gatedSql)]);
  // Q4
  if (hasUtxo && atTip) out.push(['Q4', 'utxo', top20(view('utxo_at'), zeroCategory)]);
  out.push(['Q4', 'antijoin', top20(chUnspent(view, scope, 'input', gatedSql), zeroCategory)]);
  out.push(['Q4', 'antijoin-hash', `${top20(chUnspentHash(view, scope, 'input', gatedSql), zeroCategory)}${hashJoinSettings}`]);
  if (hasSpend) out.push(['Q4', 'spendidx-hash', `${top20(chUnspentHash(view, scope, 'spendidx', gatedSql), zeroCategory)}${hashJoinSettings}`]);
  if (hasSpend) out.push(['Q4', 'spendidx', top20(chUnspent(view, scope, 'spendidx', gatedSql), zeroCategory)]);
  return out.filter(([query, form]) => only.test(`${query}:${form}`));
};

const dropChCaches = async (client) => {
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

const chRun = async (client, statement, params) => {
  const id = `la-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  const started = performance.now();
  const rows = await client.query(statement, params, {
    log_comment: id,
    max_execution_time: 1800,
    use_query_cache: 0,
  });
  return { id, ms: performance.now() - started, rows };
};

const measureCh = async (client, statement, params) => {
  await chRun(client, statement, params); // warm-up
  const warm = [];
  for (let rep = 0; rep < warmReps; rep += 1) warm.push(await chRun(client, statement, params));
  const cold = [];
  for (let rep = 0; rep < coldReps; rep += 1) {
    await dropChCaches(client);
    cold.push(await chRun(client, statement, params));
  }
  await client.command('SYSTEM FLUSH LOGS');
  const [stats] = await client.query(
    `SELECT median(query_duration_ms) AS server_ms, median(read_rows) AS read_rows,
            median(read_bytes) AS read_bytes, max(memory_usage) AS memory
     FROM system.query_log WHERE type = 'QueryFinish' AND event_date >= yesterday()
       AND log_comment IN {ids:Array(String)}`,
    { ids: warm.map((item) => item.id) }
  );
  const [coldStats] = await client.query(
    `SELECT median(query_duration_ms) AS server_ms FROM system.query_log
     WHERE type = 'QueryFinish' AND event_date >= yesterday() AND log_comment IN {ids:Array(String)}`,
    { ids: cold.map((item) => item.id) }
  );
  return {
    coldMs: cold.length === 0 ? undefined : round(median(cold.map((item) => item.ms))),
    coldServerMs: Number(coldStats?.server_ms ?? 0),
    memoryBytes: Number(stats?.memory ?? 0),
    readBytes: Number(stats?.read_bytes ?? 0),
    readRows: Number(stats?.read_rows ?? 0),
    result: warm[0].rows,
    serverWarmMs: Number(stats?.server_ms ?? 0),
    warmMs: round(median(warm.map((item) => item.ms))),
  };
};

/* ---------------------------------------------------------------- Postgres */

/** v1 acceptance: node_validations (mempool) ∪ block_inclusions of the node's blocks; or blocks <= H. */
const pgAcceptedIds = (scope) =>
  scope.height === undefined
    ? `SELECT transaction_internal_id AS id FROM node_transaction WHERE node_internal_id = ${node}
       UNION SELECT bt.transaction_internal_id FROM block_transaction bt
         JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id AND nb.node_internal_id = ${node}`
    : `SELECT bt.transaction_internal_id AS id FROM block_transaction bt
         JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id AND nb.node_internal_id = ${node}
         JOIN block b ON b.internal_id = bt.block_internal_id WHERE b.height <= ${scope.height}`;

const pgUnspent = (scope) => `WITH acc AS (${pgAcceptedIds(scope)}),
  acc_hash AS (SELECT t.hash FROM transaction t WHERE t.internal_id IN (SELECT id FROM acc))
  SELECT o.token_category, o.value_satoshis FROM output o
  WHERE o.transaction_hash IN (SELECT hash FROM acc_hash)
    AND NOT EXISTS (SELECT 1 FROM input i
                    WHERE i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
                      AND i.transaction_internal_id IN (SELECT id FROM acc))`;

const pgStatements = (scope) => {
  const out = [
    ['Q1', 'pg-antijoin', `SELECT count(*)::text AS n, sum(value_satoshis)::text AS sats FROM (${pgUnspent(scope)}) u`],
    [
      'Q2',
      'pg-blocks',
      `SELECT count(*)::text AS n, sum(o.value_satoshis)::text AS sats FROM output o
       WHERE o.transaction_hash IN (SELECT t.hash FROM transaction t WHERE t.internal_id IN (
         SELECT bt.transaction_internal_id FROM block_transaction bt
           JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id AND nb.node_internal_id = ${node}
           JOIN block b ON b.internal_id = bt.block_internal_id${
             scope.height === undefined ? '' : ` WHERE b.height <= ${scope.height}`
           }))`,
    ],
    [
      'Q3',
      'pg-join',
      `WITH nc AS MATERIALIZED (SELECT t.internal_id, t.hash FROM transaction t WHERE NOT t.is_coinbase AND t.internal_id IN (
         SELECT bt.transaction_internal_id FROM block_transaction bt
           JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id AND nb.node_internal_id = ${node}
           JOIN block b ON b.internal_id = bt.block_internal_id WHERE b.height BETWEEN ${scope.feeLo} AND ${scope.feeHi}))
       SELECT (SELECT count(*) FROM nc)::text AS txs,
         (SELECT sum(o.value_satoshis) FROM input i JOIN nc ON nc.internal_id = i.transaction_internal_id
            JOIN output o ON o.transaction_hash = i.outpoint_transaction_hash AND o.output_index = i.outpoint_index)::text AS sats_in,
         (SELECT sum(o.value_satoshis) FROM output o JOIN nc ON o.transaction_hash = nc.hash)::text AS sats_out,
         ((SELECT sum(o.value_satoshis) FROM input i JOIN nc ON nc.internal_id = i.transaction_internal_id
            JOIN output o ON o.transaction_hash = i.outpoint_transaction_hash AND o.output_index = i.outpoint_index)
          - (SELECT sum(o.value_satoshis) FROM output o JOIN nc ON o.transaction_hash = nc.hash))::text AS fees`,
    ],
    [
      'Q4',
      'pg-antijoin',
      `SELECT coalesce(encode(token_category, 'hex'), '') AS category, count(*)::text AS n, sum(value_satoshis)::text AS sats
       FROM (${pgUnspent(scope)}) u GROUP BY 1 ORDER BY count(*) DESC, category LIMIT 20`,
    ],
  ];
  return out.filter(([query, form]) => only.test(`${query}:${form}`));
};

const pgRun = async (client, statement) => {
  const started = performance.now();
  const result = await client.query(statement);
  return { ms: performance.now() - started, rows: result.rows };
};

const measurePg = async (client, statement) => {
  await pgRun(client, statement); // warm-up
  const warm = [];
  for (let rep = 0; rep < warmReps; rep += 1) warm.push(await pgRun(client, statement));
  const explain = await client.query(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${statement}`);
  const plan = explain.rows[0]['QUERY PLAN'][0];
  const top = plan.Plan;
  return {
    blocksHit: top['Shared Hit Blocks'],
    blocksRead: top['Shared Read Blocks'],
    explainMs: round(plan['Execution Time']),
    result: warm[0].rows,
    tempBlocksWritten: top['Temp Written Blocks'],
    topNode: top['Node Type'],
    warmMs: round(median(warm.map((item) => item.ms))),
  };
};

/* ---------------------------------------------------------------- run */

const pg = new PgClient({ connectionString: pgUrl, statement_timeout: 1_800_000 });
await pg.connect();
await pg.query('SET default_transaction_read_only = on');
const [pgTip] = (
  await pg.query(`SELECT max(b.height) AS h FROM node_block nb JOIN block b ON b.internal_id = nb.block_internal_id
                  WHERE nb.node_internal_id = ${node}`)
).rows;
const pgSettings = (await pg.query(`SELECT name, setting FROM pg_settings
  WHERE name IN ('work_mem', 'shared_buffers', 'max_parallel_workers_per_gather', 'effective_cache_size')`)).rows;

const databases = [
  { client: chClient(utxoDb), hasDenorm: true, hasSpend: false, hasUtxo: true, name: utxoDb },
  { client: chClient(spendDb), hasDenorm: false, hasSpend: true, hasUtxo: false, name: spendDb },
];
for (const database of databases) {
  const [tip] = await database.client.query(
    `SELECT max(height) AS h FROM node_block_at(${visibilityModule.nodeViewArgs})`,
    visibilityModule.nodeViewParams((database.snapshot = await snapshotOf(database.client, database.hasSpend)))
  );
  database.tip = Number(tip.h);
}
const common = Math.min(Number(pgTip.h), ...databases.map((database) => database.tip));
const scopeAt = (tip, height) => ({ feeHi: tip, feeLo: tip - 9_999, height, label: height === undefined ? 'tip' : `H=${height}` });
console.log(
  JSON.stringify({
    common,
    pgSettings,
    pgTip: Number(pgTip.h),
    tips: Object.fromEntries(databases.map((database) => [database.name, database.tip])),
    snapshots: Object.fromEntries(
      databases.map((database) => [
        database.name,
        {
          spend: database.snapshot.spend.toString(),
          tail: database.snapshot.committedTail.length,
          visible: database.snapshot.visible.toString(),
          visible0: database.snapshot.visible0.toString(),
          void: database.snapshot.void.length,
        },
      ])
    ),
  })
);

const results = [];
const emit = (record) => {
  results.push(record);
  console.log(JSON.stringify(record));
};
for (const database of databases) {
  for (const scope of [scopeAt(database.tip), scopeAt(common, common)]) {
    for (const mode of scope.height === undefined ? ['gated', 'base'] : ['gated']) {
      const sql = new sqlModule.Sql(database.snapshot);
      for (const [query, form, statement] of chStatements(database, mode, scope, sql)) {
        const measured = await measureCh(database.client, statement, sql.params);
        emit({ db: database.name, form, mode, query, scope: scope.label, tip: database.tip, ...measured });
      }
    }
  }
}
for (const scope of [scopeAt(Number(pgTip.h)), scopeAt(common, common)]) {
  for (const workMem of ['default', '256MB']) {
    if (workMem !== 'default') await pg.query(`SET work_mem = '${workMem}'`);
    for (const [query, form, statement] of pgStatements(scope)) {
      if (workMem !== 'default' && (query === 'Q2' || query === 'Q3')) continue;
      const measured = await measurePg(pg, statement);
      emit({ db: 'chipnet_pg', form, mode: workMem === 'default' ? 'pg' : `pg work_mem=${workMem}`, query, scope: scope.label, tip: Number(pgTip.h), ...measured });
    }
    if (workMem !== 'default') await pg.query('RESET work_mem');
  }
}

/* ---------------------------------------------------------------- agreement */

const key = (record) => JSON.stringify(record.result);
const groups = new Map();
for (const record of results) {
  // Like with like: same query and scope; at the tip, the same tip height.
  const group = `${record.query} ${record.scope}${record.scope === 'tip' ? `@${record.tip}` : ''}`;
  groups.set(group, [...(groups.get(group) ?? []), record]);
}
let allEqual = true;
for (const [group, records] of groups) {
  const distinct = new Set(records.map(key));
  allEqual &&= distinct.size === 1;
  console.log(
    JSON.stringify({
      agreement: group,
      equal: distinct.size === 1,
      forms: records.map((record) => `${record.db}/${record.form}/${record.mode}`),
      ...(distinct.size === 1 ? {} : { values: records.map((record) => [`${record.db}/${record.form}/${record.mode}`, record.result]) }),
    })
  );
}
await pg.end();
for (const database of databases) await database.client.close();
process.exitCode = allEqual ? 0 : 1;
