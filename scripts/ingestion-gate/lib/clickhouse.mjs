/**
 * ClickHouse backend of the ingestion gate (`--store clickhouse`): the same
 * surface as `postgres.mjs` (see `storeBackend` there).
 *
 * - The schema is the DDL of the agent under test (`<agent>/src/store/clickhouse/ddl`),
 *   applied by the agent's compiled `build/store/clickhouse/ddl-apply.js`.
 * - Every correctness read goes through the agent's compiled ClickHouse
 *   checker or the pinned gated views (`*_at`, one `readSnapshot` per read),
 *   so the clock stops only when a save is committed AND published.
 * - WAL bytes are replaced by server-side write metrics from
 *   `system.part_log`, `system.events`, `system.parts`, `system.merges` and
 *   `system.query_log` (see `writeMetricsStart` / `writeMetricsSince`).
 *
 * Point it at a private server (local docker `clickhouse/clickhouse-server:26.8`):
 * the `system.events` deltas are server-wide.
 */
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Hex hashes per array query parameter, as the store's `defaultLookupChunkSize`
 * (`src/store/clickhouse/clickhouse-store.ts`): 1,000 × ~67 bytes stays under
 * the server's `http_max_field_value_size` (128 KiB). The catch-up scenario
 * reads 10,000 block hashes at once ("Field value too long" in one parameter).
 */
export const hashChunkSize = 1_000;

/** `list` split into consecutive slices of at most `size` (none for an empty list). */
export const chunked = (list, size = hashChunkSize) =>
  Array.from({ length: Math.ceil(list.length / size) }, (_, index) => list.slice(index * size, (index + 1) * size));

/** Sum of `read(chunk)` over the chunks of `hashes`, one chunk at a time. */
const sumOverChunks = async (hashes, read) => {
  let total = 0;
  for (const chunk of chunked(hashes)) total += await read(chunk);
  return total;
};

/** Agent-compiled modules (checker, views, DDL) of the agent under test. */
const loadModules = async (agentDirectory) => {
  const load = (path) => import(pathToFileURL(join(agentDirectory, 'build/store/clickhouse', path)).href);
  try {
    const [client, checker, ddl, visibility] = await Promise.all([
      load('client.js'),
      load('checker.js'),
      load('ddl-apply.js'),
      load('visibility.js'),
    ]);
    return { checker, client, ddl, visibility };
  } catch (error) {
    throw new Error(`${agentDirectory} has no compiled ClickHouse store (build/store/clickhouse/{client,checker,ddl-apply,visibility}.js) – build an agent with WP5: ${error.message}`);
  }
};

/** `system.events` counters read before/after a measured window (names checked on 26.8). */
export const eventCounters = [
  'InsertedBytes',
  'InsertedRows',
  'InsertQuery',
  'DelayedInserts',
  'DelayedInsertsMilliseconds',
  'RejectedInserts',
  'DuplicatedInsertedBlocks',
  'MergedRows',
  'MergedUncompressedBytes',
  'MergeTotalMilliseconds',
];

export const metricQueries = {
  eventSnapshot: `SELECT name, toString(value) AS value FROM system.events
    WHERE has({names:Array(String)}, name)
    SETTINGS system_events_show_zero_values = 1`,
  /** Compressed bytes written by inserts (NewPart) and by merges (MergeParts), parts created, merge time. */
  partLog: `SELECT
      toString(sumIf(size_in_bytes, event_type = 'NewPart')) AS new_part_bytes,
      toString(countIf(event_type = 'NewPart')) AS new_parts,
      toString(sumIf(size_in_bytes, event_type = 'MergeParts')) AS merge_bytes,
      toString(countIf(event_type = 'MergeParts')) AS merges,
      toString(sumIf(duration_ms, event_type = 'MergeParts')) AS merge_ms
    FROM system.part_log
    WHERE database = {database:String} AND event_time_microseconds >= {since:DateTime64(6)}`,
  partLogByTable: `SELECT table,
      toString(countIf(event_type = 'NewPart')) AS new_parts,
      toString(sumIf(size_in_bytes, event_type = 'NewPart')) AS new_part_bytes,
      toString(sumIf(size_in_bytes, event_type = 'MergeParts')) AS merge_bytes
    FROM system.part_log
    WHERE database = {database:String} AND event_time_microseconds >= {since:DateTime64(6)}
    GROUP BY table ORDER BY table`,
  /** Active parts of the busiest (table, partition), against parts_to_delay_insert. */
  maxActiveParts: `SELECT toString(max(parts)) AS parts FROM (
      SELECT count() AS parts FROM system.parts
      WHERE database = {database:String} AND active
      GROUP BY table, partition_id)`,
  /** Inactive parts (merged away or replaced, waiting for cleanup) of the database: count and bytes on disk. */
  inactiveParts: `SELECT toString(count()) AS parts, toString(sum(bytes_on_disk)) AS bytes FROM system.parts
      WHERE database = {database:String} AND NOT active`,
  /** Peak server memory of one INSERT (the Postgres "~3 GB per block" note). */
  maxInsertMemory: `SELECT toString(max(memory_usage)) AS bytes, toString(count()) AS inserts
    FROM system.query_log
    WHERE type = 'QueryFinish' AND query_kind = 'Insert'
      AND current_database = {database:String}
      AND event_time_microseconds >= {since:DateTime64(6)}`,
  runningMerges: `SELECT toString(count()) AS merges FROM system.merges WHERE database = {database:String}`,
  serverNow: `SELECT toString(now64(6)) AS now`,
};

const adminServer = (baseUrl) => ({ url: baseUrl });

/** A session on `databaseName`: client, compiled checker and the pinned-view helpers. */
export const openSession = async ({ agentDirectory, baseUrl, databaseName }) => {
  const modules = await loadModules(agentDirectory);
  const client = new modules.client.ClickHouseClient({ database: databaseName, password: '', requestTimeoutMs: 600_000, url: baseUrl, username: '' });
  const checker = modules.checker.createClickHouseChecker(client, databaseName);
  return { baseUrl, checker, client, databaseName, kind: 'clickhouse', modules };
};

export const closeSession = async (session) => session.client.close();

/** Drop, create and apply the agent's DDL; returns a session. */
export const recreateDatabase = async ({ agentDirectory, baseUrl, databaseName }) => {
  const modules = await loadModules(agentDirectory);
  await modules.ddl.applyClickHouseDdl(adminServer(baseUrl), databaseName, { recreate: true });
  return openSession({ agentDirectory, baseUrl, databaseName });
};

export const dropDatabase = async ({ agentDirectory, baseUrl, databaseName }) => {
  const modules = await loadModules(agentDirectory);
  await modules.ddl.dropClickHouseDatabase(adminServer(baseUrl), databaseName);
};

/**
 * The gate's fixtures spend outpoints that never exist (the base chain and the
 * first block of every sequence). The store waits `pendingSpendTimeoutMs`
 * (default 60 s) for such parents before writing the inputs with a stand-in,
 * holding the node's watermark meanwhile: every scenario would include a
 * 60 s wait. As in the e2e harness the gate sets 1 ms (override with
 * INGESTION_GATE_CH_PENDING_SPEND_TIMEOUT_MS). Spends of blocks the gate does
 * provide (burst, catch-up, re-org, concurrent sequences) still resolve from
 * the output registry or the store as on mainnet.
 */
const pendingSpendTimeoutMs = process.env.INGESTION_GATE_CH_PENDING_SPEND_TIMEOUT_MS ?? '1';

/** Agent environment for this backend (the agent's config still requires a Postgres string). */
export const agentEnvironment = ({ baseUrl, databaseName }) => ({
  CHAINGRAPH_CLICKHOUSE_DATABASE: databaseName,
  CHAINGRAPH_CLICKHOUSE_PENDING_SPEND_TIMEOUT_MS: pendingSpendTimeoutMs,
  CHAINGRAPH_CLICKHOUSE_URL: baseUrl,
  CHAINGRAPH_POSTGRES_CONNECTION_STRING: 'postgres://unused:unused@127.0.0.1:1/unused',
  CHAINGRAPH_STORE: 'clickhouse',
});

/** Pinned parameters for node-agnostic views (`*_at(visible0, tail, fence, void)`). */
const agnosticParams = async (session) => {
  const { visibility } = session.modules;
  const snapshot = await visibility.readSnapshot(session.client, visibility.nodeAgnosticId);
  return visibility.agnosticViewParams(snapshot);
};

/** The agent's own pinned-view call text (`visibility.pinnedView`), so the gate follows its view signature. */
const agnosticView = (session, name) => session.modules.visibility.pinnedView(`${name}_at`);

/** Rows of a node-agnostic table visible through its gated view. */
export const countRows = async (session, table) => {
  const params = await agnosticParams(session);
  const [row] = await session.client.query(`SELECT toString(count()) AS c FROM ${agnosticView(session, table)}`, params);
  return Number(row.c);
};

/**
 * Blocks among `blockHashes` accepted by `nodeName` (pinned `node_block_at`,
 * via the checker), summed over chunks of `hashChunkSize` hashes (one
 * snapshot per chunk; the gate polls until the count is complete).
 */
export const acceptedBlockCount = async (session, nodeName, blockHashes) =>
  sumOverChunks(blockHashes, (chunk) => session.checker.acceptedBlockCount(nodeName, chunk));

/**
 * Linked transactions of the given blocks, one pinned snapshot for the whole
 * set, summed over chunks of `hashChunkSize` hashes (the hashes are distinct
 * blocks, so the chunk counts add up).
 */
export const blockTransactionCount = async (session, blockHashes) => {
  const params = await agnosticParams(session);
  return sumOverChunks(blockHashes, async (chunk) => {
    const [row] = await session.client.query(
      `SELECT toString(count()) AS c FROM ${agnosticView(session, 'block_transaction')}
       WHERE block_internal_id IN (SELECT internal_id FROM ${agnosticView(session, 'block')}
                                   WHERE has(arrayMap(h -> toFixedString(unhex(h), 32), {hashes:Array(String)}), hash))`,
      { ...params, hashes: chunk }
    );
    return Number(row.c);
  });
};

/** Accepted blocks of a node: `{ height, hash }`, by height. */
export const acceptedChain = async (session, nodeName) =>
  (await session.checker.acceptedBlocks(nodeName)).map(({ hash, height }) => ({ hash, height }));

/** Number of accepted blocks of a node (pinned `node_block_at` count). */
export const nodeBlockCount = async (session, nodeName) => {
  const nodeId = await session.checker.nodeInternalId(nodeName);
  if (nodeId === undefined) return 0;
  const { visibility } = session.modules;
  const snapshot = await visibility.readSnapshot(session.client, nodeId);
  const [row] = await session.client.query(
    `SELECT toString(count()) AS c FROM ${visibility.pinnedView('node_block_at')}`,
    visibility.nodeViewParams(snapshot)
  );
  return Number(row.c);
};

/** Mempool rows among `transactionHashes` (distinct hashes, summed over chunks). */
export const mempoolRowCount = async (session, nodeName, transactionHashes) =>
  sumOverChunks(transactionHashes, async (chunk) => (await session.checker.mempoolMembership(nodeName, chunk)).size);

/** Distinct `transactionHashes` archived in the node's history (summed over chunks of distinct hashes). */
export const historyNodeCount = async (session, nodeName, transactionHashes) =>
  sumOverChunks(transactionHashes, async (chunk) => new Set((await session.checker.transactionHistory(nodeName, chunk)).map((row) => row.hash)).size);

/** Per node: mempool transactions confirmed in a block the same node accepts. */
export const confirmedButInMempoolCount = async (session, nodeName) => (await session.checker.confirmedButInMempool(nodeName)).length;

const eventValues = async (client) =>
  Object.fromEntries((await client.query(metricQueries.eventSnapshot, { names: eventCounters })).map((row) => [row.name, Number(row.value)]));

/**
 * Start a measured window: server time, event counters, and a 0.5 s sampler of
 * the busiest (table, partition)'s active part count and the database's
 * inactive parts (count and bytes on disk).
 */
export const writeMetricsStart = async (session) => {
  const { client, databaseName } = session;
  const [{ now: since }] = await client.query(metricQueries.serverNow);
  const events = await eventValues(client);
  const sampler = { maxActiveParts: 0, maxInactiveBytes: 0, maxInactiveParts: 0, running: true };
  const sample = async () => {
    while (sampler.running) {
      try {
        const [row] = await client.query(metricQueries.maxActiveParts, { database: databaseName });
        sampler.maxActiveParts = Math.max(sampler.maxActiveParts, Number(row.parts));
        const [inactive] = await client.query(metricQueries.inactiveParts, { database: databaseName });
        sampler.maxInactiveParts = Math.max(sampler.maxInactiveParts, Number(inactive.parts));
        sampler.maxInactiveBytes = Math.max(sampler.maxInactiveBytes, Number(inactive.bytes));
      } catch {
        // sampling is best-effort
      }
      await sleep(500);
    }
  };
  sampler.done = sample();
  return { events, sampler, since };
};

/**
 * End a window: `SYSTEM FLUSH LOGS`, then part_log / query_log totals since
 * the start, event deltas, the sampled part peak, and the time until no merge
 * of this database is running (`quiesceSeconds`, capped at 120 s).
 */
export const writeMetricsSince = async (session, start) => {
  const { client, databaseName } = session;
  start.sampler.running = false;
  await start.sampler.done;
  const params = { database: databaseName, since: start.since };
  const [finalSample] = await client.query(metricQueries.maxActiveParts, { database: databaseName });
  const maxActiveParts = Math.max(start.sampler.maxActiveParts, Number(finalSample.parts));
  const quiesceStarted = Date.now();
  for (;;) {
    const [{ merges }] = await client.query(metricQueries.runningMerges, { database: databaseName });
    if (Number(merges) === 0 || Date.now() - quiesceStarted > 120_000) break;
    await sleep(100);
  }
  const quiesceSeconds = (Date.now() - quiesceStarted) / 1000;
  await client.command('SYSTEM FLUSH LOGS');
  const events = await eventValues(client);
  const [partLog] = await client.query(metricQueries.partLog, params);
  const byTable = await client.query(metricQueries.partLogByTable, params);
  const [insertMemory] = await client.query(metricQueries.maxInsertMemory, params);
  const delta = Object.fromEntries(eventCounters.map((name) => [name, (events[name] ?? 0) - (start.events[name] ?? 0)]));
  return {
    bytesWritten: Number(partLog.new_part_bytes),
    clickhouse: {
      eventDeltas: delta,
      insertQueries: Number(insertMemory.inserts),
      partLogByTable: byTable.map((row) => ({ mergeBytes: Number(row.merge_bytes), newPartBytes: Number(row.new_part_bytes), newParts: Number(row.new_parts), table: row.table })),
    },
    delayedInserts: delta.DelayedInserts,
    maxActiveParts,
    /** Peak inactive parts / bytes of the database while the window ran (sampled every 0.5 s). */
    maxInactiveBytes: start.sampler.maxInactiveBytes,
    maxInactiveParts: start.sampler.maxInactiveParts,
    maxInsertMemoryBytes: Number(insertMemory.bytes),
    mergeBytesWritten: Number(partLog.merge_bytes),
    merges: Number(partLog.merges),
    mergeSeconds: Number(partLog.merge_ms) / 1000,
    partsCreated: Number(partLog.new_parts),
    quiesceSeconds,
    rejectedInserts: delta.RejectedInserts,
  };
};
