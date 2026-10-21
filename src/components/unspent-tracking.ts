/* eslint-disable sort-keys, max-params */
/**
 * Stored per-node unspent set (`CHAINGRAPH_UNSPENT_TRACKING`): SQL and pure
 * helpers for the tracking job and its configuration. See migration
 * `1791500000000_unspent_node_ids` for the schema, the job batch and the
 * query root.
 *
 * - Ingestion stays append-only: outputs are inserted with a NULL
 *   `unspent_node_ids` ("not processed yet"); the only write-path additions
 *   are the release-event triggers and an "accepted" event on the agent's
 *   re-acceptance paths.
 * - A recurring job runs one batch per partition (outpoint hash range), each
 *   on its own connection. It only advances to sequence values that are
 *   "settled": every transaction that allocated an id at or below them has
 *   finished, so no row below a watermark can commit later.
 */

/**
 * Locking bytecode length up to which a stored token output is in the
 * covering per-node index (B-tree index tuples must stay under ~2.7 kB).
 */
export const coveringBytecodeMaxBytes = 1000;

/**
 * The per-node partial indexes (created by the agent for every registered
 * node once the initial sync is complete):
 * - `category`: token outputs by (category, capability), covering every
 *   column the usual token queries read (index-only scans), for locking
 *   bytecode up to {@link coveringBytecodeMaxBytes};
 * - `category_other`: every other stored output (no token, or a longer
 *   locking bytecode), by (category, capability), so a category filter is an
 *   index condition on both stored arms of the query root (the outputs
 *   without a token share the NULL key and deduplicate);
 * - `search`: every stored output by the 25-byte locking bytecode prefix (as
 *   `output_search_index`).
 * The predicates are spelled exactly as the query root's arms.
 */
export const nodeIndexDefinitions = (nodeInternalId: number) => ({
  [`output_unspent_node_${nodeInternalId}_category_index`]: /* sql */ `CREATE INDEX output_unspent_node_${nodeInternalId}_category_index ON output USING btree (token_category, nonfungible_token_capability) INCLUDE (transaction_hash, output_index, value_satoshis, fungible_token_amount, nonfungible_token_commitment, locking_bytecode) WHERE ${nodeInternalId} = ANY (unspent_node_ids) AND token_category IS NOT NULL AND octet_length(locking_bytecode) <= ${coveringBytecodeMaxBytes};`,
  [`output_unspent_node_${nodeInternalId}_category_other_index`]: /* sql */ `CREATE INDEX output_unspent_node_${nodeInternalId}_category_other_index ON output USING btree (token_category, nonfungible_token_capability) WHERE ${nodeInternalId} = ANY (unspent_node_ids) AND (token_category IS NULL OR octet_length(locking_bytecode) > ${coveringBytecodeMaxBytes});`,
  [`output_unspent_node_${nodeInternalId}_search_index`]: /* sql */ `CREATE INDEX output_unspent_node_${nodeInternalId}_search_index ON output USING btree (substring(locking_bytecode, 0, 26)) WHERE ${nodeInternalId} = ANY (unspent_node_ids);`,
});

/**
 * The `CREATE INDEX CONCURRENTLY` form of a per-node index definition (the
 * agent builds them while blocks and transactions are being saved).
 */
export const concurrentIndexDefinition = (definition: string) =>
  definition.replace(/^CREATE INDEX /u, 'CREATE INDEX CONCURRENTLY ');

/**
 * Record that a node's three per-node indexes are valid: the query root serves
 * it from the stored set once the events recorded until now are consumed.
 */
export const markNodeIndexedSql = (nodeInternalId: number) =>
  /* sql */ `INSERT INTO unspent_tracking_nodes (node_internal_id, ready_after_event_id) SELECT ${nodeInternalId}, CASE WHEN is_called THEN last_value ELSE 0 END FROM unspent_tracking_events_id_seq ON CONFLICT ON CONSTRAINT unspent_tracking_nodes_pkey DO NOTHING;`;

/**
 * Stop serving a node from the stored set (its per-node indexes are rebuilt).
 */
export const clearNodeIndexedSql = (nodeInternalId: number) =>
  /* sql */ `DELETE FROM unspent_tracking_nodes WHERE node_internal_id = ${nodeInternalId};`;

export const nodeIndexNamePattern = /^output_unspent_node_\d+_/u;

export const trackingTriggers = [
  ['node_block', 'trigger_public_node_block_delete_unspent_tracking'],
  [
    'node_transaction',
    'trigger_public_node_transaction_delete_unspent_tracking',
  ],
] as const;

export const trackingTriggerNames = trackingTriggers.map(([, name]) => name);

/**
 * Statements enabling (tracking on) or disabling (off) the release-event
 * triggers, for triggers whose state differs.
 */
export const configureTrackingTriggersSql = (
  enabled: boolean,
  existingTriggers: { [name: string]: boolean }
) =>
  trackingTriggers
    .filter(
      ([, name]) =>
        name in existingTriggers && existingTriggers[name] !== enabled
    )
    .map(
      ([table, name]) =>
        `ALTER TABLE ${table} ${
          enabled ? 'ENABLE' : 'DISABLE'
        } TRIGGER ${name};`
    );

/**
 * The lowest PostgreSQL version (`server_version_num`) unspent tracking runs
 * on. PostgreSQL 18.0 to 18.5 let a read stream over a temporary table pin
 * every local buffer, so a later statement step that needs one more (the
 * tracking job reads and writes its own temporary tables) fails with "no
 * empty local buffer available"; 18.6 caps the read stream at a quarter of
 * the local buffers (upstream commit ed8050370b7, "Make local buffers pin
 * limit more conservative").
 */
export const minimumServerVersionNum = 180006;
export const serverVersionSql = /* sql */ `SELECT current_setting('server_version_num')::integer AS "versionNum", current_setting('server_version') AS version;`;

/**
 * The error refusing unspent tracking on a server older than
 * {@link minimumServerVersionNum}, or `undefined` when the server is recent
 * enough.
 */
export const serverVersionError = (server: {
  version: string;
  versionNum: number;
}) =>
  server.versionNum >= minimumServerVersionNum
    ? undefined
    : `CHAINGRAPH_UNSPENT_TRACKING=true requires PostgreSQL 18.6 or later, but the server is ${server.version}: PostgreSQL 18.0 to 18.5 can fail the tracking job's temporary-table reads with "no empty local buffer available" (a read-stream bug fixed in 18.6). Upgrade PostgreSQL, or set CHAINGRAPH_UNSPENT_TRACKING=false.`;

export const readSettingsSql = /* sql */ `SELECT read_hashed_max_transactions::text AS "readHashedMaxTransactions", read_probed_max_transactions::text AS "readProbedMaxTransactions", read_stored_max_events::text AS "readStoredMaxEvents" FROM unspent_tracking_settings;`;
export const writeSettingsSql = /* sql */ `UPDATE unspent_tracking_settings SET read_hashed_max_transactions = $1::bigint, read_probed_max_transactions = $2::bigint, read_stored_max_events = $3::bigint;`;
export const buildRootSql = /* sql */ `SELECT unspent_tracking_build_root();`;
export const stopTrackingSql = /* sql */ `SELECT unspent_tracking_stop() AS stopped;`;
/**
 * Advisory lock key serializing the agent's unspent tracking DDL (trigger
 * configuration, root builds, per-node index creation) across agents and
 * concurrent callers; `unspent_tracking_build_root()` takes the same key.
 */
export const configurationLockKey = 1970172787;

/**
 * Settling the watermark limits. The sequences are read first; after a short
 * settle delay the next transaction id is taken (`pg_current_xact_id()` in its own
 * statement, so every transaction that allocated an id at or below the values
 * read has a smaller xid); once the current snapshot's xmin (the oldest xid
 * still running) has passed it, every id at or below the values read has
 * either committed or will never commit. (The settle delay covers the gap between a
 * statement's `nextval` and the assignment of its transaction id.) A
 * snapshot's in-progress list is not enough: it omits running transactions
 * whose xid is at or above the snapshot's xmax. Event ids are settled the same
 * way: an event id is allocated when its row is inserted, so a later id can
 * commit first, and a batch consuming past an uncommitted id would skip it.
 */
export const readSequencesSql = /* sql */ `
SELECT
  (SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM transaction_internal_id_seq)::text AS "transactionLimit",
  (SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM block_internal_id_seq)::text AS "blockLimit",
  (SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM unspent_tracking_events_id_seq)::text AS "eventLimit";`;
export const nextTransactionIdSql = /* sql */ `SELECT pg_current_xact_id()::text AS "nextXid";`;
export const snapshotSettledSql = /* sql */ `SELECT pg_snapshot_xmin(pg_current_snapshot()) > $1::xid8 AS settled;`;

export const initializeSql = /* sql */ `SELECT unspent_tracking_initialize($1::integer, $2::bigint, $3::bigint, $4::bigint) AS initialized;`;
export const repartitionSql = /* sql */ `SELECT unspent_tracking_repartition($1::integer) AS repartitioned;`;
export const progressSql = /* sql */ `
SELECT count(*)::text AS partitions,
       min(partition_count)::text AS "partitionCount",
       min(input_transaction_internal_id)::text AS "inputWatermark",
       max(input_transaction_internal_id)::text AS "maxInputWatermark",
       min(block_internal_id)::text AS "blockWatermark",
       min(consumed_event_id)::text AS "consumedEventId",
       min(backfill_transaction_internal_id)::text AS "backfillWatermark",
       (SELECT backfill_complete FROM unspent_tracking_settings) AS "backfillComplete",
       unspent_tracking_backfill_pending() AS "backfillPending",
       (SELECT rebuild_required FROM unspent_tracking_settings) AS "rebuildRequired",
       unspent_tracking_read_mode() AS "readMode"
  FROM unspent_tracking_progress;`;
export const runBatchSql = /* sql */ `SELECT unspent_tracking_run_batch($1::integer, $2::integer, $3::bigint, $4::bigint, $5::bigint, $6::integer, $7::integer, $8::integer, $9::bigint) AS result;`;
export const watchPassSql = /* sql */ `SELECT unspent_tracking_watch_pass($1::bigint) AS result;`;

/**
 * Result of the once-per-pass watch-set check (`unspent_tracking_watch_pass`):
 * released transactions added, transactions watched, "accepted" events
 * written for re-acceptances, entries expired.
 */
export interface WatchPassResult {
  added: number;
  busy?: boolean;
  expired: number;
  reaccepted: number;
  uninitialized?: boolean;
  watched: number;
}
// eslint-disable-next-line complexity
export const parseWatchPassResult = (raw: {
  [key: string]: unknown;
}): WatchPassResult => ({
  added: Number(raw.added ?? 0),
  ...(raw.busy === true ? { busy: true } : {}),
  expired: Number(raw.expired ?? 0),
  reaccepted: Number(raw.reaccepted ?? 0),
  ...(raw.uninitialized === true ? { uninitialized: true } : {}),
  watched: Number(raw.watched ?? 0),
});
/**
 * Backfill: the preparation (once, REPEATABLE READ, settled transaction /
 * block / event floors), one batch of one partition (REPEATABLE READ), and the
 * reset of a prepared backfill a batch found stale.
 */
export const backfillPrepareSql = /* sql */ `SELECT unspent_tracking_backfill_prepare($1::bigint, $2::bigint, $3::bigint) AS result;`;
export const backfillBatchSql = /* sql */ `SELECT unspent_tracking_backfill_batch($1::integer, $2::integer, $3::integer) AS result;`;
export const backfillResetSql = /* sql */ `SELECT unspent_tracking_backfill_reset() AS reset;`;
/**
 * Keep the events above a backfill's event floor (`$1`) until the backfill is
 * complete; run in its own transaction before the preparation's.
 */
export const backfillRetainEventsSql = /* sql */ `SELECT unspent_tracking_backfill_retain_events($1::bigint);`;
export const backfillStateSql = /* sql */ `SELECT unspent_tracking_backfill_pending() AS pending, EXISTS (SELECT 1 FROM unspent_tracking_backfill) AS prepared;`;
/**
 * Whether the job must backfill whatever CHAINGRAPH_UNSPENT_TRACKING_BACKFILL
 * says: a rebuild after a stop (until it completes, reads use the live
 * predicate, so the rebuild always runs).
 */
export const rebuildRequiredSql = /* sql */ `SELECT rebuild_required AS "rebuildRequired" FROM unspent_tracking_settings;`;
export const deleteConsumedEventsSql = /* sql */ `SELECT unspent_tracking_delete_consumed_events()::text AS deleted;`;
/**
 * Backlog seen by the job (logs, metrics, tests): highest transaction id,
 * unconsumed events and their age.
 */
export const backlogSql = /* sql */ `
SELECT (SELECT COALESCE(max(internal_id), 0) FROM transaction)::text AS "maxTransactionId",
       (SELECT count(*) FROM unspent_tracking_events)::text AS events,
       (SELECT COALESCE(extract(epoch FROM now() - min(created_at)), 0) FROM unspent_tracking_events)::text AS "oldestEventAgeSeconds";`;

/**
 * Re-acceptance events (the agent's known re-acceptance paths), written in
 * the saving transaction.
 *
 * A block save whose block row already existed: one event per node_block row
 * inserted by this transaction (`$1` block hash, `$2` accepting node IDs).
 */
export const blockReacceptedEventsSql = /* sql */ `
INSERT INTO unspent_tracking_events (event_kind, node_internal_id, block_internal_id)
  SELECT 'accepted', nb.node_internal_id, nb.block_internal_id
    FROM block b
    CROSS JOIN LATERAL (
      SELECT node_internal_id, block_internal_id FROM node_block
        WHERE node_block.block_internal_id = b.internal_id
          AND node_block.node_internal_id = ANY ($2::bigint[])
          AND node_block.xmin = pg_current_xact_id()::xid
    ) nb
    WHERE b.hash = $1::bytea AND b.xmin <> pg_current_xact_id()::xid;`;

/**
 * A save of a block that already existed which linked transactions the block
 * was missing (incomplete-block repair): those transactions are now accepted
 * by every node accepting the block, so every node_block row of the block
 * gets an event (`$1` block hash).
 */
export const blockRepairedEventsSql = /* sql */ `
INSERT INTO unspent_tracking_events (event_kind, node_internal_id, block_internal_id)
  SELECT 'accepted', nb.node_internal_id, nb.block_internal_id
    FROM block b
    CROSS JOIN LATERAL (
      SELECT node_internal_id, block_internal_id FROM node_block
        WHERE node_block.block_internal_id = b.internal_id
    ) nb
    WHERE b.hash = $1::bytea;`;

/**
 * Headers acceptance (the blocks exist already): `insertNodeBlocks` is the
 * agent's node_block insert (returning `block_internal_id`), wrapped so the
 * inserted rows also get an "accepted" event.
 */
export const headersAcceptedEventsSql = (
  nodeInternalId: number,
  insertNodeBlocks: string
) => /* sql */ `
WITH inserted_node_blocks AS (${insertNodeBlocks}
), accepted_events AS (
  INSERT INTO unspent_tracking_events (event_kind, node_internal_id, block_internal_id)
    SELECT 'accepted', ${nodeInternalId}::bigint, "blockInternalId" FROM inserted_node_blocks
)
SELECT "blockInternalId" FROM inserted_node_blocks;`;

/**
 * A mempool save of a transaction that already existed, or a validation
 * recorded for a known transaction: one event per node_transaction row
 * inserted by this transaction (`$1` transaction internal ID, `$2` node IDs).
 */
export const transactionReacceptedEventsSql = /* sql */ `
INSERT INTO unspent_tracking_events (event_kind, node_internal_id, transaction_internal_id)
  SELECT 'accepted', nt.node_internal_id, nt.transaction_internal_id
    FROM node_transaction nt
    WHERE nt.transaction_internal_id = $1::bigint
      AND nt.node_internal_id = ANY ($2::bigint[])
      AND nt.xmin = pg_current_xact_id()::xid;`;

/**
 * Result of one job batch (`unspent_tracking_run_batch`).
 */
export interface BatchResult {
  affected: number;
  blockTransactions: number;
  blockWatermark: number;
  blocks: number;
  busy?: boolean;
  changed: number;
  events: number;
  eventTransactions: number;
  fresh: number;
  inputs: number;
  inputWatermark: number;
  previousBlockWatermark: number;
  previousInputWatermark: number;
  repartition?: boolean;
  skippedInputs: number;
  stalledAt: number | null;
  uninitialized?: boolean;
}

// eslint-disable-next-line complexity
export const parseBatchResult = (raw: {
  [key: string]: unknown;
}): BatchResult => {
  const num = (key: string) => Number(raw[key] ?? 0);
  return {
    affected: num('affected'),
    blockTransactions: num('blockTransactions'),
    blockWatermark: num('blockWatermark'),
    blocks: num('blocks'),
    ...(raw.busy === true ? { busy: true } : {}),
    changed: num('changed'),
    events: num('events'),
    eventTransactions: num('eventTransactions'),
    fresh: num('fresh'),
    inputs: num('inputs'),
    inputWatermark: num('inputWatermark'),
    previousBlockWatermark: num('previousBlockWatermark'),
    previousInputWatermark: num('previousInputWatermark'),
    ...(raw.repartition === true ? { repartition: true } : {}),
    skippedInputs: num('skippedInputs'),
    stalledAt:
      raw.stalledAt === null || raw.stalledAt === undefined
        ? null
        : Number(raw.stalledAt),
    ...(raw.uninitialized === true ? { uninitialized: true } : {}),
  };
};

/**
 * Whether a batch did any work (another batch may follow at once).
 */
export const batchDidWork = (result: BatchResult) =>
  result.inputWatermark !== result.previousInputWatermark ||
  result.blockWatermark !== result.previousBlockWatermark ||
  result.events > 0;

/**
 * Whether a partition reached the settled limits (nothing left but events
 * that arrive later).
 */
export const batchReachedLimits = (
  result: BatchResult,
  limits: { blockLimit: number; transactionLimit: number }
) =>
  result.inputWatermark >= limits.transactionLimit &&
  result.blockWatermark >= limits.blockLimit;

/**
 * Whether a partition's tracking lets the backfill run: it reached the
 * settled limits, or it has nothing left to do now but wait for a missing
 * parent output (a child saved before its parent: stalled, up to
 * CHAINGRAPH_UNSPENT_TRACKING_STALL_MAX_MS per transaction). A partition still
 * working through a backlog (the pass ran out of time) holds the backfill
 * back, so catch-ups keep priority.
 *
 * Stalls do not hold it back: the backfill only recomputes pre-tracking
 * outputs from the current state, which does not depend on how far tracking
 * has walked (later spends and acceptance changes are re-applied by tracking,
 * recomputing from scratch), and a mempool child-before-parent resolves only
 * when the parent arrives or the stall cap passes. Gating on stalls would let
 * one stalled partition (e.g. after a restart reloads the mempool) block the
 * whole backfill for the stall cap, again for every new stalled transaction.
 */
export const readyForBackfill = (partition: {
  caughtUp: boolean;
  last: BatchResult | undefined;
  settled: boolean;
}) =>
  partition.caughtUp ||
  (partition.settled &&
    partition.last?.stalledAt !== null &&
    partition.last?.stalledAt !== undefined);

/**
 * Stall bookkeeping (child-before-parent): a partition stops before a
 * transaction with an input whose output row does not exist yet, and retries
 * next pass. Once one stall has lasted `stallMaxMs`, every missing-output
 * input up to the current settled limit is skipped (`skipThrough`,
 * monotonic). Skipping is safe: the missing output, when its transaction
 * commits, has an id above the watermark, so it is processed later as a NULL
 * output and recomputed from `spent_by_index` (the skipped input is recorded,
 * so the output never takes the fast path). The skip only guards against
 * inputs whose output never arrives.
 */
export interface StallState {
  since: number;
  transactionInternalId: number;
}
export const nextStallState = (
  previous: StallState | undefined,
  stalledAt: number | null,
  now: number
): StallState | undefined => {
  if (stalledAt === null) {
    return undefined;
  }
  if (previous?.transactionInternalId === stalledAt) {
    return previous;
  }
  return { since: now, transactionInternalId: stalledAt };
};
export const nextSkipThrough = (
  previousSkipThrough: number,
  stall: StallState | undefined,
  now: number,
  stallMaxMs: number,
  settledTransactionLimit: number
) =>
  stall !== undefined && now - stall.since >= stallMaxMs
    ? Math.max(previousSkipThrough, settledTransactionLimit)
    : previousSkipThrough;

/**
 * Sequence values whose allocating transactions have all finished: the job
 * walks transactions and blocks and consumes events up to them.
 */
export interface SettledLimits {
  blockLimit: number;
  eventLimit: number;
  transactionLimit: number;
}

/**
 * Settling candidate: sequence values read at `readAt`, then (after the
 * settle delay) the next xid; usable once every xid up to it has finished.
 */
export interface SettleCandidate {
  blockLimit: number;
  eventLimit: number;
  nextXid?: string;
  readAt: number;
  transactionLimit: number;
}

/**
 * The partition of an outpoint transaction hash (hex), as the job computes it
 * in SQL: `((get_byte(hash, 0) * partitionCount) >> 8)`.
 */
export const partitionOf = (hashHex: string, partitionCount: number) =>
  // eslint-disable-next-line no-bitwise, @typescript-eslint/no-magic-numbers
  (parseInt(hashHex.slice(0, 2), 16) * partitionCount) >> 8;

export const formatBatchLog = (
  partition: number,
  partitionCount: number,
  result: BatchResult,
  ms: number
) =>
  `Unspent tracking job [${
    partition + 1
  }/${partitionCount}]: inputs ${result.inputs.toLocaleString()} (watermark ${result.previousInputWatermark.toLocaleString()} -> ${result.inputWatermark.toLocaleString()}), blocks ${
    result.blocks
  } (+${result.blockTransactions.toLocaleString()} earlier txs), events ${
    result.events
  } (${result.eventTransactions.toLocaleString()} txs); ${result.affected.toLocaleString()} outputs checked (${result.fresh.toLocaleString()} first seen, no probe), ${result.changed.toLocaleString()} changed${
    result.stalledAt === null
      ? ''
      : `; stalled before tx ${result.stalledAt} (output not saved yet)`
  }${
    result.skippedInputs > 0
      ? `; skipped ${result.skippedInputs} input(s) whose output never arrived`
      : ''
  } in ${ms} ms`;
