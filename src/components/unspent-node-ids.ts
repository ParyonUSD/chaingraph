/* eslint-disable sort-keys, max-params */
/**
 * Stored per-node unspent set (`CHAINGRAPH_UNSPENT_NODE_IDS`): SQL and pure
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
 * - `category_long`: the remaining token outputs, by category;
 * - `search`: every stored output by the 25-byte locking bytecode prefix (as
 *   `output_search_index`).
 */
export const nodeIndexDefinitions = (nodeInternalId: number) => ({
  [`output_unspent_node_${nodeInternalId}_category_index`]: /* sql */ `CREATE INDEX output_unspent_node_${nodeInternalId}_category_index ON output USING btree (token_category, nonfungible_token_capability) INCLUDE (transaction_hash, output_index, value_satoshis, fungible_token_amount, nonfungible_token_commitment, locking_bytecode) WHERE ${nodeInternalId} = ANY (unspent_node_ids) AND token_category IS NOT NULL AND octet_length(locking_bytecode) <= ${coveringBytecodeMaxBytes};`,
  [`output_unspent_node_${nodeInternalId}_category_long_index`]: /* sql */ `CREATE INDEX output_unspent_node_${nodeInternalId}_category_long_index ON output USING btree (token_category, nonfungible_token_capability) WHERE ${nodeInternalId} = ANY (unspent_node_ids) AND token_category IS NOT NULL AND octet_length(locking_bytecode) > ${coveringBytecodeMaxBytes};`,
  [`output_unspent_node_${nodeInternalId}_search_index`]: /* sql */ `CREATE INDEX output_unspent_node_${nodeInternalId}_search_index ON output USING btree (substring(locking_bytecode, 0, 26)) WHERE ${nodeInternalId} = ANY (unspent_node_ids);`,
});

export const nodeIndexNamePattern = /^output_unspent_node_\d+_/u;

export const trackingTriggers = [
  ['node_block', 'trigger_unspent_tracking_node_block_delete'],
  ['node_transaction', 'trigger_unspent_tracking_node_transaction_delete'],
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

export const readSettingsSql = /* sql */ `SELECT hash_max_transactions::text AS "hashMaxTransactions", fallback_transactions::text AS "fallbackTransactions", fallback_events::text AS "fallbackEvents" FROM unspent_tracking_settings;`;
export const writeSettingsSql = /* sql */ `UPDATE unspent_tracking_settings SET hash_max_transactions = $1::bigint, fallback_transactions = $2::bigint, fallback_events = $3::bigint;`;
export const buildRootSql = /* sql */ `SELECT unspent_node_ids_build_root();`;

/**
 * Settling the watermark limits. The sequences are read first; after a short
 * grace the next transaction id is taken (`pg_current_xact_id()` in its own
 * statement, so every transaction that allocated an id at or below the values
 * read has a smaller xid); once the current snapshot's xmin (the oldest xid
 * still running) has passed it, every id at or below the values read has
 * either committed or will never commit. (The grace covers the gap between a
 * statement's `nextval` and the assignment of its transaction id.) A
 * snapshot's in-progress list is not enough: it omits running transactions
 * whose xid is at or above the snapshot's xmax.
 */
export const readSequencesSql = /* sql */ `
SELECT
  (SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM transaction_internal_id_seq)::text AS "transactionLimit",
  (SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM block_internal_id_seq)::text AS "blockLimit";`;
export const nextTransactionIdSql = /* sql */ `SELECT pg_current_xact_id()::text AS "nextXid";`;
export const snapshotSettledSql = /* sql */ `SELECT pg_snapshot_xmin(pg_current_snapshot()) > $1::xid8 AS settled;`;

export const initializeSql = /* sql */ `SELECT unspent_node_ids_initialize($1::integer, $2::bigint, $3::bigint) AS initialized;`;
export const repartitionSql = /* sql */ `SELECT unspent_node_ids_repartition($1::integer) AS repartitioned;`;
export const progressSql = /* sql */ `
SELECT count(*)::text AS partitions,
       min(partition_count)::text AS "partitionCount",
       min(input_transaction_internal_id)::text AS "inputWatermark",
       max(input_transaction_internal_id)::text AS "maxInputWatermark",
       min(block_internal_id)::text AS "blockWatermark",
       min(consumed_event_id)::text AS "consumedEventId",
       min(backfill_transaction_internal_id)::text AS "backfillWatermark",
       (SELECT backfill_complete FROM unspent_tracking_settings) AS "backfillComplete"
  FROM unspent_tracking_progress;`;
export const runBatchSql = /* sql */ `SELECT unspent_node_ids_run_batch($1::integer, $2::integer, $3::bigint, $4::bigint, $5::integer, $6::integer, $7::integer, $8::bigint, $9::boolean) AS result;`;
export const backfillBatchSql = /* sql */ `SELECT unspent_node_ids_backfill_batch($1::integer, $2::integer, $3::integer) AS result;`;
export const deleteConsumedEventsSql = /* sql */ `SELECT unspent_node_ids_delete_consumed_events()::text AS deleted;`;
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
 * Result of one job batch (`unspent_node_ids_run_batch`).
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
  watchAdded: number;
  watchChanged: number;
  watchExpired: number;
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
    watchAdded: num('watchAdded'),
    watchChanged: num('watchChanged'),
    watchExpired: num('watchExpired'),
  };
};

/**
 * Whether a batch did any work (another batch may follow at once).
 */
export const batchDidWork = (result: BatchResult) =>
  result.inputWatermark !== result.previousInputWatermark ||
  result.blockWatermark !== result.previousBlockWatermark ||
  result.events > 0 ||
  result.watchChanged > 0;

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
 * Settling candidate: sequence values read at `readAt`, then (after the
 * grace) the next xid; usable once every xid up to it has finished.
 */
export interface SettleCandidate {
  blockLimit: number;
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
  } (${result.eventTransactions.toLocaleString()} txs), watch +${
    result.watchAdded
  }/~${result.watchChanged}/-${
    result.watchExpired
  }; ${result.affected.toLocaleString()} outputs checked (${result.fresh.toLocaleString()} first seen, no probe), ${result.changed.toLocaleString()} changed${
    result.stalledAt === null
      ? ''
      : `; stalled before tx ${result.stalledAt} (output not saved yet)`
  }${
    result.skippedInputs > 0
      ? `; skipped ${result.skippedInputs} input(s) whose output never arrived`
      : ''
  } in ${ms} ms`;
