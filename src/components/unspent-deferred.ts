/* eslint-disable camelcase, @typescript-eslint/naming-convention, sort-keys */
/**
 * Experiment (E17, `CHAINGRAPH_UNSPENT_TRACKING=deferred-marker|deferred-bitmask`):
 * SQL and pure helpers for deferred unspent tracking. See migration
 * `1791400002000_unspent_deferred` for the schema, the job batch and the
 * query roots.
 *
 * - Ingestion stays append-only: outputs keep a NULL marker / NULL bits
 *   ("unprocessed"); the only write-path additions are the release-event
 *   triggers and an "accepted" event on the agent's re-acceptance paths.
 * - A recurring job (one instance) processes everything behind watermarks.
 *   It only advances to sequence values that are "settled": every
 *   transaction that allocated an id at or below them has finished, so no
 *   row below a watermark can commit later.
 */

export type UnspentDeferredKind = 'array' | 'bitmask' | 'marker';

/**
 * The partial indexes of a deferred mode (created by the agent at start-up if
 * missing, like the per-node indexes of `bitmask`):
 * - NULL (unprocessed) rows, by token category (token outputs only) and by
 *   the 25-byte locking bytecode prefix (as `output_search_index`);
 * - `bitmask` only: rows with any node's bit, by category and by prefix (the
 *   `marker` equivalents, `WHERE … = 0`, come with migration
 *   `1791400000000_unspent_tracking`).
 */
export const deferredIndexDefinitions = (
  kind: UnspentDeferredKind,
  nodeInternalIds: number[] = []
): { [name: string]: string } =>
  kind === 'array'
    ? arrayIndexDefinitions(nodeInternalIds)
    : kind === 'marker'
    ? {
        /* eslint-disable @typescript-eslint/naming-convention */
        output_unspent_null_search_index: /* sql */ `CREATE INDEX output_unspent_null_search_index ON output USING btree (substring(locking_bytecode, 0, 26)) WHERE spent_by_transaction_internal_id IS NULL;`,
        output_unspent_null_token_category_index: /* sql */ `CREATE INDEX output_unspent_null_token_category_index ON output USING btree (token_category) WHERE spent_by_transaction_internal_id IS NULL AND token_category IS NOT NULL;`,
      }
    : {
        output_unspent_bits_null_search_index: /* sql */ `CREATE INDEX output_unspent_bits_null_search_index ON output USING btree (substring(locking_bytecode, 0, 26)) WHERE unspent_node_bits IS NULL;`,
        output_unspent_bits_null_token_category_index: /* sql */ `CREATE INDEX output_unspent_bits_null_token_category_index ON output USING btree (token_category) WHERE unspent_node_bits IS NULL AND token_category IS NOT NULL;`,
        output_unspent_bits_search_index: /* sql */ `CREATE INDEX output_unspent_bits_search_index ON output USING btree (substring(locking_bytecode, 0, 26)) WHERE unspent_node_bits <> 0;`,
        output_unspent_bits_token_category_index: /* sql */ `CREATE INDEX output_unspent_bits_token_category_index ON output USING btree (token_category) WHERE unspent_node_bits <> 0;`,
        /* eslint-enable @typescript-eslint/naming-convention */
      };

/**
 * E17b `deferred-array`: per node, partial B-trees on the category, the
 * category + capability and the 25-byte prefix `WHERE <n> = ANY
 * (unspent_node_ids)`, plus the NULL (unprocessed) token-output index for the
 * backlog correction. Created at start-up for every node, like the bitmask's.
 */
export const arrayIndexDefinitions = (nodeInternalIds: number[]) => ({
  output_unspent_array_null_token_category_index: /* sql */ `CREATE INDEX output_unspent_array_null_token_category_index ON output USING btree (token_category) WHERE unspent_node_ids IS NULL AND token_category IS NOT NULL;`,
  ...Object.fromEntries(
    nodeInternalIds.flatMap((id) => [
      [
        `output_unspent_array_node_${id}_token_category_index`,
        /* sql */ `CREATE INDEX output_unspent_array_node_${id}_token_category_index ON output USING btree (token_category) WHERE ${id} = ANY (unspent_node_ids);`,
      ],
      [
        `output_unspent_array_node_${id}_category_capability_index`,
        /* sql */ `CREATE INDEX output_unspent_array_node_${id}_category_capability_index ON output USING btree (token_category, nonfungible_token_capability) WHERE ${id} = ANY (unspent_node_ids);`,
      ],
      [
        `output_unspent_array_node_${id}_search_index`,
        /* sql */ `CREATE INDEX output_unspent_array_node_${id}_search_index ON output USING btree (substring(locking_bytecode, 0, 26)) WHERE ${id} = ANY (unspent_node_ids);`,
      ],
    ])
  ),
});

/**
 * Names of every index a deferred mode may create (for tests and cleanup).
 */
export const deferredIndexNames = [
  ...Object.keys(deferredIndexDefinitions('marker')),
  ...Object.keys(deferredIndexDefinitions('bitmask')),
];

export const deferredTriggers = [
  ['node_block', 'trigger_unspent_deferred_node_block_delete'],
  ['node_transaction', 'trigger_unspent_deferred_node_transaction_delete'],
] as const;

export const deferredTriggerNames = deferredTriggers.map(([, name]) => name);

/**
 * Statements enabling (deferred modes) or disabling (other modes) the
 * release-event triggers, for triggers whose state differs.
 */
export const configureDeferredTriggersSql = (
  enabled: boolean,
  existingTriggers: { [name: string]: boolean }
) =>
  deferredTriggers
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
 * Re-point the generic query root at the active kind (inlinable: one SQL
 * statement calling the kind's inlinable function).
 */
export const deferredQueryRootSql = (kind: UnspentDeferredKind) => /* sql */ `${
  kind === 'array' ? 'SELECT unspent_deferred_array_build_root();\n' : ''
}
CREATE OR REPLACE FUNCTION unspent_output_deferred (node_name text)
  RETURNS SETOF output LANGUAGE sql STABLE AS $$
  SELECT * FROM unspent_output_deferred_${kind}(node_name)
$$;`;

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

export const initializeSql = /* sql */ `SELECT unspent_deferred_initialize($1::text, $2::bigint, $3::bigint) AS initialized;`;
export const progressSql = /* sql */ `
SELECT input_transaction_internal_id::text AS "inputWatermark",
       node_block_block_internal_id::text AS "blockWatermark",
       consumed_event_id::text AS "consumedEventId"
  FROM unspent_tracking_progress WHERE tracking_kind = $1 AND node_internal_id = 0;`;
export const runBatchSql = /* sql */ `SELECT unspent_deferred_run_batch($1::text, $2::bigint, $3::bigint, $4::integer, $5::integer, $6::integer, $7::integer, $8::bigint, $9::boolean) AS result;`;
/**
 * Backlog seen by the job (for logs and metrics): unprocessed transactions
 * (approximate: ids above the input watermark), unconsumed events and their
 * age.
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
 * A mempool save of a transaction that already existed (the `transaction`
 * upsert returned no row), or a validation recorded for a known transaction:
 * one event per node_transaction row inserted by this transaction (`$1`
 * transaction internal ID, `$2` node IDs).
 */
export const transactionReacceptedEventsSql = /* sql */ `
INSERT INTO unspent_tracking_events (event_kind, node_internal_id, transaction_internal_id)
  SELECT 'accepted', nt.node_internal_id, nt.transaction_internal_id
    FROM node_transaction nt
    WHERE nt.transaction_internal_id = $1::bigint
      AND nt.node_internal_id = ANY ($2::bigint[])
      AND nt.xmin = pg_current_xact_id()::xid;`;

/**
 * Result of one job batch (`unspent_deferred_run_batch`).
 */
export interface DeferredBatchResult {
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
  skippedInputs: number;
  stalledAt: number | null;
  sweep: number;
  uninitialized?: boolean;
  watchAdded: number;
  watchChanged: number;
  watchExpired: number;
}

export const parseBatchResult = (raw: {
  [key: string]: unknown;
}): DeferredBatchResult => {
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
    skippedInputs: num('skippedInputs'),
    stalledAt:
      raw.stalledAt === null || raw.stalledAt === undefined
        ? null
        : Number(raw.stalledAt),
    sweep: num('sweep'),
    ...(raw.uninitialized === true ? { uninitialized: true } : {}),
    watchAdded: num('watchAdded'),
    watchChanged: num('watchChanged'),
    watchExpired: num('watchExpired'),
  };
};

/**
 * Whether a batch did any work (another batch may follow at once).
 */
export const batchDidWork = (result: DeferredBatchResult) =>
  result.inputWatermark !== result.previousInputWatermark ||
  result.blockWatermark !== result.previousBlockWatermark ||
  result.events > 0 ||
  result.watchChanged > 0 ||
  result.sweep > 0;

/**
 * Stall bookkeeping (child-before-parent): the job stops before a
 * transaction with an input whose output row does not exist yet, and retries
 * next pass. Once one stall has lasted `stallMaxMs`, every missing-output
 * input up to the current settled limit is skipped (`skipThrough`, monotonic).
 * Skipping is safe: the missing output, when its transaction commits, has an
 * id above the watermark (every id at or below a settled limit had committed
 * when the batch ran), so it is processed later as a NULL output and
 * recomputed from `spent_by_index`. The skip only guards against inputs whose
 * output never arrives (e.g. test fixtures).
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
// eslint-disable-next-line max-params
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

export const formatBatchLog = (
  kind: UnspentDeferredKind,
  result: DeferredBatchResult,
  ms: number
) =>
  `Unspent deferred job (${kind}): inputs ${result.inputs.toLocaleString()} (watermark ${result.previousInputWatermark.toLocaleString()} -> ${result.inputWatermark.toLocaleString()}), blocks ${
    result.blocks
  } (+${result.blockTransactions.toLocaleString()} earlier txs), events ${
    result.events
  } (${result.eventTransactions.toLocaleString()} txs), watch +${
    result.watchAdded
  }/~${result.watchChanged}/-${result.watchExpired}, sweep ${
    result.sweep
  }; ${result.affected.toLocaleString()} outputs checked (${result.fresh.toLocaleString()} first seen, no probe), ${result.changed.toLocaleString()} changed${
    result.stalledAt === null
      ? ''
      : `; stalled before tx ${result.stalledAt} (output not saved yet)`
  }${
    result.skippedInputs > 0
      ? `; skipped ${result.skippedInputs} input(s) whose output never arrived`
      : ''
  } in ${ms} ms`;
