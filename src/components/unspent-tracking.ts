/**
 * Experiment (`CHAINGRAPH_UNSPENT_TRACKING`): SQL for maintaining a stored
 * "unspent" read model during ingestion. See migration
 * `1791400000000_unspent_tracking` for the schema and the semantics.
 *
 * - `off`: no extra SQL; statements are byte-identical to the untracked agent.
 * - `marker`: new outputs get `spent_by_transaction_internal_id = 0`; each
 *   block's DB transaction sets the spender of every outpoint its inputs spend
 *   (one batched `UPDATE … FROM (VALUES …)` per block).
 * - `settable`: new outputs get a `unspent_output_set` row (in the same
 *   statement that inserts them); each block's DB transaction deletes the rows
 *   of every outpoint its inputs spend (one batched `DELETE … USING (VALUES …)`).
 *
 * In both modes, releasing spends when acceptance rows are deleted is done by
 * statement-level triggers (enabled for the configured mode at start-up).
 */
import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../types/chaingraph.js';

export const unspentTrackingModes = ['off', 'marker', 'settable'] as const;
export type UnspentTrackingMode = (typeof unspentTrackingModes)[number];

export interface Spend {
  outpointIndex: number;
  outpointTransactionHash: string;
  spenderHash: string;
}

/**
 * Every outpoint spent by the given transactions (coinbase inputs excluded).
 */
export const collectSpends = (transactions: ChaingraphTransaction[]) =>
  transactions.reduce<Spend[]>((spends, transaction) => {
    if (transaction.isCoinbase) {
      return spends;
    }
    transaction.inputs.forEach((input) => {
      spends.push({
        outpointIndex: input.outpointIndex,
        outpointTransactionHash: input.outpointTransactionHash,
        spenderHash: transaction.hash,
      });
    });
    return spends;
  }, []);

export const collectBlockSpends = (block: ChaingraphBlock) =>
  collectSpends(block.transactions);

/**
 * The value written to `output.spent_by_transaction_internal_id` for new
 * outputs, or `undefined` if the column is not written (it then stays NULL).
 */
export const newOutputMarkerValue = (mode: UnspentTrackingMode) =>
  mode === 'marker' ? '0' : undefined;

/**
 * Column-list and select-list suffixes for `INSERT INTO output`.
 */
export const outputMarkerInsertParts = (mode: UnspentTrackingMode) => {
  const value = newOutputMarkerValue(mode);
  return value === undefined
    ? { column: '', value: '' }
    : {
        column: ', spent_by_transaction_internal_id',
        value: `, ${value}::bigint`,
      };
};

/**
 * A CTE (with leading comma) inserting `unspent_output_set` rows for the
 * outputs of newly saved transactions, or `''` unless the mode is `settable`.
 * @param mode - the tracking mode
 * @param outputSource - relation with `transaction_hash`, `output_index`,
 * `token_category`, `locking_bytecode`
 * @param newTransactions - relation of newly saved transactions (`hash`)
 */
export const unspentSetInsertCte = (
  mode: UnspentTrackingMode,
  outputSource: string,
  newTransactions: string
) =>
  mode === 'settable'
    ? /* sql */ `,
newly_unspent_outputs AS (
  INSERT INTO unspent_output_set (transaction_hash, output_index, token_category, locking_bytecode_prefix)
    SELECT transaction_hash, output_index, token_category::bytea, substring(locking_bytecode::bytea, 0, 26) FROM ${outputSource}
    WHERE transaction_hash IN (SELECT hash FROM ${newTransactions})
    ON CONFLICT ON CONSTRAINT unspent_output_set_pkey DO NOTHING
)`
    : '';

const bytea = (hex: string) => `'\\x${hex}'::bytea`;

/**
 * `marker` mode, `sql` write path: one statement setting the spender of every
 * spent outpoint. The spender's internal_id is resolved by hash (the
 * transaction rows were inserted earlier in the same DB transaction). Rows
 * already pointing at that spender are not rewritten. Returns `undefined` if
 * there is nothing to mark.
 */
export const buildMarkSpentOutputsSql = (spends: Spend[]) =>
  spends.length === 0
    ? undefined
    : /* sql */ `
UPDATE output o SET spent_by_transaction_internal_id = spender.internal_id
  FROM (VALUES ${spends
    .map(
      (spend) =>
        `(${bytea(spend.outpointTransactionHash)}, ${
          spend.outpointIndex
        }::bigint, ${bytea(spend.spenderHash)})`
    )
    .join(',')}) AS v (outpoint_transaction_hash, outpoint_index, spender_hash)
  CROSS JOIN LATERAL (
    SELECT internal_id FROM transaction WHERE transaction.hash = v.spender_hash OFFSET 0
  ) spender
  WHERE o.transaction_hash = v.outpoint_transaction_hash
    AND o.output_index = v.outpoint_index
    AND o.spent_by_transaction_internal_id IS DISTINCT FROM spender.internal_id;`;

/**
 * `settable` mode, `sql` write path: one statement deleting the set rows of
 * every spent outpoint. Returns `undefined` if there is nothing to delete.
 */
export const buildDeleteSpentFromSetSql = (spends: Spend[]) =>
  spends.length === 0
    ? undefined
    : /* sql */ `
DELETE FROM unspent_output_set u
  USING (VALUES ${spends
    .map(
      (spend) =>
        `(${bytea(spend.outpointTransactionHash)}, ${
          spend.outpointIndex
        }::bigint)`
    )
    .join(',')}) AS v (outpoint_transaction_hash, outpoint_index)
  WHERE u.transaction_hash = v.outpoint_transaction_hash
    AND u.output_index = v.outpoint_index;`;

/**
 * `copy` write path equivalents, reading `pg_temp.chaingraph_stage_spend`
 * (filled by binary COPY with every spend of the block). The `OFFSET 0`
 * sub-selects keep the per-row index lookups (temporary tables have no
 * statistics).
 */
export const markStagedSpentOutputsSql = /* sql */ `
UPDATE output o SET spent_by_transaction_internal_id = s.spender_internal_id
  FROM (
    SELECT v.outpoint_transaction_hash, v.outpoint_index, spender.internal_id AS spender_internal_id
      FROM pg_temp.chaingraph_stage_spend v
      CROSS JOIN LATERAL (
        SELECT internal_id FROM transaction WHERE transaction.hash = v.spender_hash OFFSET 0
      ) spender
    OFFSET 0
  ) s
  WHERE o.transaction_hash = s.outpoint_transaction_hash
    AND o.output_index = s.outpoint_index
    AND o.spent_by_transaction_internal_id IS DISTINCT FROM s.spender_internal_id;`;

export const deleteStagedSpentFromSetSql = /* sql */ `
DELETE FROM unspent_output_set u
  USING (SELECT outpoint_transaction_hash, outpoint_index FROM pg_temp.chaingraph_stage_spend OFFSET 0) v
  WHERE u.transaction_hash = v.outpoint_transaction_hash
    AND u.output_index = v.outpoint_index;`;

/**
 * Re-acceptance (a transaction or block that gains acceptance after its spends
 * may have been released): mark/delete from the saved inputs of the given
 * transactions. In `marker` mode only unset markers (NULL or 0) are written, so
 * an existing accepted spender is kept. `$1` is a `bigint[]` of transaction
 * internal IDs.
 */
export const reacceptSpendsSql = (mode: UnspentTrackingMode) =>
  mode === 'marker'
    ? /* sql */ `
UPDATE output o SET spent_by_transaction_internal_id = i.transaction_internal_id
  FROM (SELECT DISTINCT unnest($1::bigint[]) AS id) s
  JOIN input i ON i.transaction_internal_id = s.id
  WHERE o.transaction_hash = i.outpoint_transaction_hash
    AND o.output_index = i.outpoint_index
    AND (o.spent_by_transaction_internal_id IS NULL OR o.spent_by_transaction_internal_id = 0);`
    : mode === 'settable'
    ? /* sql */ `
DELETE FROM unspent_output_set u
  USING (SELECT DISTINCT unnest($1::bigint[]) AS id) s
  JOIN input i ON i.transaction_internal_id = s.id
  WHERE u.transaction_hash = i.outpoint_transaction_hash
    AND u.output_index = i.outpoint_index;`
    : undefined;

/**
 * Transactions of blocks that just gained their first accepting node: `$1` is
 * a `bigint[]` of block internal IDs, `$2` the accepting node.
 */
export const firstAcceptedBlockTransactionsSql = /* sql */ `
SELECT array_agg(DISTINCT bt.transaction_internal_id)::text[] AS ids
  FROM block_transaction bt
  WHERE bt.block_internal_id = ANY ($1::bigint[])
    AND NOT EXISTS (
      SELECT 1 FROM node n
        JOIN node_block nb
          ON nb.node_internal_id = n.internal_id
         AND nb.block_internal_id = bt.block_internal_id
        WHERE n.internal_id <> $2::bigint
    );`;

const triggers: {
  [mode in 'marker' | 'settable']: readonly (readonly [string, string])[];
} = {
  marker: [
    ['node_block', 'trigger_unspent_marker_node_block_delete'],
    ['node_transaction', 'trigger_unspent_marker_node_transaction_delete'],
  ],
  settable: [
    ['node_block', 'trigger_unspent_settable_node_block_delete'],
    ['node_transaction', 'trigger_unspent_settable_node_transaction_delete'],
  ],
} as const;

/**
 * Statements enabling the release triggers of `mode` and disabling the others,
 * for the triggers whose state differs. `existingTriggers` maps each trigger
 * present in the database to whether it is enabled (the migration may be
 * missing in `off` mode).
 */
export const configureUnspentTrackingTriggersSql = (
  mode: UnspentTrackingMode,
  existingTriggers: { [name: string]: boolean }
) =>
  (['marker', 'settable'] as const).flatMap((triggerMode) =>
    triggers[triggerMode]
      .filter(
        ([, name]) =>
          name in existingTriggers &&
          existingTriggers[name] !== (triggerMode === mode)
      )
      .map(
        ([table, name]) =>
          `ALTER TABLE ${table} ${
            triggerMode === mode ? 'ENABLE' : 'DISABLE'
          } TRIGGER ${name};`
      )
  );

export const unspentTrackingTriggerNames = [
  ...triggers.marker.map(([, name]) => name),
  ...triggers.settable.map(([, name]) => name),
];
