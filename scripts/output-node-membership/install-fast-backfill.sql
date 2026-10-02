\set ON_ERROR_STOP on

/*
 * Install an offline, restartable backfill which computes normalized state
 * once and updates only output rows whose arrays differ.
 */
DO $preflight$
BEGIN
  IF to_regclass('output_membership.state') IS NULL
    OR to_regclass('public.output') IS NULL
    OR to_regclass('public.input') IS NULL
    OR to_regclass('public.transaction') IS NULL
    OR to_regclass('public.node_transaction') IS NULL
    OR to_regclass('public.block_transaction') IS NULL
    OR to_regclass('public.node_block') IS NULL THEN
    RAISE EXCEPTION 'output membership migration and normalized source tables are required';
  END IF;

  IF (SELECT count(*) FROM output_membership.state WHERE id) <> 1 THEN
    RAISE EXCEPTION 'exactly one output_membership.state row is required';
  END IF;
END
$preflight$;

CREATE SCHEMA IF NOT EXISTS output_membership_backfill;

CREATE TABLE IF NOT EXISTS output_membership_backfill.state (
  id boolean PRIMARY KEY DEFAULT true CHECK (id),
  version integer NOT NULL CHECK (version = 1),
  phase text NOT NULL CHECK (
    phase IN (
      'pending',
      'acceptance-ready',
      'target-building',
      'target-ready',
      'backfilling',
      'backfilled'
    )
  ),
  input_rows bigint,
  output_rows bigint,
  transaction_rows bigint,
  node_rows bigint,
  scratch_budget_bytes bigint,
  required_scratch_bytes bigint,
  desired_row_ceiling bigint,
  output_heap_blocks bigint,
  next_target_heap_block bigint NOT NULL DEFAULT 0,
  next_output_heap_block bigint NOT NULL DEFAULT 0,
  source_rows bigint,
  rows_updated bigint NOT NULL DEFAULT 0,
  started_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
  target_built_at timestamp with time zone,
  finished_at timestamp with time zone,
  updated_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
  original_membership_state jsonb NOT NULL
);

ALTER TABLE output_membership_backfill.state
  ADD COLUMN IF NOT EXISTS next_target_heap_block bigint NOT NULL DEFAULT 0;
ALTER TABLE output_membership_backfill.state
  DROP CONSTRAINT IF EXISTS state_phase_check;
ALTER TABLE output_membership_backfill.state
  ADD CONSTRAINT state_phase_check CHECK (
    phase IN (
      'pending',
      'acceptance-ready',
      'target-building',
      'target-ready',
      'backfilling',
      'backfilled'
    )
  );

INSERT INTO output_membership_backfill.state (
  id,
  version,
  phase,
  original_membership_state
)
SELECT true, 1, 'pending', to_jsonb(state)
  FROM output_membership.state AS state
  WHERE state.id
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS output_membership_backfill.batch (
  start_output_heap_block bigint PRIMARY KEY,
  end_output_heap_block bigint NOT NULL,
  attempts integer NOT NULL,
  started_at timestamp with time zone NOT NULL,
  finished_at timestamp with time zone NOT NULL,
  source_rows bigint NOT NULL,
  rows_updated bigint NOT NULL,
  duration interval NOT NULL
);

CREATE OR REPLACE PROCEDURE output_membership_backfill.run (
  batch_heap_blocks bigint DEFAULT 20000,
  operator_confirmation text DEFAULT NULL,
  scratch_budget_bytes bigint DEFAULT NULL,
  desired_row_ceiling bigint DEFAULT NULL,
  fail_after_update boolean DEFAULT false
)
LANGUAGE plpgsql
AS $procedure$
DECLARE
  current_phase text;
  start_block bigint;
  end_block bigint;
  final_block bigint;
  source_count bigint;
  updated_count bigint;
  mismatch_count bigint;
  input_count bigint;
  output_count bigint;
  transaction_count bigint;
  node_count bigint;
  required_bytes numeric;
  batch_started timestamp with time zone;
BEGIN
  IF operator_confirmation IS DISTINCT FROM 'WRITERS_PAUSED' THEN
    RAISE EXCEPTION
      'operator_confirmation must be WRITERS_PAUSED after pausing the agent and every competing backfill';
  END IF;

  IF batch_heap_blocks < 1 OR batch_heap_blocks > 200000 THEN
    RAISE EXCEPTION 'batch_heap_blocks must be between 1 and 200000';
  END IF;

  IF scratch_budget_bytes IS NULL OR scratch_budget_bytes < 1073741824 THEN
    RAISE EXCEPTION 'scratch_budget_bytes must report at least 1 GiB of free space';
  END IF;

  IF desired_row_ceiling IS NULL OR desired_row_ceiling < 1 THEN
    RAISE EXCEPTION 'desired_row_ceiling must be a positive observed upper bound';
  END IF;

  IF NOT pg_try_advisory_lock(20261001, 1) THEN
    RAISE EXCEPTION
      'writer advisory lock is held; a writer or another backfill is active';
  END IF;

  IF NOT EXISTS (SELECT 1 FROM output_membership.state WHERE id) THEN
    RAISE EXCEPTION 'output_membership.state is missing';
  END IF;

  IF (SELECT ready FROM output_membership.state WHERE id) THEN
    RAISE EXCEPTION 'output membership is already exposed as ready';
  END IF;

  IF (SELECT phase FROM output_membership.state WHERE id)
    NOT IN ('pending-backfill', 'backfilling', 'backfilled') THEN
    RAISE EXCEPTION 'output membership state is not eligible for offline backfill';
  END IF;

  PERFORM set_config('output_membership.backfill', 'on', false);

  SELECT phase
    INTO STRICT current_phase
    FROM output_membership_backfill.state
    WHERE id
    FOR UPDATE;

  IF current_phase <> 'pending' AND EXISTS (
    SELECT 1
      FROM output_membership_backfill.state
      WHERE id
        AND (
          state.scratch_budget_bytes <> run.scratch_budget_bytes
          OR state.desired_row_ceiling <> run.desired_row_ceiling
        )
  ) THEN
    RAISE EXCEPTION
      'resume must use the original scratch budget and desired row ceiling';
  END IF;

  IF current_phase = 'pending' THEN
    IF to_regclass('output_membership_backfill.accepted_transaction') IS NOT NULL
      OR to_regclass('output_membership_backfill.desired_output') IS NOT NULL THEN
      RAISE EXCEPTION 'unexpected fast-backfill source table exists';
    END IF;

    UPDATE output_membership.state
      SET phase = 'backfilling', ready = false, updated_at = clock_timestamp()
      WHERE id;

    PERFORM set_config('enable_nestloop', 'off', false);
    PERFORM set_config('enable_mergejoin', 'off', false);
    PERFORM set_config('enable_hashjoin', 'on', false);

    /* Exact counts avoid relying on input n_distinct or stale row estimates. */
    SELECT count(*) INTO input_count FROM public.input;
    SELECT count(*) INTO output_count FROM public.output;
    SELECT count(*) INTO transaction_count FROM public.transaction;
    SELECT count(*) INTO node_count FROM public.node;

    IF desired_row_ceiling > output_count THEN
      RAISE EXCEPTION
        'desired_row_ceiling % exceeds the % output rows',
        desired_row_ceiling,
        output_count;
    END IF;

    /*
     * Upper-bound the accepted heap, its two lookup indexes, the covering
     * input lookup index, the desired heap and CTID index, 15 percent page
     * overhead, and 64 GiB for WAL/checkpoints. Target batches need no
     * database-sized hash or sort spill.
     */
    required_bytes := ceil((
      transaction_count * (160 + 4 * node_count)
      + input_count * 80::numeric
      + desired_row_ceiling::numeric * (112 + 8 * node_count)
    ) * 1.15) + 68719476736;

    IF required_bytes > scratch_budget_bytes THEN
      RAISE EXCEPTION
        'scratch gate rejected build: required % bytes, budget % bytes',
        required_bytes::bigint,
        scratch_budget_bytes;
    END IF;

    UPDATE output_membership_backfill.state
      SET
        input_rows = input_count,
        output_rows = output_count,
        transaction_rows = transaction_count,
        node_rows = node_count,
        scratch_budget_bytes = run.scratch_budget_bytes,
        required_scratch_bytes = required_bytes::bigint,
        desired_row_ceiling = run.desired_row_ceiling,
        updated_at = clock_timestamp()
      WHERE id AND phase = 'pending';

    /* One row per accepted transaction, including mempool and every accepted fork. */
    CREATE TABLE output_membership_backfill.accepted_transaction AS
      WITH accepted AS MATERIALIZED (
        SELECT
          node_transaction.transaction_internal_id,
          node_transaction.node_internal_id
          FROM public.node_transaction
        UNION ALL
        SELECT
          block_transaction.transaction_internal_id,
          node_block.node_internal_id
          FROM public.block_transaction
          INNER JOIN public.node_block USING (block_internal_id)
      )
      SELECT
        accepted.transaction_internal_id,
        transaction.hash AS transaction_hash,
        array_agg(DISTINCT
          accepted.node_internal_id ORDER BY accepted.node_internal_id
        )::integer[] AS accepted_node_ids
        FROM accepted
        INNER JOIN public.transaction
          ON transaction.internal_id = accepted.transaction_internal_id
        GROUP BY accepted.transaction_internal_id, transaction.hash;

    ANALYZE output_membership_backfill.accepted_transaction;

    UPDATE output_membership_backfill.state
      SET phase = 'acceptance-ready', updated_at = clock_timestamp()
      WHERE id AND phase = 'pending';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'fast-backfill phase changed while building acceptance';
    END IF;
    COMMIT;

    IF current_setting(
      'output_membership_backfill.test_fail_after_acceptance', true
    ) = 'on' THEN
      RAISE EXCEPTION 'requested fault after acceptance build';
    END IF;
  END IF;

  SELECT phase
    INTO STRICT current_phase
    FROM output_membership_backfill.state
    WHERE id
    FOR UPDATE;

  IF current_phase IN ('acceptance-ready', 'target-building') THEN
    PERFORM set_config('enable_nestloop', 'on', false);
    PERFORM set_config('enable_mergejoin', 'off', false);
    PERFORM set_config('enable_hashjoin', 'off', false);

    /*
     * These indexes turn each output heap batch into bounded point lookups.
     * Their builds use bounded maintenance_work_mem rather than the unbounded
     * hash and sort spills of the former whole-database target query.
     */
    CREATE UNIQUE INDEX IF NOT EXISTS accepted_transaction_internal_id
      ON output_membership_backfill.accepted_transaction
        (transaction_internal_id);
    COMMIT;
    CREATE UNIQUE INDEX IF NOT EXISTS accepted_transaction_hash
      ON output_membership_backfill.accepted_transaction
        (transaction_hash);
    COMMIT;
    CREATE INDEX IF NOT EXISTS output_membership_backfill_input_outpoint
      ON public.input (outpoint_transaction_hash, outpoint_index)
      INCLUDE (transaction_internal_id);
    COMMIT;

    CREATE TABLE IF NOT EXISTS output_membership_backfill.desired_output (
      target_ctid tid PRIMARY KEY,
      transaction_hash bytea NOT NULL,
      output_index bigint NOT NULL,
      accepted_node_ids integer[] NOT NULL,
      unspent_node_ids integer[] NOT NULL
    );
    COMMIT;

    IF current_phase = 'acceptance-ready' THEN
      UPDATE output_membership_backfill.state
        SET
          phase = 'target-building',
          output_heap_blocks = (
            pg_relation_size('public.output')
            + current_setting('block_size')::bigint - 1
          ) / current_setting('block_size')::bigint,
          next_target_heap_block = 0,
          source_rows = 0,
          updated_at = clock_timestamp()
        WHERE id AND phase = 'acceptance-ready';
      IF NOT FOUND THEN
        RAISE EXCEPTION 'fast-backfill phase changed while starting target';
      END IF;
      COMMIT;
    END IF;

    LOOP
      SELECT next_target_heap_block, output_heap_blocks, source_rows
        INTO STRICT start_block, final_block, source_count
        FROM output_membership_backfill.state
        WHERE id
        FOR UPDATE;

      EXIT WHEN start_block >= final_block;
      end_block := least(start_block + batch_heap_blocks, final_block);

      WITH output_batch AS MATERIALIZED (
        SELECT
          output.ctid AS target_ctid,
          output.transaction_hash,
          output.output_index,
          output.accepted_node_ids AS old_accepted_node_ids,
          output.unspent_node_ids AS old_unspent_node_ids,
          output.locking_bytecode
          FROM public.output
          WHERE output.ctid >= format('(%s,0)', start_block)::tid
            AND output.ctid < format('(%s,0)', end_block)::tid
      ),
      spent_output AS MATERIALIZED (
        SELECT
          output_batch.target_ctid,
          array_agg(DISTINCT node_id ORDER BY node_id)::integer[] AS spent_node_ids
          FROM output_batch
          INNER JOIN public.input
            ON input.outpoint_transaction_hash = output_batch.transaction_hash
            AND input.outpoint_index = output_batch.output_index
          INNER JOIN output_membership_backfill.accepted_transaction
            USING (transaction_internal_id)
          CROSS JOIN LATERAL unnest(accepted_node_ids) AS node_id
          GROUP BY output_batch.target_ctid
      ),
      computed AS MATERIALIZED (
        SELECT
          output_batch.target_ctid,
          output_batch.transaction_hash,
          output_batch.output_index,
          output_batch.old_accepted_node_ids,
          output_batch.old_unspent_node_ids,
          coalesce(
            accepted_transaction.accepted_node_ids,
            ARRAY[]::integer[]
          ) AS accepted_node_ids,
          CASE
            WHEN octet_length(output_batch.locking_bytecode) > 0
              AND get_byte(output_batch.locking_bytecode, 0) = 106
              THEN ARRAY[]::integer[]
            ELSE ARRAY(
              SELECT accepted_node_id
                FROM unnest(coalesce(
                  accepted_transaction.accepted_node_ids,
                  ARRAY[]::integer[]
                )) AS accepted_node_id
                WHERE NOT accepted_node_id = ANY(coalesce(
                  spent_output.spent_node_ids,
                  ARRAY[]::integer[]
                ))
                ORDER BY accepted_node_id
            )::integer[]
          END AS unspent_node_ids
          FROM output_batch
          LEFT JOIN output_membership_backfill.accepted_transaction
            USING (transaction_hash)
          LEFT JOIN spent_output
            USING (target_ctid)
      )
      INSERT INTO output_membership_backfill.desired_output
      SELECT
        target_ctid,
        transaction_hash,
        output_index,
        accepted_node_ids,
        unspent_node_ids
        FROM computed
        WHERE ROW(old_accepted_node_ids, old_unspent_node_ids)
          IS DISTINCT FROM ROW(accepted_node_ids, unspent_node_ids)
        LIMIT desired_row_ceiling - source_count + 1;
      GET DIAGNOSTICS updated_count = ROW_COUNT;

      IF source_count + updated_count > desired_row_ceiling THEN
        RAISE EXCEPTION
          'desired output exceeded the % row ceiling; retry with a larger proven budget',
          desired_row_ceiling;
      END IF;

      UPDATE output_membership_backfill.state
        SET
          next_target_heap_block = end_block,
          source_rows = source_count + updated_count,
          updated_at = clock_timestamp()
        WHERE id
          AND phase = 'target-building'
          AND next_target_heap_block = start_block;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'target-build cursor changed unexpectedly';
      END IF;
      COMMIT;

      IF current_setting(
        'output_membership_backfill.test_fail_during_target', true
      ) = 'on' THEN
        RAISE EXCEPTION 'requested fault during target build';
      END IF;
    END LOOP;

    ANALYZE output_membership_backfill.desired_output;
    DROP TABLE output_membership_backfill.accepted_transaction;
    DROP INDEX public.output_membership_backfill_input_outpoint;

    UPDATE output_membership_backfill.state
      SET
        phase = 'target-ready',
        target_built_at = clock_timestamp(),
        updated_at = clock_timestamp()
      WHERE id
        AND phase = 'target-building'
        AND next_target_heap_block = output_heap_blocks;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'fast-backfill phase changed while building target';
    END IF;
    COMMIT;
  END IF;

  PERFORM set_config('enable_nestloop', 'on', false);
  PERFORM set_config('enable_mergejoin', 'off', false);
  PERFORM set_config('enable_hashjoin', 'off', false);

  LOOP
    SELECT next_output_heap_block, output_heap_blocks
      INTO STRICT start_block, final_block
      FROM output_membership_backfill.state
      WHERE id
      FOR UPDATE;

    EXIT WHEN start_block >= final_block;
    end_block := least(start_block + batch_heap_blocks, final_block);
    batch_started := clock_timestamp();

    SELECT count(*)
      INTO source_count
      FROM output_membership_backfill.desired_output
      WHERE target_ctid >= format('(%s,0)', start_block)::tid
        AND target_ctid < format('(%s,0)', end_block)::tid;

    WITH batch AS MATERIALIZED (
      SELECT
        target_ctid,
        transaction_hash,
        output_index,
        accepted_node_ids,
        unspent_node_ids
        FROM output_membership_backfill.desired_output
        WHERE target_ctid >= format('(%s,0)', start_block)::tid
          AND target_ctid < format('(%s,0)', end_block)::tid
    )
    UPDATE public.output
      SET
        accepted_node_ids = batch.accepted_node_ids,
        unspent_node_ids = batch.unspent_node_ids
      FROM batch
      WHERE output.ctid = batch.target_ctid
        AND output.transaction_hash = batch.transaction_hash
        AND output.output_index = batch.output_index
        AND ROW(output.accepted_node_ids, output.unspent_node_ids)
          IS DISTINCT FROM ROW(batch.accepted_node_ids, batch.unspent_node_ids);
    GET DIAGNOSTICS updated_count = ROW_COUNT;

    IF fail_after_update AND source_count > 0 THEN
      RAISE EXCEPTION 'requested fault after output update';
    END IF;

    IF source_count <> updated_count THEN
      RAISE EXCEPTION
        'CTID batch updated % rows from a % row source; output changed while offline',
        updated_count,
        source_count;
    END IF;

    INSERT INTO output_membership_backfill.batch (
      start_output_heap_block,
      end_output_heap_block,
      attempts,
      started_at,
      finished_at,
      source_rows,
      rows_updated,
      duration
    ) VALUES (
      start_block,
      end_block,
      1,
      batch_started,
      clock_timestamp(),
      source_count,
      updated_count,
      clock_timestamp() - batch_started
    )
    ON CONFLICT (start_output_heap_block) DO UPDATE SET
      end_output_heap_block = excluded.end_output_heap_block,
      attempts = output_membership_backfill.batch.attempts + 1,
      started_at = excluded.started_at,
      finished_at = excluded.finished_at,
      source_rows = excluded.source_rows,
      rows_updated = excluded.rows_updated,
      duration = excluded.duration;

    UPDATE output_membership_backfill.state
      SET
        phase = 'backfilling',
        next_output_heap_block = end_block,
        rows_updated = rows_updated + updated_count,
        updated_at = clock_timestamp()
      WHERE id AND next_output_heap_block = start_block;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'fast-backfill cursor changed unexpectedly';
    END IF;

    UPDATE output_membership.state
      SET
        phase = 'backfilling',
        ready = false,
        rows_updated = rows_updated + updated_count,
        updated_at = clock_timestamp()
      WHERE id;

    COMMIT;
  END LOOP;

  PERFORM set_config('enable_nestloop', 'off', false);
  PERFORM set_config('enable_hashjoin', 'on', false);

  SELECT count(*)
    INTO mismatch_count
    FROM output_membership_backfill.desired_output
    LEFT JOIN public.output USING (transaction_hash, output_index)
    WHERE output.ctid IS NULL
      OR ROW(output.accepted_node_ids, output.unspent_node_ids)
        IS DISTINCT FROM ROW(
          desired_output.accepted_node_ids,
          desired_output.unspent_node_ids
        );

  IF mismatch_count <> 0 THEN
    RAISE EXCEPTION '% materialized output rows do not match', mismatch_count;
  END IF;

  UPDATE output_membership_backfill.state
    SET
      phase = 'backfilled',
      finished_at = clock_timestamp(),
      updated_at = clock_timestamp()
    WHERE id
      AND next_output_heap_block = output_heap_blocks
      AND rows_updated = source_rows;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'fast-backfill totals are incomplete';
  END IF;

  UPDATE output_membership.state
    SET
      phase = 'backfilled',
      ready = false,
      next_heap_block = original_heap_blocks,
      updated_at = clock_timestamp()
    WHERE id;
  COMMIT;

  PERFORM set_config('output_membership.backfill', 'off', false);
  PERFORM pg_advisory_unlock(20261001, 1);
END
$procedure$;

SELECT row_to_json(state) FROM output_membership_backfill.state;
