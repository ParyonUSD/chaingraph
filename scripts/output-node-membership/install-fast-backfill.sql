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
  original_membership_state jsonb NOT NULL,
  target_stage text,
  target_scratch_budget_bytes bigint
);

ALTER TABLE output_membership_backfill.state
  ADD COLUMN IF NOT EXISTS next_target_heap_block bigint NOT NULL DEFAULT 0;
ALTER TABLE output_membership_backfill.state
  ADD COLUMN IF NOT EXISTS target_stage text;
ALTER TABLE output_membership_backfill.state
  ADD COLUMN IF NOT EXISTS target_scratch_budget_bytes bigint;
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

CREATE TABLE IF NOT EXISTS output_membership_backfill.target_node (
  node_internal_id integer PRIMARY KEY,
  status text NOT NULL CHECK (status IN ('pending', 'complete')),
  rows_written bigint,
  started_at timestamp with time zone,
  finished_at timestamp with time zone
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
  default_node_id integer;
  current_node_id integer;
  current_target_stage text;
  target_budget bigint;
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

  SELECT phase, input_rows, transaction_rows, node_rows
    INTO STRICT current_phase, input_count, transaction_count, node_count
    FROM output_membership_backfill.state
    WHERE id
    FOR UPDATE;

  IF current_phase <> 'pending' AND (
    input_count IS NULL
    OR transaction_count IS NULL
    OR node_count IS NULL
  ) THEN
    RAISE EXCEPTION
      'persisted source counts are required to resume phase %', current_phase;
  END IF;

  IF current_phase <> 'pending' AND EXISTS (
    SELECT 1
      FROM output_membership_backfill.state
      WHERE id
        AND state.desired_row_ceiling <> run.desired_row_ceiling
  ) THEN
    RAISE EXCEPTION
      'resume must use the original desired row ceiling';
  END IF;

  IF current_phase <> 'pending' AND EXISTS (
    SELECT 1
      FROM output_membership_backfill.state
      WHERE id
        AND target_scratch_budget_bytes IS NOT NULL
        AND target_scratch_budget_bytes <> run.scratch_budget_bytes
  ) THEN
    RAISE EXCEPTION
      'replacement target resume must use its recorded current-free-space budget';
  END IF;

  IF current_phase = 'pending' THEN
    IF to_regclass('output_membership_backfill.accepted_transaction') IS NOT NULL
      OR to_regclass('output_membership_backfill.desired_output') IS NOT NULL THEN
      RAISE EXCEPTION 'unexpected fast-backfill source table exists';
    END IF;

    UPDATE output_membership.state
      SET phase = 'backfilling', ready = false, updated_at = clock_timestamp()
      WHERE id;

    PERFORM set_config('enable_nestloop', 'off', true);
    PERFORM set_config('enable_mergejoin', 'off', true);
    PERFORM set_config('enable_hashjoin', 'on', true);

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

  SELECT phase, target_stage
    INTO STRICT current_phase, current_target_stage
    FROM output_membership_backfill.state
    WHERE id
    FOR UPDATE;

  /*
   * Revision 1 built a target by probing the accepted and input indexes once
   * per output. Only abandon that target when it has proven empty and no CTID
   * update has started; the canonical backfill cursor and count must still be
   * exactly the values captured at installation.
   */
  IF current_phase = 'target-building' AND current_target_stage IS NULL THEN
    IF to_regclass('output_membership_backfill.desired_output') IS NULL THEN
      RAISE EXCEPTION 'legacy target provenance is missing';
    END IF;
    SELECT count(*) INTO source_count
      FROM output_membership_backfill.desired_output;
    IF source_count <> 0
      OR EXISTS (SELECT 1 FROM output_membership_backfill.batch)
      OR EXISTS (
        SELECT 1
        FROM output_membership_backfill.state s
        CROSS JOIN output_membership.state canonical
        WHERE s.id AND canonical.id
          AND (
            s.next_output_heap_block <> 0
            OR s.rows_updated <> 0
            OR canonical.next_heap_block IS DISTINCT FROM
              (s.original_membership_state->>'next_heap_block')::bigint
            OR canonical.rows_updated IS DISTINCT FROM
              (s.original_membership_state->>'rows_updated')::bigint
          )
      ) THEN
      RAISE EXCEPTION
        'cannot replace a nonempty target or a backfill which has begun updating output';
    END IF;
    DROP TABLE output_membership_backfill.desired_output;
    TRUNCATE output_membership_backfill.target_node;
    UPDATE output_membership_backfill.state
      SET phase = 'acceptance-ready', next_target_heap_block = 0,
          source_rows = NULL, updated_at = clock_timestamp()
      WHERE id AND phase = 'target-building' AND target_stage IS NULL;
    COMMIT;
    current_phase := 'acceptance-ready';
  END IF;

  IF current_phase IN ('acceptance-ready', 'target-building') THEN
    SELECT default_node_internal_id
      INTO STRICT default_node_id
      FROM output_membership.state
      WHERE id;
    IF default_node_id IS NULL
      OR NOT EXISTS (
        SELECT 1 FROM public.node WHERE internal_id = default_node_id
      ) THEN
      RAISE EXCEPTION 'the canonical default node is missing';
    END IF;

    CREATE UNIQUE INDEX IF NOT EXISTS accepted_transaction_internal_id
      ON output_membership_backfill.accepted_transaction
        (transaction_internal_id);
    COMMIT;
    CREATE UNIQUE INDEX IF NOT EXISTS accepted_transaction_hash
      ON output_membership_backfill.accepted_transaction
        (transaction_hash);
    COMMIT;

    SELECT target_stage
      INTO STRICT current_target_stage
      FROM output_membership_backfill.state
      WHERE id
      FOR UPDATE;

    IF current_target_stage IS NULL THEN
      IF to_regclass('output_membership_backfill.acceptance_exception') IS NOT NULL
        OR to_regclass('output_membership_backfill.desired_nondefault') IS NOT NULL
        OR to_regclass('output_membership_backfill.desired_output') IS NOT NULL
        OR EXISTS (SELECT 1 FROM output_membership_backfill.target_node) THEN
        RAISE EXCEPTION 'unexpected replacement target relation exists';
      END IF;

      /* Usually small: every accepted array which differs from the heap default. */
      CREATE TABLE output_membership_backfill.acceptance_exception AS
        SELECT transaction_internal_id, transaction_hash, accepted_node_ids
        FROM output_membership_backfill.accepted_transaction
        WHERE accepted_node_ids IS DISTINCT FROM ARRAY[default_node_id]::integer[];
      CREATE UNIQUE INDEX acceptance_exception_internal_id
        ON output_membership_backfill.acceptance_exception
          (transaction_internal_id);
      CREATE UNIQUE INDEX acceptance_exception_hash
        ON output_membership_backfill.acceptance_exception
          (transaction_hash);
      ANALYZE output_membership_backfill.acceptance_exception;

      CREATE TABLE output_membership_backfill.excluded_default_output AS
        SELECT output.transaction_hash, output.output_index
        FROM output_membership_backfill.acceptance_exception exception
        INNER JOIN public.output USING (transaction_hash)
        WHERE NOT default_node_id = ANY(exception.accepted_node_ids)
        LIMIT desired_row_ceiling + 1;
      SELECT count(*) INTO source_count
        FROM output_membership_backfill.excluded_default_output;
      IF source_count > desired_row_ceiling THEN
        RAISE EXCEPTION
          'default-node exclusion source exceeded the % row ceiling',
          desired_row_ceiling;
      END IF;
      ALTER TABLE output_membership_backfill.excluded_default_output
        ADD PRIMARY KEY (transaction_hash, output_index);
      ANALYZE output_membership_backfill.excluded_default_output;

      CREATE TABLE output_membership_backfill.desired_nondefault (
        transaction_hash bytea NOT NULL,
        output_index bigint NOT NULL,
        accepted_node_ids integer[] NOT NULL,
        unspent_node_ids integer[] NOT NULL,
        PRIMARY KEY (transaction_hash, output_index)
      );
      CREATE TABLE output_membership_backfill.desired_output (
        target_ctid tid PRIMARY KEY,
        transaction_hash bytea NOT NULL,
        output_index bigint NOT NULL,
        accepted_node_ids integer[] NOT NULL,
        unspent_node_ids integer[] NOT NULL
      );
      CREATE TABLE output_membership_backfill.node_utxo_stage (
        transaction_hash bytea NOT NULL,
        output_index bigint NOT NULL,
        PRIMARY KEY (transaction_hash, output_index)
      );
      INSERT INTO output_membership_backfill.target_node (
        node_internal_id, status
      )
      SELECT internal_id, 'pending' FROM public.node ORDER BY internal_id;

      /*
       * Peak replacement scratch is the larger of: (a) one external outpoint
       * sort plus the sparse source, capped node stage, and capped excluded-key
       * stream; or (b) the sparse source and final target plus one 8 GiB hash
       * spill allowance. Add measured exceptions, 15 percent page margin, and
       * a fixed 64 GiB WAL/checkpoint reserve. The two peaks occur in separate
       * committed stages; no billion-row spent table is retained.
       */
      required_bytes := ceil((
        greatest(
          input_count * 112::numeric
            + desired_row_ceiling::numeric * (
                (112 + 8 * node_count) + 128
              ),
          desired_row_ceiling::numeric * 2 * (112 + 8 * node_count)
            + 8589934592::numeric
        ) + pg_total_relation_size(
            'output_membership_backfill.acceptance_exception'
          )::numeric + pg_total_relation_size(
            'output_membership_backfill.excluded_default_output'
          )::numeric
      ) * 1.15) + 68719476736;
      IF required_bytes > scratch_budget_bytes THEN
        RAISE EXCEPTION
          'replacement scratch gate rejected build: required % bytes, current free-space budget % bytes',
          required_bytes::bigint, scratch_budget_bytes;
      END IF;

      UPDATE output_membership_backfill.state
        SET phase = 'target-building',
            target_stage = 'exceptions-ready',
            target_scratch_budget_bytes = run.scratch_budget_bytes,
            required_scratch_bytes = required_bytes::bigint,
            output_heap_blocks = (
              pg_relation_size('public.output')
              + current_setting('block_size')::bigint - 1
            ) / current_setting('block_size')::bigint,
            next_target_heap_block = 0,
            source_rows = 0,
            updated_at = clock_timestamp()
        WHERE id AND phase = 'acceptance-ready' AND target_stage IS NULL;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'fast-backfill phase changed while starting replacement target';
      END IF;
      COMMIT;
      current_target_stage := 'exceptions-ready';
    END IF;

    IF current_target_stage IN ('exceptions-ready', 'nodes-building') THEN
      UPDATE output_membership_backfill.state
        SET target_stage = 'nodes-building', updated_at = clock_timestamp()
        WHERE id AND target_stage = 'exceptions-ready';
      COMMIT;

      LOOP
        SELECT node_internal_id
          INTO current_node_id
          FROM output_membership_backfill.target_node
          WHERE status = 'pending'
          ORDER BY (node_internal_id = default_node_id) DESC, node_internal_id
          LIMIT 1;
        EXIT WHEN NOT FOUND;

        UPDATE output_membership_backfill.target_node
          SET started_at = clock_timestamp()
          WHERE node_internal_id = current_node_id AND status = 'pending';
        TRUNCATE output_membership_backfill.node_utxo_stage;

        /*
         * The input primary key is almost perfectly heap-correlated in the
         * production database. Disabling optional sorts selects that stream;
         * the one unavoidable outpoint Sort remains in the plan with
         * "Disabled: true". Disabling hash aggregation makes GroupAggregate
         * consume that sort directly, avoiding simultaneous HashAggregate and
         * Sort spills. These settings are transaction-local and reset at COMMIT.
         */
        PERFORM set_config('work_mem', '4GB', true);
        PERFORM set_config('jit', 'off', true);
        PERFORM set_config('max_parallel_workers_per_gather', '0', true);
        PERFORM set_config('join_collapse_limit', '1', true);
        PERFORM set_config('from_collapse_limit', '1', true);
        PERFORM set_config('enable_nestloop', 'off', true);
        PERFORM set_config('enable_hashjoin', 'off', true);
        PERFORM set_config('enable_mergejoin', 'on', true);
        PERFORM set_config('enable_seqscan', 'off', true);
        PERFORM set_config('enable_sort', 'off', true);
        PERFORM set_config('enable_hashagg', 'off', true);

        IF current_node_id = default_node_id THEN
          WITH spender_acceptance AS NOT MATERIALIZED (
            SELECT accepted.transaction_internal_id
            FROM output_membership_backfill.accepted_transaction accepted
            LEFT JOIN (
              SELECT transaction_internal_id
              FROM output_membership_backfill.acceptance_exception
              WHERE NOT current_node_id = ANY(accepted_node_ids)
            ) excluded USING (transaction_internal_id)
            WHERE excluded.transaction_internal_id IS NULL
          ),
          created_output AS NOT MATERIALIZED (
            SELECT output.transaction_hash, output.output_index
            FROM public.output
            INNER JOIN output_membership_backfill.accepted_transaction accepted
              USING (transaction_hash)
            LEFT JOIN output_membership_backfill.excluded_default_output excluded
              USING (transaction_hash, output_index)
            WHERE excluded.transaction_hash IS NULL
          ),
          spent_output AS NOT MATERIALIZED (
            SELECT
              input.outpoint_transaction_hash AS transaction_hash,
              input.outpoint_index AS output_index
            FROM public.input
            INNER JOIN spender_acceptance USING (transaction_internal_id)
            WHERE NOT (
              input.outpoint_transaction_hash = decode(repeat('00', 32), 'hex')
              AND input.outpoint_index = 4294967295
            )
            GROUP BY input.outpoint_transaction_hash, input.outpoint_index
            HAVING count(*) > 0
          ),
          node_utxo AS NOT MATERIALIZED (
            SELECT created_output.transaction_hash, created_output.output_index
            FROM created_output
            LEFT JOIN spent_output USING (transaction_hash, output_index)
            WHERE spent_output.transaction_hash IS NULL
          )
          INSERT INTO output_membership_backfill.node_utxo_stage
          SELECT
            transaction_hash,
            output_index
          FROM node_utxo
          LIMIT desired_row_ceiling + 1;
        ELSE
          WITH node_acceptance AS NOT MATERIALIZED (
            SELECT transaction_internal_id, transaction_hash
            FROM output_membership_backfill.acceptance_exception
            WHERE current_node_id = ANY(accepted_node_ids)
          ),
          created_output AS NOT MATERIALIZED (
            SELECT output.transaction_hash, output.output_index
            FROM node_acceptance
            INNER JOIN public.output USING (transaction_hash)
          ),
          spent_output AS NOT MATERIALIZED (
            SELECT
              input.outpoint_transaction_hash AS transaction_hash,
              input.outpoint_index AS output_index
            FROM public.input
            INNER JOIN node_acceptance USING (transaction_internal_id)
            WHERE NOT (
              input.outpoint_transaction_hash = decode(repeat('00', 32), 'hex')
              AND input.outpoint_index = 4294967295
            )
            GROUP BY input.outpoint_transaction_hash, input.outpoint_index
            HAVING count(*) > 0
          ),
          node_utxo AS NOT MATERIALIZED (
            SELECT created_output.transaction_hash, created_output.output_index
            FROM created_output
            LEFT JOIN spent_output USING (transaction_hash, output_index)
            WHERE spent_output.transaction_hash IS NULL
          )
          INSERT INTO output_membership_backfill.node_utxo_stage
          SELECT
            transaction_hash,
            output_index
          FROM node_utxo
          LIMIT desired_row_ceiling + 1;
        END IF;

        SELECT count(*) INTO source_count
          FROM output_membership_backfill.node_utxo_stage;
        IF source_count > desired_row_ceiling THEN
          RAISE EXCEPTION
            'node % UTXO source exceeded the % row ceiling',
            current_node_id, desired_row_ceiling;
        END IF;
        INSERT INTO output_membership_backfill.desired_nondefault
        SELECT
          transaction_hash,
          output_index,
          ARRAY[default_node_id]::integer[],
          ARRAY[current_node_id]::integer[]
        FROM output_membership_backfill.node_utxo_stage
        ON CONFLICT (transaction_hash, output_index) DO UPDATE
          SET unspent_node_ids = ARRAY(
            SELECT DISTINCT node_id
            FROM unnest(
              desired_nondefault.unspent_node_ids
              || excluded.unspent_node_ids
            ) AS node_id
            ORDER BY node_id
          )::integer[];
        GET DIAGNOSTICS updated_count = ROW_COUNT;

        SELECT count(*) INTO source_count
          FROM output_membership_backfill.desired_nondefault;
        IF source_count > desired_row_ceiling THEN
          RAISE EXCEPTION
            'desired nondefault source exceeded the % row ceiling',
            desired_row_ceiling;
        END IF;
        UPDATE output_membership_backfill.target_node
          SET status = 'complete', rows_written = updated_count,
              finished_at = clock_timestamp()
          WHERE node_internal_id = current_node_id AND status = 'pending';
        TRUNCATE output_membership_backfill.node_utxo_stage;
        COMMIT;

        IF current_setting(
          'output_membership_backfill.test_fail_during_node', true
        ) = 'on' THEN
          RAISE EXCEPTION 'requested fault during node target build';
        END IF;
      END LOOP;

      UPDATE output_membership_backfill.state
        SET target_stage = 'nodes-ready', updated_at = clock_timestamp()
        WHERE id AND target_stage = 'nodes-building'
          AND NOT EXISTS (
            SELECT 1 FROM output_membership_backfill.target_node
            WHERE status <> 'complete'
          );
      IF NOT FOUND THEN
        RAISE EXCEPTION 'node target stages are incomplete';
      END IF;
      COMMIT;
      current_target_stage := 'nodes-ready';
    END IF;

    IF current_target_stage = 'nodes-ready' THEN
      PERFORM set_config('work_mem', '4GB', true);
      PERFORM set_config('jit', 'off', true);
      PERFORM set_config('max_parallel_workers_per_gather', '0', true);
      PERFORM set_config('enable_nestloop', 'off', true);
      PERFORM set_config('enable_hashjoin', 'off', true);
      PERFORM set_config('enable_mergejoin', 'on', true);
      PERFORM set_config('enable_seqscan', 'off', true);

      /* Exact non-default accepted arrays, preserving any UTXO memberships. */
      INSERT INTO output_membership_backfill.desired_nondefault
      SELECT
        output.transaction_hash,
        output.output_index,
        exception.accepted_node_ids,
        ARRAY[]::integer[]
      FROM public.output
      INNER JOIN output_membership_backfill.acceptance_exception exception
        USING (transaction_hash)
      ON CONFLICT (transaction_hash, output_index) DO UPDATE
        SET accepted_node_ids = excluded.accepted_node_ids;

      /* Outputs of unaccepted transactions must be corrected from the heap default. */
      INSERT INTO output_membership_backfill.desired_nondefault
      SELECT
        output.transaction_hash,
        output.output_index,
        ARRAY[]::integer[],
        ARRAY[]::integer[]
      FROM public.output
      LEFT JOIN output_membership_backfill.accepted_transaction accepted
        USING (transaction_hash)
      WHERE accepted.transaction_hash IS NULL
      ON CONFLICT (transaction_hash, output_index) DO UPDATE
        SET accepted_node_ids = excluded.accepted_node_ids;

      SELECT count(*) INTO source_count
        FROM output_membership_backfill.desired_nondefault;
      IF source_count > desired_row_ceiling THEN
        RAISE EXCEPTION
          'desired nondefault source exceeded the % row ceiling',
          desired_row_ceiling;
      END IF;
      ANALYZE output_membership_backfill.desired_nondefault;
      UPDATE output_membership_backfill.state
        SET target_stage = 'source-ready', updated_at = clock_timestamp()
        WHERE id AND target_stage = 'nodes-ready';
      IF NOT FOUND THEN
        RAISE EXCEPTION 'fast-backfill source stage changed unexpectedly';
      END IF;
      COMMIT;
      current_target_stage := 'source-ready';

      IF current_setting(
        'output_membership_backfill.test_fail_after_source', true
      ) = 'on' THEN
        RAISE EXCEPTION 'requested fault after sparse source build';
      END IF;
    END IF;

    IF current_target_stage = 'source-ready' THEN
      TRUNCATE output_membership_backfill.desired_output;
      PERFORM set_config('work_mem', '4GB', true);
      PERFORM set_config('hash_mem_multiplier', '2', true);
      PERFORM set_config('jit', 'off', true);
      PERFORM set_config('max_parallel_workers_per_gather', '0', true);
      PERFORM set_config('enable_nestloop', 'off', true);
      PERFORM set_config('enable_mergejoin', 'off', true);
      PERFORM set_config('enable_hashjoin', 'on', true);
      PERFORM set_config('enable_seqscan', 'on', true);

      WITH computed AS NOT MATERIALIZED (
        SELECT
          output.ctid AS target_ctid,
          output.transaction_hash,
          output.output_index,
          output.accepted_node_ids AS old_accepted_node_ids,
          output.unspent_node_ids AS old_unspent_node_ids,
          coalesce(
            desired.accepted_node_ids,
            ARRAY[default_node_id]::integer[]
          ) AS accepted_node_ids,
          CASE
            WHEN octet_length(output.locking_bytecode) > 0
              AND get_byte(output.locking_bytecode, 0) = 106
              THEN ARRAY[]::integer[]
            ELSE coalesce(desired.unspent_node_ids, ARRAY[]::integer[])
          END AS unspent_node_ids
        FROM public.output
        LEFT JOIN output_membership_backfill.desired_nondefault desired
          USING (transaction_hash, output_index)
      )
      INSERT INTO output_membership_backfill.desired_output
      SELECT
        target_ctid, transaction_hash, output_index,
        accepted_node_ids, unspent_node_ids
      FROM computed
      WHERE ROW(old_accepted_node_ids, old_unspent_node_ids)
        IS DISTINCT FROM ROW(accepted_node_ids, unspent_node_ids)
      LIMIT desired_row_ceiling + 1;

      SELECT count(*) INTO source_count
        FROM output_membership_backfill.desired_output;
      IF source_count > desired_row_ceiling THEN
        RAISE EXCEPTION
          'desired output exceeded the % row ceiling; retry with a larger proven budget',
          desired_row_ceiling;
      END IF;
      UPDATE output_membership_backfill.state
        SET target_stage = 'target-built', source_rows = source_count,
            next_target_heap_block = output_heap_blocks,
            updated_at = clock_timestamp()
        WHERE id AND phase = 'target-building'
          AND target_stage = 'source-ready';
      IF NOT FOUND THEN
        RAISE EXCEPTION 'fast-backfill target stage changed unexpectedly';
      END IF;
      COMMIT;

      IF current_setting(
        'output_membership_backfill.test_fail_during_target', true
      ) = 'on' THEN
        RAISE EXCEPTION 'requested fault during target build';
      END IF;
      current_target_stage := 'target-built';
    END IF;

    IF current_target_stage = 'target-built' THEN
      ANALYZE output_membership_backfill.desired_output;
      UPDATE output_membership_backfill.state
        SET phase = 'target-ready', target_stage = 'complete',
            target_built_at = clock_timestamp(), updated_at = clock_timestamp()
        WHERE id AND phase = 'target-building'
          AND target_stage = 'target-built';
      IF NOT FOUND THEN
        RAISE EXCEPTION 'fast-backfill phase changed while completing target';
      END IF;
      COMMIT;
    END IF;
  END IF;

  LOOP
    SELECT next_output_heap_block, output_heap_blocks
      INTO STRICT start_block, final_block
      FROM output_membership_backfill.state
      WHERE id
      FOR UPDATE;

    EXIT WHEN start_block >= final_block;
    end_block := least(start_block + batch_heap_blocks, final_block);
    batch_started := clock_timestamp();
    PERFORM set_config('enable_nestloop', 'on', true);
    PERFORM set_config('enable_mergejoin', 'off', true);
    PERFORM set_config('enable_hashjoin', 'off', true);

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

  PERFORM set_config('enable_nestloop', 'off', true);
  PERFORM set_config('enable_hashjoin', 'on', true);

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
