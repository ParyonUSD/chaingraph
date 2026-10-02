CREATE SCHEMA output_membership;

CREATE TABLE output_membership.state (
  id boolean PRIMARY KEY DEFAULT true CHECK (id),
  phase text NOT NULL,
  ready boolean NOT NULL DEFAULT false,
  default_node_internal_id integer REFERENCES public.node(internal_id),
  original_heap_blocks bigint NOT NULL,
  next_heap_block bigint NOT NULL DEFAULT 0,
  rows_updated bigint NOT NULL DEFAULT 0,
  started_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamp with time zone NOT NULL DEFAULT clock_timestamp(),
  validated_at timestamp with time zone,
  validation_details jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE output_membership.backfill_batch (
  start_heap_block bigint PRIMARY KEY,
  end_heap_block bigint NOT NULL,
  started_at timestamp with time zone NOT NULL,
  finished_at timestamp with time zone,
  rows_scanned bigint,
  rows_updated bigint,
  duration interval
);

DO $migration$
DECLARE
  default_node integer;
  heap_blocks bigint;
BEGIN
  /*
   * Existing databases usually contain one node which accepted nearly every
   * historical output. Using that node as the historical column default lets
   * PostgreSQL add accepted_node_ids without rewriting the output heap. The
   * resumable backfill below replaces every exception with its exact array.
   */
  SELECT node_internal_id
    INTO default_node
    FROM node_block
    GROUP BY node_internal_id
    ORDER BY count(*) DESC, node_internal_id
    LIMIT 1;

  IF default_node IS NULL THEN
    ALTER TABLE public.output
      ADD COLUMN accepted_node_ids integer[] NOT NULL DEFAULT ARRAY[]::integer[],
      ADD COLUMN unspent_node_ids integer[] NOT NULL DEFAULT ARRAY[]::integer[];
  ELSE
    EXECUTE format(
      'ALTER TABLE public.output '
      'ADD COLUMN accepted_node_ids integer[] NOT NULL DEFAULT %L::integer[], '
      'ADD COLUMN unspent_node_ids integer[] NOT NULL DEFAULT ARRAY[]::integer[]',
      ARRAY[default_node]::integer[]
    );
  END IF;

  /* New outputs begin empty; statement triggers populate exact membership. */
  ALTER TABLE public.output
    ALTER COLUMN accepted_node_ids SET DEFAULT ARRAY[]::integer[],
    ALTER COLUMN unspent_node_ids SET DEFAULT ARRAY[]::integer[];

  heap_blocks := (
    pg_relation_size('public.output') +
    current_setting('block_size')::bigint - 1
  ) / current_setting('block_size')::bigint;

  INSERT INTO output_membership.state (
    id,
    phase,
    default_node_internal_id,
    original_heap_blocks
  ) VALUES (
    true,
    CASE WHEN heap_blocks = 0 THEN 'awaiting-initial-sync' ELSE 'pending-backfill' END,
    default_node,
    heap_blocks
  );
END
$migration$;

COMMENT ON COLUMN public.output.accepted_node_ids IS
  'Sorted internal IDs of nodes which currently accept the creating transaction, either in the mempool or an accepted block.';
COMMENT ON COLUMN public.output.unspent_node_ids IS
  'Sorted internal IDs of nodes which accept the creating transaction and do not accept a transaction spending this output. OP_RETURN outputs are always excluded.';

ALTER TABLE public.output
  ADD CONSTRAINT output_node_membership_valid
  CHECK (unspent_node_ids <@ accepted_node_ids) NOT VALID;

/*
 * Serialize writes which can change derived membership. This prevents two
 * concurrent ingestion transactions from calculating arrays from different
 * READ COMMITTED snapshots and overwriting each other.
 */
CREATE FUNCTION output_membership.lock_writer() RETURNS trigger
  LANGUAGE plpgsql
AS $lock_writer$
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION
      'output membership maintenance requires READ COMMITTED isolation';
  END IF;
  PERFORM pg_advisory_xact_lock(20261001, 1);
  RETURN NULL;
END
$lock_writer$;

CREATE FUNCTION output_membership.reject_truncate() RETURNS trigger
  LANGUAGE plpgsql
AS $reject_truncate$
BEGIN
  RAISE EXCEPTION
    'TRUNCATE requires rebuilding output node membership';
END
$reject_truncate$;

CREATE FUNCTION output_membership.refresh(
  transaction_internal_ids bigint[],
  direct_outputs jsonb
) RETURNS void
  LANGUAGE sql
  SET plan_cache_mode = 'force_generic_plan'
AS $refresh$
WITH changed_hashes AS MATERIALIZED (
  SELECT hash
    FROM public.transaction
    WHERE internal_id = ANY(transaction_internal_ids)
),
affected AS MATERIALIZED (
  SELECT output.transaction_hash, output.output_index
    FROM public.output
    WHERE output.transaction_hash IN (SELECT hash FROM changed_hashes)
  UNION
  SELECT input.outpoint_transaction_hash, input.outpoint_index
    FROM public.input
    WHERE input.transaction_internal_id = ANY(transaction_internal_ids)
      AND NOT (
        input.outpoint_transaction_hash = '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
        AND input.outpoint_index = 4294967295
      )
  UNION
  SELECT
    (item ->> 'transaction_hash')::bytea,
    (item ->> 'output_index')::bigint
    FROM jsonb_array_elements(direct_outputs) AS item
    WHERE NOT (
      (item ->> 'transaction_hash')::bytea = '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
      AND (item ->> 'output_index')::bigint = 4294967295
    )
),
creators AS MATERIALIZED (
  SELECT DISTINCT
    affected.transaction_hash,
    transaction.internal_id AS transaction_internal_id
    FROM affected
    INNER JOIN public.transaction
      ON transaction.hash = affected.transaction_hash
),
spenders AS MATERIALIZED (
  SELECT DISTINCT
    affected.transaction_hash,
    affected.output_index,
    input.transaction_internal_id
    FROM affected
    INNER JOIN public.input
      ON input.outpoint_transaction_hash = affected.transaction_hash
      AND input.outpoint_index = affected.output_index
),
relevant_transactions AS MATERIALIZED (
  SELECT transaction_internal_id FROM creators
  UNION
  SELECT transaction_internal_id FROM spenders
),
membership AS MATERIALIZED (
  SELECT
    accepted.transaction_internal_id,
    array_agg(accepted.node_internal_id ORDER BY accepted.node_internal_id)::integer[] AS node_ids
    FROM (
      SELECT
        node_transaction.transaction_internal_id,
        node_transaction.node_internal_id
        FROM relevant_transactions
        INNER JOIN public.node_transaction USING (transaction_internal_id)
      UNION
      SELECT
        block_transaction.transaction_internal_id,
        node_block.node_internal_id
        FROM relevant_transactions
        INNER JOIN public.block_transaction USING (transaction_internal_id)
        INNER JOIN public.node_block USING (block_internal_id)
    ) AS accepted
    GROUP BY accepted.transaction_internal_id
),
creator_state AS MATERIALIZED (
  SELECT
    creators.transaction_hash,
    coalesce(membership.node_ids, ARRAY[]::integer[]) AS accepted_node_ids
    FROM creators
    LEFT JOIN membership USING (transaction_internal_id)
),
spent_state AS MATERIALIZED (
  SELECT
    spenders.transaction_hash,
    spenders.output_index,
    array_agg(DISTINCT node_id ORDER BY node_id)::integer[] AS spent_node_ids
    FROM spenders
    INNER JOIN membership USING (transaction_internal_id)
    CROSS JOIN LATERAL unnest(membership.node_ids) AS node_id
    GROUP BY spenders.transaction_hash, spenders.output_index
),
computed AS MATERIALIZED (
  SELECT
    affected.transaction_hash,
    affected.output_index,
    creator_state.accepted_node_ids,
    CASE
      /* OP_RETURN (0x6a) outputs are provably unspendable, not UTXOs. */
      WHEN octet_length(output.locking_bytecode) > 0
        AND get_byte(output.locking_bytecode, 0) = 106
        THEN ARRAY[]::integer[]
      ELSE ARRAY(
        SELECT node_id
          FROM unnest(creator_state.accepted_node_ids) AS node_id
          WHERE NOT node_id = ANY(
            coalesce(spent_state.spent_node_ids, ARRAY[]::integer[])
          )
          ORDER BY node_id
      )::integer[]
    END AS unspent_node_ids
    FROM affected
    INNER JOIN public.output USING (transaction_hash, output_index)
    INNER JOIN creator_state USING (transaction_hash)
    LEFT JOIN spent_state USING (transaction_hash, output_index)
)
UPDATE public.output
  SET
    accepted_node_ids = computed.accepted_node_ids,
    unspent_node_ids = computed.unspent_node_ids
  FROM computed
  WHERE output.transaction_hash = computed.transaction_hash
    AND output.output_index = computed.output_index
    AND ROW(output.accepted_node_ids, output.unspent_node_ids)
      IS DISTINCT FROM
      ROW(computed.accepted_node_ids, computed.unspent_node_ids);
$refresh$;

CREATE FUNCTION output_membership.changed() RETURNS trigger
  LANGUAGE plpgsql
AS $changed$
DECLARE
  rows_sql text;
  transaction_ids_sql text;
  direct_outputs_sql text := '''[]''::jsonb';
  transaction_ids bigint[];
  direct_outputs jsonb;
BEGIN
  /* Skip the output UPDATE issued by output_membership.refresh itself. */
  IF TG_TABLE_NAME = 'output' AND TG_OP = 'UPDATE' AND pg_trigger_depth() > 1 THEN
    RETURN NULL;
  END IF;

  /* The resumable historical backfill already writes exact values. */
  IF current_setting('output_membership.backfill', true) = 'on' THEN
    RETURN NULL;
  END IF;

  rows_sql := CASE TG_OP
    WHEN 'INSERT' THEN 'SELECT * FROM new_rows'
    WHEN 'DELETE' THEN 'SELECT * FROM old_rows'
    ELSE 'SELECT * FROM old_rows UNION ALL SELECT * FROM new_rows'
  END;

  IF TG_TABLE_NAME = 'node_transaction' THEN
    transaction_ids_sql := 'SELECT transaction_internal_id FROM changed_rows';
  ELSIF TG_TABLE_NAME = 'node_block' THEN
    transaction_ids_sql :=
      'SELECT block_transaction.transaction_internal_id '
      'FROM public.block_transaction '
      'INNER JOIN changed_rows USING (block_internal_id)';
  ELSIF TG_TABLE_NAME = 'block_transaction' THEN
    transaction_ids_sql := 'SELECT transaction_internal_id FROM changed_rows';
  ELSIF TG_TABLE_NAME = 'input' THEN
    transaction_ids_sql := 'SELECT transaction_internal_id FROM changed_rows';
    direct_outputs_sql :=
      '(SELECT coalesce(jsonb_agg(jsonb_build_object('
      '''transaction_hash'', outpoint_transaction_hash, '
      '''output_index'', outpoint_index)), ''[]''::jsonb) FROM changed_rows)';
  ELSIF TG_TABLE_NAME = 'output' THEN
    transaction_ids_sql :=
      'SELECT transaction.internal_id FROM public.transaction '
      'INNER JOIN changed_rows '
      'ON changed_rows.transaction_hash = transaction.hash';
    direct_outputs_sql :=
      '(SELECT coalesce(jsonb_agg(jsonb_build_object('
      '''transaction_hash'', transaction_hash, '
      '''output_index'', output_index)), ''[]''::jsonb) FROM changed_rows)';
  ELSE
    RAISE EXCEPTION 'unsupported output membership source table: %', TG_TABLE_NAME;
  END IF;

  EXECUTE
    'WITH changed_rows AS MATERIALIZED (' || rows_sql || ') '
    'SELECT ARRAY(SELECT DISTINCT * FROM (' ||
      transaction_ids_sql || ') AS changed_transactions), ' ||
      direct_outputs_sql
    INTO transaction_ids, direct_outputs;

  IF coalesce(cardinality(transaction_ids), 0) > 0 OR direct_outputs <> '[]'::jsonb THEN
    PERFORM output_membership.refresh(transaction_ids, direct_outputs);
  END IF;
  RETURN NULL;
END
$changed$;

/*
 * Process the heap as it existed when the migration was installed. Updates
 * move tuples, so the fixed upper bound prevents revisiting rewritten rows.
 * The cursor and each completed batch are durable, making CALL resumable.
 */
CREATE PROCEDURE output_membership.backfill(batch_heap_blocks bigint DEFAULT 20000)
  LANGUAGE plpgsql
AS $backfill$
DECLARE
  start_block bigint;
  end_block bigint;
  final_block bigint;
  scanned bigint;
  updated bigint;
  batch_started timestamp with time zone;
BEGIN
  IF batch_heap_blocks < 1 OR batch_heap_blocks > 200000 THEN
    RAISE EXCEPTION 'batch_heap_blocks must be between 1 and 200000';
  END IF;

  PERFORM pg_advisory_lock(20261001, 1);
  PERFORM set_config('output_membership.backfill', 'on', false);

  UPDATE output_membership.state
    SET phase = 'backfilling', ready = false, updated_at = clock_timestamp()
    WHERE id;
  COMMIT;

  LOOP
    SELECT next_heap_block, original_heap_blocks
      INTO start_block, final_block
      FROM output_membership.state
      WHERE id
      FOR UPDATE;

    EXIT WHEN start_block >= final_block;
    end_block := least(start_block + batch_heap_blocks, final_block);
    batch_started := clock_timestamp();

    INSERT INTO output_membership.backfill_batch (
      start_heap_block,
      end_heap_block,
      started_at
    ) VALUES (
      start_block,
      end_block,
      batch_started
    )
    ON CONFLICT (start_heap_block) DO UPDATE SET
      end_heap_block = excluded.end_heap_block,
      started_at = excluded.started_at,
      finished_at = NULL,
      rows_scanned = NULL,
      rows_updated = NULL,
      duration = NULL;
    COMMIT;

    WITH batch AS MATERIALIZED (
      SELECT
        ctid AS source_ctid,
        transaction_hash,
        output_index,
        locking_bytecode
        FROM public.output
        WHERE ctid >= format('(%s,0)', start_block)::tid
          AND ctid < format('(%s,0)', end_block)::tid
    ),
    creators AS MATERIALIZED (
      SELECT DISTINCT
        batch.transaction_hash,
        transaction.internal_id AS transaction_internal_id
        FROM batch
        INNER JOIN public.transaction
          ON transaction.hash = batch.transaction_hash
    ),
    spenders AS MATERIALIZED (
      SELECT DISTINCT
        batch.source_ctid,
        input.transaction_internal_id
        FROM batch
        INNER JOIN public.input
          ON input.outpoint_transaction_hash = batch.transaction_hash
          AND input.outpoint_index = batch.output_index
    ),
    relevant_transactions AS MATERIALIZED (
      SELECT transaction_internal_id FROM creators
      UNION
      SELECT transaction_internal_id FROM spenders
    ),
    membership AS MATERIALIZED (
      SELECT
        accepted.transaction_internal_id,
        array_agg(accepted.node_internal_id ORDER BY accepted.node_internal_id)::integer[] AS node_ids
        FROM (
          SELECT
            node_transaction.transaction_internal_id,
            node_transaction.node_internal_id
            FROM relevant_transactions
            INNER JOIN public.node_transaction USING (transaction_internal_id)
          UNION
          SELECT
            block_transaction.transaction_internal_id,
            node_block.node_internal_id
            FROM relevant_transactions
            INNER JOIN public.block_transaction USING (transaction_internal_id)
            INNER JOIN public.node_block USING (block_internal_id)
        ) AS accepted
        GROUP BY accepted.transaction_internal_id
    ),
    creator_state AS MATERIALIZED (
      SELECT
        creators.transaction_hash,
        coalesce(membership.node_ids, ARRAY[]::integer[]) AS accepted_node_ids
        FROM creators
        LEFT JOIN membership USING (transaction_internal_id)
    ),
    spent_state AS MATERIALIZED (
      SELECT
        spenders.source_ctid,
        array_agg(DISTINCT node_id ORDER BY node_id)::integer[] AS spent_node_ids
        FROM spenders
        INNER JOIN membership USING (transaction_internal_id)
        CROSS JOIN LATERAL unnest(membership.node_ids) AS node_id
        GROUP BY spenders.source_ctid
    ),
    computed AS MATERIALIZED (
      SELECT
        batch.source_ctid,
        creator_state.accepted_node_ids,
        CASE
          WHEN octet_length(batch.locking_bytecode) > 0
            AND get_byte(batch.locking_bytecode, 0) = 106
            THEN ARRAY[]::integer[]
          ELSE ARRAY(
            SELECT node_id
              FROM unnest(creator_state.accepted_node_ids) AS node_id
              WHERE NOT node_id = ANY(
                coalesce(spent_state.spent_node_ids, ARRAY[]::integer[])
              )
              ORDER BY node_id
          )::integer[]
        END AS unspent_node_ids
        FROM batch
        INNER JOIN creator_state USING (transaction_hash)
        LEFT JOIN spent_state USING (source_ctid)
    ),
    changed AS (
      UPDATE public.output
        SET
          accepted_node_ids = computed.accepted_node_ids,
          unspent_node_ids = computed.unspent_node_ids
        FROM computed
        WHERE output.ctid = computed.source_ctid
          AND ROW(output.accepted_node_ids, output.unspent_node_ids)
            IS DISTINCT FROM
            ROW(computed.accepted_node_ids, computed.unspent_node_ids)
        RETURNING 1
    )
    SELECT
      (SELECT count(*) FROM batch),
      (SELECT count(*) FROM changed)
      INTO scanned, updated;

    UPDATE output_membership.backfill_batch
      SET
        finished_at = clock_timestamp(),
        rows_scanned = scanned,
        rows_updated = updated,
        duration = clock_timestamp() - batch_started
      WHERE start_heap_block = start_block;

    UPDATE output_membership.state
      SET
        next_heap_block = end_block,
        rows_updated = rows_updated + updated,
        updated_at = clock_timestamp()
      WHERE id AND next_heap_block = start_block;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'output membership backfill cursor changed unexpectedly';
    END IF;
    COMMIT;
  END LOOP;

  UPDATE output_membership.state
    SET phase = 'backfilled', updated_at = clock_timestamp()
    WHERE id;
  COMMIT;

  PERFORM set_config('output_membership.backfill', 'off', false);
  PERFORM pg_advisory_unlock(20261001, 1);
END
$backfill$;

CREATE FUNCTION public.unspent_output(node_name text) RETURNS SETOF public.output
  LANGUAGE plpgsql
  STABLE
  PARALLEL RESTRICTED
  SET plan_cache_mode = 'auto'
AS $unspent_output$
BEGIN
  IF NOT (SELECT ready FROM output_membership.state WHERE id) THEN
    RAISE EXCEPTION 'output node membership is not ready';
  END IF;
  RETURN QUERY
    SELECT output.*
      FROM public.output
      CROSS JOIN public.node
      WHERE node.name = $1
        AND cardinality(output.unspent_node_ids) > 0
        AND node.internal_id = ANY(output.unspent_node_ids);
END
$unspent_output$;

CREATE FUNCTION public.accepted_output(node_name text) RETURNS SETOF public.output
  LANGUAGE plpgsql
  STABLE
  PARALLEL RESTRICTED
  SET plan_cache_mode = 'auto'
AS $accepted_output$
BEGIN
  IF NOT (SELECT ready FROM output_membership.state WHERE id) THEN
    RAISE EXCEPTION 'output node membership is not ready';
  END IF;
  RETURN QUERY
    SELECT output.*
      FROM public.output
      CROSS JOIN public.node
      WHERE node.name = $1
        AND cardinality(output.accepted_node_ids) > 0
        AND node.internal_id = ANY(output.accepted_node_ids);
END
$accepted_output$;

COMMENT ON FUNCTION public.unspent_output(text) IS
  'Return outputs which are currently unspent from the perspective of the named node. OP_RETURN outputs are excluded.';
COMMENT ON FUNCTION public.accepted_output(text) IS
  'Return outputs created by transactions currently accepted by the named node.';

DO $triggers$
DECLARE
  source_table text;
BEGIN
  FOREACH source_table IN ARRAY ARRAY[
    'transaction',
    'block',
    'output',
    'input',
    'block_transaction',
    'node_block',
    'node_transaction'
  ] LOOP
    EXECUTE format(
      'CREATE TRIGGER trigger_output_membership_lock '
      'BEFORE INSERT OR UPDATE OR DELETE ON public.%I '
      'FOR EACH STATEMENT EXECUTE FUNCTION output_membership.lock_writer()',
      source_table
    );
    EXECUTE format(
      'CREATE TRIGGER trigger_output_membership_reject_truncate '
      'BEFORE TRUNCATE ON public.%I '
      'FOR EACH STATEMENT EXECUTE FUNCTION output_membership.reject_truncate()',
      source_table
    );
  END LOOP;

  FOREACH source_table IN ARRAY ARRAY[
    'output',
    'input',
    'block_transaction',
    'node_block',
    'node_transaction'
  ] LOOP
    EXECUTE format(
      'CREATE TRIGGER trigger_zz_output_membership_insert '
      'AFTER INSERT ON public.%I REFERENCING NEW TABLE AS new_rows '
      'FOR EACH STATEMENT EXECUTE FUNCTION output_membership.changed()',
      source_table
    );
    EXECUTE format(
      'CREATE TRIGGER trigger_zz_output_membership_delete '
      'AFTER DELETE ON public.%I REFERENCING OLD TABLE AS old_rows '
      'FOR EACH STATEMENT EXECUTE FUNCTION output_membership.changed()',
      source_table
    );
    EXECUTE format(
      'CREATE TRIGGER trigger_zz_output_membership_update '
      'AFTER UPDATE ON public.%I '
      'REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows '
      'FOR EACH STATEMENT EXECUTE FUNCTION output_membership.changed()',
      source_table
    );
  END LOOP;
END
$triggers$;
