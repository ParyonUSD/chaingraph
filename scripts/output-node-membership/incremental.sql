/* Experimental primitive, not a migration or production enablement script.
 * Requires the membership columns, exact initial arrays, and existing indexes:
 * transaction_pkey/hash_key, output_pkey, input_pkey, spent_by_index,
 * node_transaction_pkey, block_inclusions_index, node_block_pkey.
 * Legacy membership statement triggers MUST be disabled by the integration.
 * No backfill, finalizer, readiness change, or automatic publication is installed.
 * Collector triggers only queue changes in opted-in caller transactions.
 *
 * HARD CALLER CONTRACT (the function cannot prove when a row was locked):
 * BEGIN at READ COMMITTED; call lock_node in a SEPARATE statement BEFORE any
 * normalized acceptance/topology writes; write normalized state; refresh in
 * another statement; COMMIT. Hold the node lock throughout. Every writer for
 * that node, including late transaction/input/output ingestion, must obey this.
 * Acquire multiple node locks in ascending node ID order. Collect the COMPLETE
 * output target set for the transaction and call refresh ONCE PER NODE. Global
 * topology changes must refresh EVERY affected node; immutable transaction and
 * input data cannot change under another writer's acceptance calculation.
 * Pass OLD as well as NEW txids/outpoints on topology updates/deletions. A
 * deleted transaction's outputs need explicit direct_outpoints (no hash lookup
 * remains). Late creator ingestion passes the creator txid and affected nodes.
 *
 * Output keys are locked in bytea/hash then output_index order. Do not prelock
 * outputs in another order. Retry the WHOLE transaction on 40P01 / 40001, never
 * only the refresh; exceptions must roll back normalized writes too. Repeated
 * calls or preexisting row locks can still deadlock. Different node writers
 * contend only for shared outputs, never a global publication lock.
 * Arrays must already be sorted, unique, nonnull, one-dimensional (or empty),
 * with lower bound 1 and no null elements; helpers do not deduplicate.
 */
CREATE SCHEMA IF NOT EXISTS output_membership;

CREATE OR REPLACE FUNCTION output_membership.node_add_if_missing(
  node_ids integer[], node_id integer
) RETURNS integer[] LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $fn$
DECLARE
  position integer;
BEGIN
  IF node_id = ANY(node_ids) THEN RETURN node_ids; END IF;
  FOR position IN 1..cardinality(node_ids) LOOP
    IF node_ids[position] > node_id THEN
      RETURN node_ids[1:position - 1] || node_id || node_ids[position:cardinality(node_ids)];
    END IF;
  END LOOP;
  RETURN node_ids || node_id;
END
$fn$;

CREATE OR REPLACE FUNCTION output_membership.node_remove(
  node_ids integer[], node_id integer
) RETURNS integer[] LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $fn$ SELECT array_remove(node_ids, node_id) $fn$;

-- Ordinary per-node publication lock; NO KEY UPDATE avoids blocking FK reads.
CREATE OR REPLACE FUNCTION output_membership.lock_node(node_id integer)
RETURNS void LANGUAGE plpgsql VOLATILE
AS $fn$
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'incremental membership requires READ COMMITTED';
  END IF;
  PERFORM 1 FROM public.node WHERE internal_id = node_id FOR NO KEY UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'unknown node %', node_id; END IF;
END
$fn$;

CREATE OR REPLACE FUNCTION output_membership.refresh_for_node(
  node_id integer,
  transaction_internal_ids bigint[],
  direct_outpoints jsonb DEFAULT '[]'::jsonb
) RETURNS bigint LANGUAGE plpgsql VOLATILE
SET plan_cache_mode = 'force_custom_plan'
SET enable_seqscan = 'off'
AS $fn$
DECLARE
  locked_tids tid[];
  updated bigint := 0;
BEGIN
  IF node_id IS NULL OR transaction_internal_ids IS NULL OR direct_outpoints IS NULL THEN
    RAISE EXCEPTION 'node, transaction IDs and direct outpoints must be nonnull';
  END IF;
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'incremental membership requires READ COMMITTED';
  END IF;
  -- No lock acquired here: acquiring it after normalized changes is too late.
  -- OFFSET 0 keeps each expansion correlated with one indexed key lookup.
  -- UNION deduplicates only the bounded target KEYS, never membership arrays.
  WITH ids AS MATERIALIZED (
      SELECT DISTINCT id FROM unnest(transaction_internal_ids) AS id WHERE id IS NOT NULL
    ), target_union AS (
      SELECT o.transaction_hash, o.output_index
      FROM ids
      CROSS JOIN LATERAL (
        SELECT hash FROM public.transaction WHERE internal_id = ids.id OFFSET 0
      ) AS t
      CROSS JOIN LATERAL (
        SELECT transaction_hash, output_index FROM public.output
        WHERE transaction_hash = t.hash OFFSET 0
      ) AS o
      UNION
      SELECT i.outpoint_transaction_hash, i.outpoint_index
      FROM ids
      CROSS JOIN LATERAL (
        SELECT outpoint_transaction_hash, outpoint_index FROM public.input
        WHERE transaction_internal_id = ids.id OFFSET 0
      ) AS i
      UNION
      SELECT (item ->> 'transaction_hash')::bytea, (item ->> 'output_index')::bigint
      FROM jsonb_array_elements(direct_outpoints) AS item
    ), targets AS MATERIALIZED (
    SELECT transaction_hash, output_index FROM target_union
    WHERE transaction_hash IS NOT NULL AND output_index IS NOT NULL
      AND NOT (transaction_hash = decode(repeat('00', 32), 'hex') AND output_index = 4294967295)
    ORDER BY transaction_hash, output_index
  )
  -- Correlation barrier + sorted materialized keys give ordered singleton index
  -- locks, but this is ONE SQL statement regardless of the number of outputs.
  -- The executor resolves concurrent updates before returning the latest CTID.
  SELECT coalesce(array_agg(o.row_tid), ARRAY[]::tid[]) INTO locked_tids
  FROM targets AS target
  CROSS JOIN LATERAL (
    SELECT o.ctid AS row_tid FROM public.output AS o
    WHERE o.transaction_hash = target.transaction_hash AND o.output_index = target.output_index
    FOR NO KEY UPDATE OFFSET 0
  ) AS o;
  IF cardinality(locked_tids) = 0 THEN RETURN 0; END IF;

  -- Fresh snapshot after output lock waits. CTIDs remain valid because all
  -- tuples are locked through commit. Acceptance is calculated only for node_id.
  -- The extra CTID ANY restriction bounds the UPDATE destination independently
  -- of join-order estimates. enable_seqscan=off is local to this function:
  -- required indexes remain a caller precondition, not newly created here.
  WITH desired AS MATERIALIZED (
    SELECT row_tid,
      CASE WHEN acceptance.accepted
        THEN output_membership.node_add_if_missing(o.accepted_node_ids, node_id)
        ELSE output_membership.node_remove(o.accepted_node_ids, node_id) END AS accepted_node_ids,
      CASE WHEN acceptance.accepted AND substring(o.locking_bytecode FROM 1 FOR 1) <> '\x6a'::bytea
        AND NOT EXISTS (
          SELECT 1 FROM (
            -- This barrier prevents a hashed ALL-inputs spender subplan when
            -- the target list is large. Each outpoint remains an index probe.
            SELECT transaction_internal_id FROM public.input
            WHERE outpoint_transaction_hash = o.transaction_hash AND outpoint_index = o.output_index
            OFFSET 0
          ) AS i
          WHERE (EXISTS (
            SELECT 1 FROM public.node_transaction AS nt
            WHERE nt.transaction_internal_id = i.transaction_internal_id AND nt.node_internal_id = node_id
          ) OR EXISTS (
            SELECT 1 FROM public.block_transaction AS bt
            WHERE bt.transaction_internal_id = i.transaction_internal_id
              AND EXISTS (SELECT 1 FROM public.node_block AS nb
                WHERE nb.node_internal_id = node_id AND nb.block_internal_id = bt.block_internal_id)
          ))
        )
        THEN output_membership.node_add_if_missing(o.unspent_node_ids, node_id)
        ELSE output_membership.node_remove(o.unspent_node_ids, node_id) END AS unspent_node_ids
    FROM unnest(locked_tids) AS key(row_tid)
    CROSS JOIN LATERAL (
      SELECT * FROM public.output WHERE ctid = key.row_tid OFFSET 0
    ) AS o
    CROSS JOIN LATERAL (
      SELECT internal_id FROM public.transaction WHERE hash = o.transaction_hash OFFSET 0
    ) AS creator
    CROSS JOIN LATERAL (
      SELECT (EXISTS (
        SELECT 1 FROM public.node_transaction AS nt
        WHERE nt.transaction_internal_id = creator.internal_id AND nt.node_internal_id = node_id
      ) OR EXISTS (
        SELECT 1 FROM public.block_transaction AS bt
        WHERE bt.transaction_internal_id = creator.internal_id
          AND EXISTS (SELECT 1 FROM public.node_block AS nb
            WHERE nb.node_internal_id = node_id AND nb.block_internal_id = bt.block_internal_id)
      )) AS accepted OFFSET 0
    ) AS acceptance
  )
  UPDATE public.output AS destination
  SET accepted_node_ids = desired.accepted_node_ids, unspent_node_ids = desired.unspent_node_ids
  FROM desired
  WHERE destination.ctid = desired.row_tid AND destination.ctid = ANY(locked_tids)
    AND (destination.accepted_node_ids IS DISTINCT FROM desired.accepted_node_ids
      OR destination.unspent_node_ids IS DISTINCT FROM desired.unspent_node_ids);
  GET DIAGNOSTICS updated = ROW_COUNT;
  RETURN updated;
END
$fn$;

/* Queued integration API.
 * begin_membership_changes(); lock ALL known affected nodes in ascending order
 * BEFORE facts; write facts (including implicit archive/cascade changes); note
 * body/outpoint changes; finish_membership_changes(prelocked_node_ids); COMMIT.
 * If an implicit change touches an unlocked node, finish raises: roll back the
 * WHOLE transaction, discover/lock the wider node set, then retry. The supplied
 * IDs certify the caller's earlier locks; SQL cannot verify their chronology.
 * Finish prelocks the union of every group's outputs BEFORE refreshing any
 * group, so group order cannot reverse output lock order. Do not prelock output
 * rows outside this API or publish a partial group earlier in the transaction.
 * GUC output_membership.collect_changes='on' enables collectors; all other
 * values (including 'off' and 'deferred') skip them. Deferred facts must be
 * explicitly noted before publication, or left to an external exact rebuild.
 * Queues are session-private TEMP tables, cleared automatically at COMMIT and
 * explicitly after publication. Deduplication covers only this transaction's
 * keys. No readiness/default/legacy trigger state is modified by this script.
 */
CREATE OR REPLACE FUNCTION output_membership.begin_membership_changes()
RETURNS void LANGUAGE plpgsql VOLATILE
AS $fn$
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'incremental membership requires READ COMMITTED';
  END IF;
  IF current_setting('output_membership.collect_changes', true) = 'on' THEN
    RAISE EXCEPTION 'membership collection already active';
  END IF;
  CREATE TEMP TABLE IF NOT EXISTS output_membership_transaction_changes (
    node_internal_id integer NOT NULL,
    transaction_internal_id bigint NOT NULL,
    PRIMARY KEY (node_internal_id, transaction_internal_id)
  ) ON COMMIT DELETE ROWS;
  CREATE TEMP TABLE IF NOT EXISTS output_membership_outpoint_changes (
    node_internal_id integer NOT NULL,
    transaction_hash bytea NOT NULL,
    output_index bigint NOT NULL,
    PRIMARY KEY (node_internal_id, transaction_hash, output_index)
  ) ON COMMIT DELETE ROWS;
  IF EXISTS (SELECT 1 FROM pg_temp.output_membership_transaction_changes)
     OR EXISTS (SELECT 1 FROM pg_temp.output_membership_outpoint_changes) THEN
    RAISE EXCEPTION 'cannot restart membership collection with unpublished queued changes';
  END IF;
  PERFORM set_config('output_membership.collect_changes', 'on', true);
END
$fn$;

CREATE OR REPLACE FUNCTION output_membership.note_membership_changes(
  node_id integer,
  transaction_internal_ids bigint[],
  direct_outpoints jsonb DEFAULT '[]'::jsonb
) RETURNS void LANGUAGE plpgsql VOLATILE
AS $fn$
BEGIN
  IF current_setting('output_membership.collect_changes', true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION 'begin_membership_changes must precede noting changes';
  END IF;
  IF node_id IS NULL OR transaction_internal_ids IS NULL OR direct_outpoints IS NULL THEN
    RAISE EXCEPTION 'node, transaction IDs and direct outpoints must be nonnull';
  END IF;
  INSERT INTO pg_temp.output_membership_transaction_changes
    SELECT node_id, id FROM unnest(transaction_internal_ids) AS id WHERE id IS NOT NULL
    ON CONFLICT DO NOTHING;
  INSERT INTO pg_temp.output_membership_outpoint_changes
    SELECT node_id, (item ->> 'transaction_hash')::bytea, (item ->> 'output_index')::bigint
    FROM jsonb_array_elements(direct_outpoints) AS item
    ON CONFLICT DO NOTHING;
END
$fn$;

CREATE OR REPLACE FUNCTION output_membership.collect_membership_changes()
RETURNS trigger LANGUAGE plpgsql VOLATILE
SET enable_seqscan = 'off'
AS $fn$
DECLARE
  rows_sql text;
  changes_sql text;
BEGIN
  -- The skip precedes all temp-table access, including in uninitialized sessions.
  IF current_setting('output_membership.collect_changes', true) IS DISTINCT FROM 'on' THEN
    RETURN NULL;
  END IF;
  rows_sql := CASE TG_OP
    WHEN 'INSERT' THEN 'SELECT * FROM output_membership_new'
    WHEN 'DELETE' THEN 'SELECT * FROM output_membership_old'
    WHEN 'UPDATE' THEN 'SELECT * FROM output_membership_old UNION ALL SELECT * FROM output_membership_new'
  END;
  IF TG_TABLE_NAME = 'node_transaction' THEN
    changes_sql := format('SELECT change.node_internal_id, change.transaction_internal_id FROM (%s) AS change', rows_sql);
  ELSIF TG_TABLE_NAME = 'node_block' THEN
    changes_sql := format(
      'SELECT change.node_internal_id, inclusion.transaction_internal_id FROM (%s) AS change '
      'CROSS JOIN LATERAL (SELECT transaction_internal_id FROM public.block_transaction '
      'WHERE block_internal_id = change.block_internal_id OFFSET 0) AS inclusion', rows_sql);
  ELSE
    RAISE EXCEPTION 'unexpected membership collector table %', TG_TABLE_NAME;
  END IF;
  -- Transition rows are the only change source; the block expansion is indexed
  -- by each changed block, never an accepted-history/global relation rebuild.
  EXECUTE 'INSERT INTO pg_temp.output_membership_transaction_changes ' || changes_sql || ' ON CONFLICT DO NOTHING';
  RETURN NULL;
END
$fn$;

CREATE OR REPLACE FUNCTION output_membership.finish_membership_changes(
  locked_node_ids integer[]
) RETURNS bigint LANGUAGE plpgsql VOLATILE
SET plan_cache_mode = 'force_custom_plan'
SET enable_seqscan = 'off'
AS $fn$
DECLARE
  group_node integer;
  transaction_ids bigint[];
  direct_outpoints jsonb;
  updated bigint := 0;
BEGIN
  IF current_setting('output_membership.collect_changes', true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION 'begin_membership_changes must precede finish';
  END IF;
  IF locked_node_ids IS NULL OR array_position(locked_node_ids, NULL) IS NOT NULL THEN
    RAISE EXCEPTION 'prelocked node IDs must be nonnull with no null elements';
  END IF;
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'incremental membership requires READ COMMITTED';
  END IF;
  IF EXISTS (
    SELECT node_internal_id FROM pg_temp.output_membership_transaction_changes
      WHERE NOT (node_internal_id = ANY(locked_node_ids))
    UNION ALL
    SELECT node_internal_id FROM pg_temp.output_membership_outpoint_changes
      WHERE NOT (node_internal_id = ANY(locked_node_ids))
  ) THEN
    RAISE EXCEPTION 'queued membership changes include a node not locked before facts'
      USING ERRCODE = '23514';
  END IF;

  WITH ids AS MATERIALIZED (
    SELECT DISTINCT transaction_internal_id AS id
    FROM pg_temp.output_membership_transaction_changes
  ), target_union AS (
    SELECT o.transaction_hash, o.output_index FROM ids
    CROSS JOIN LATERAL (
      SELECT hash FROM public.transaction WHERE internal_id = ids.id OFFSET 0
    ) AS creator
    CROSS JOIN LATERAL (
      SELECT transaction_hash, output_index FROM public.output
      WHERE transaction_hash = creator.hash OFFSET 0
    ) AS o
    UNION
    SELECT i.outpoint_transaction_hash, i.outpoint_index FROM ids
    CROSS JOIN LATERAL (
      SELECT outpoint_transaction_hash, outpoint_index FROM public.input
      WHERE transaction_internal_id = ids.id OFFSET 0
    ) AS i
    UNION
    SELECT transaction_hash, output_index FROM pg_temp.output_membership_outpoint_changes
  ), targets AS MATERIALIZED (
    SELECT transaction_hash, output_index FROM target_union
    WHERE NOT (transaction_hash = decode(repeat('00',32), 'hex') AND output_index = 4294967295)
    ORDER BY transaction_hash, output_index
  )
  -- Count forces every lock, not merely the first row, preserving the sorted
  -- materialized outer target stream through the correlated nested loop.
  SELECT count(*) INTO updated FROM targets AS target
  CROSS JOIN LATERAL (
    SELECT 1 FROM public.output AS o
    WHERE o.transaction_hash = target.transaction_hash AND o.output_index = target.output_index
    FOR NO KEY UPDATE OFFSET 0
  ) AS locked;
  updated := 0;

  FOR group_node IN
    SELECT node_internal_id FROM pg_temp.output_membership_transaction_changes
    UNION SELECT node_internal_id FROM pg_temp.output_membership_outpoint_changes
    ORDER BY node_internal_id
  LOOP
    SELECT coalesce(array_agg(transaction_internal_id ORDER BY transaction_internal_id), ARRAY[]::bigint[])
      INTO transaction_ids FROM pg_temp.output_membership_transaction_changes WHERE node_internal_id = group_node;
    SELECT coalesce(jsonb_agg(jsonb_build_object('transaction_hash', transaction_hash, 'output_index', output_index)), '[]'::jsonb)
      INTO direct_outpoints FROM pg_temp.output_membership_outpoint_changes WHERE node_internal_id = group_node;
    updated := updated + output_membership.refresh_for_node(group_node, transaction_ids, direct_outpoints);
  END LOOP;
  DELETE FROM pg_temp.output_membership_transaction_changes;
  DELETE FROM pg_temp.output_membership_outpoint_changes;
  PERFORM set_config('output_membership.collect_changes', 'off', true);
  RETURN updated;
END
$fn$;

DO $collectors$
DECLARE
  relation_name text;
BEGIN
  FOREACH relation_name IN ARRAY ARRAY['node_transaction', 'node_block'] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trigger_output_membership_collect_insert ON public.%I', relation_name);
    EXECUTE format('CREATE TRIGGER trigger_output_membership_collect_insert AFTER INSERT ON public.%I '
      'REFERENCING NEW TABLE AS output_membership_new FOR EACH STATEMENT '
      'EXECUTE FUNCTION output_membership.collect_membership_changes()', relation_name);
    EXECUTE format('DROP TRIGGER IF EXISTS trigger_output_membership_collect_delete ON public.%I', relation_name);
    EXECUTE format('CREATE TRIGGER trigger_output_membership_collect_delete AFTER DELETE ON public.%I '
      'REFERENCING OLD TABLE AS output_membership_old FOR EACH STATEMENT '
      'EXECUTE FUNCTION output_membership.collect_membership_changes()', relation_name);
    EXECUTE format('DROP TRIGGER IF EXISTS trigger_output_membership_collect_update ON public.%I', relation_name);
    EXECUTE format('CREATE TRIGGER trigger_output_membership_collect_update AFTER UPDATE ON public.%I '
      'REFERENCING OLD TABLE AS output_membership_old NEW TABLE AS output_membership_new FOR EACH STATEMENT '
      'EXECUTE FUNCTION output_membership.collect_membership_changes()', relation_name);
  END LOOP;
END
$collectors$;
