\set ON_ERROR_STOP on
\timing on

SET statement_timeout = 0;
SET lock_timeout = '5s';
SET maintenance_work_mem = '512MB';
SET work_mem = '256MB';
SET jit = off;

/* Writer triggers use the same key, so normalized state remains fixed. */
SELECT pg_advisory_lock(20261001, 1);

DO $guard$
BEGIN
  IF (SELECT phase FROM output_membership.state WHERE id)
    NOT IN ('backfilled', 'indexing', 'indexed') THEN
    RAISE EXCEPTION 'a completed output membership backfill is required';
  END IF;

  IF EXISTS (
    SELECT 1
      FROM pg_index
      WHERE indexrelid IN (
        to_regclass('public.output_accepted_node_ids_gin'),
        to_regclass('public.output_unspent_node_ids_gin'),
        to_regclass('public.output_unspent_fungible_category'),
        to_regclass('public.output_unspent_locking_prefix')
      )
      AND (NOT indisvalid OR NOT indisready)
  ) THEN
    RAISE EXCEPTION 'remove invalid output membership indexes before retrying';
  END IF;
END
$guard$;

UPDATE output_membership.state
  SET phase = 'indexing', ready = false, updated_at = clock_timestamp()
  WHERE id;

CREATE INDEX IF NOT EXISTS output_accepted_node_ids_gin
  ON public.output USING gin (accepted_node_ids);
CREATE INDEX IF NOT EXISTS output_unspent_node_ids_gin
  ON public.output USING gin (unspent_node_ids);
CREATE INDEX IF NOT EXISTS output_unspent_fungible_category
  ON public.output USING btree (token_category)
  WHERE cardinality(unspent_node_ids) > 0
    AND token_category IS NOT NULL
    AND nonfungible_token_capability IS NULL;
CREATE INDEX IF NOT EXISTS output_unspent_locking_prefix
  ON public.output USING btree (substring(locking_bytecode, 0, 26))
  WHERE cardinality(unspent_node_ids) > 0;

ANALYZE public.input;
ANALYZE public.output;
ALTER TABLE public.output
  VALIDATE CONSTRAINT output_node_membership_valid;

UPDATE output_membership.state
  SET phase = 'indexed', updated_at = clock_timestamp()
  WHERE id;

BEGIN ISOLATION LEVEL REPEATABLE READ;
SET LOCAL statement_timeout = 0;
SET LOCAL work_mem = '256MB';
SET LOCAL jit = off;
SET LOCAL enable_nestloop = off;
SET LOCAL max_parallel_workers_per_gather = 0;

CREATE TEMP TABLE source_acceptance ON COMMIT DROP AS
  SELECT
    node_block.node_internal_id AS node_id,
    block_transaction.transaction_internal_id
    FROM public.node_block
    INNER JOIN public.block_transaction USING (block_internal_id)
  UNION
  SELECT
    node_transaction.node_internal_id,
    node_transaction.transaction_internal_id
    FROM public.node_transaction;
ALTER TABLE source_acceptance
  ADD PRIMARY KEY (node_id, transaction_internal_id);
ANALYZE source_acceptance;

CREATE TEMP TABLE source_utxos ON COMMIT DROP AS
  WITH spent_outpoints AS MATERIALIZED (
    SELECT
      source_acceptance.node_id,
      input.outpoint_transaction_hash,
      input.outpoint_index
      FROM public.input
      INNER JOIN source_acceptance
        ON source_acceptance.transaction_internal_id = input.transaction_internal_id
  )
  SELECT
    source_acceptance.node_id,
    output.transaction_hash,
    output.output_index
    FROM public.output
    INNER JOIN public.transaction
      ON transaction.hash = output.transaction_hash
    INNER JOIN source_acceptance
      ON source_acceptance.transaction_internal_id = transaction.internal_id
    WHERE NOT (
      octet_length(output.locking_bytecode) > 0
      AND get_byte(output.locking_bytecode, 0) = 106
    )
    AND NOT EXISTS (
      SELECT 1
        FROM spent_outpoints
        WHERE spent_outpoints.node_id = source_acceptance.node_id
          AND spent_outpoints.outpoint_transaction_hash = output.transaction_hash
          AND spent_outpoints.outpoint_index = output.output_index
    );
ALTER TABLE source_utxos
  ADD PRIMARY KEY (node_id, transaction_hash, output_index);
ANALYZE source_utxos;

CREATE TEMP TABLE membership_validation (
  expected_accepted bigint NOT NULL DEFAULT 0,
  actual_accepted bigint NOT NULL DEFAULT 0,
  missing_accepted bigint NOT NULL DEFAULT 0,
  extra_accepted bigint NOT NULL DEFAULT 0,
  expected_unspent bigint NOT NULL DEFAULT 0,
  actual_unspent bigint NOT NULL DEFAULT 0,
  missing_unspent bigint NOT NULL DEFAULT 0,
  extra_unspent bigint NOT NULL DEFAULT 0,
  malformed_arrays bigint NOT NULL DEFAULT 0,
  op_return_unspent bigint NOT NULL DEFAULT 0
) ON COMMIT DROP;
INSERT INTO membership_validation DEFAULT VALUES;

WITH expected AS (
  SELECT
    source_acceptance.node_id,
    output.transaction_hash,
    output.output_index
    FROM source_acceptance
    INNER JOIN public.transaction
      ON transaction.internal_id = source_acceptance.transaction_internal_id
    INNER JOIN public.output
      ON output.transaction_hash = transaction.hash
),
actual AS (
  SELECT
    node_id,
    output.transaction_hash,
    output.output_index
    FROM public.output
    CROSS JOIN LATERAL unnest(output.accepted_node_ids) AS node_id
),
comparison AS (
  SELECT
    count(expected.node_id) AS expected_count,
    count(actual.node_id) AS actual_count,
    count(*) FILTER (WHERE actual.node_id IS NULL) AS missing_count,
    count(*) FILTER (WHERE expected.node_id IS NULL) AS extra_count
    FROM expected
    FULL JOIN actual USING (node_id, transaction_hash, output_index)
)
UPDATE membership_validation
  SET
    expected_accepted = comparison.expected_count,
    actual_accepted = comparison.actual_count,
    missing_accepted = comparison.missing_count,
    extra_accepted = comparison.extra_count
  FROM comparison;

WITH actual AS (
  SELECT
    node_id,
    output.transaction_hash,
    output.output_index
    FROM public.output
    CROSS JOIN LATERAL unnest(output.unspent_node_ids) AS node_id
),
comparison AS (
  SELECT
    count(source_utxos.node_id) AS expected_count,
    count(actual.node_id) AS actual_count,
    count(*) FILTER (WHERE actual.node_id IS NULL) AS missing_count,
    count(*) FILTER (WHERE source_utxos.node_id IS NULL) AS extra_count
    FROM source_utxos
    FULL JOIN actual USING (node_id, transaction_hash, output_index)
)
UPDATE membership_validation
  SET
    expected_unspent = comparison.expected_count,
    actual_unspent = comparison.actual_count,
    missing_unspent = comparison.missing_count,
    extra_unspent = comparison.extra_count
  FROM comparison;

UPDATE membership_validation
  SET
    malformed_arrays = (
      SELECT count(*)
        FROM public.output
        WHERE accepted_node_ids IS NULL
          OR unspent_node_ids IS NULL
          OR accepted_node_ids IS DISTINCT FROM ARRAY(
            SELECT DISTINCT node_id
              FROM unnest(accepted_node_ids) AS node_id
              ORDER BY node_id
          )
          OR unspent_node_ids IS DISTINCT FROM ARRAY(
            SELECT DISTINCT node_id
              FROM unnest(unspent_node_ids) AS node_id
              ORDER BY node_id
          )
          OR NOT unspent_node_ids <@ accepted_node_ids
    ),
    op_return_unspent = (
      SELECT count(*)
        FROM public.output
        WHERE octet_length(locking_bytecode) > 0
          AND get_byte(locking_bytecode, 0) = 106
          AND cardinality(unspent_node_ids) > 0
    );

DO $validate$
DECLARE
  result membership_validation%ROWTYPE;
BEGIN
  SELECT * INTO STRICT result FROM membership_validation;
  IF result.missing_accepted <> 0
    OR result.extra_accepted <> 0
    OR result.missing_unspent <> 0
    OR result.extra_unspent <> 0
    OR result.malformed_arrays <> 0
    OR result.op_return_unspent <> 0 THEN
    RAISE EXCEPTION 'output membership validation failed: %', row_to_json(result);
  END IF;

  IF (
    SELECT count(*)
      FROM unnest(ARRAY[
        'output_accepted_node_ids_gin',
        'output_unspent_node_ids_gin',
        'output_unspent_fungible_category',
        'output_unspent_locking_prefix'
      ]) AS expected(index_name)
      INNER JOIN pg_class ON pg_class.relname = expected.index_name
      INNER JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
      INNER JOIN pg_index ON pg_index.indexrelid = pg_class.oid
      WHERE pg_namespace.nspname = 'public'
        AND pg_index.indisvalid
        AND pg_index.indisready
  ) <> 4 THEN
    RAISE EXCEPTION 'all four valid output membership indexes are required';
  END IF;

  UPDATE output_membership.state
    SET
      phase = 'ready',
      ready = true,
      validated_at = clock_timestamp(),
      updated_at = clock_timestamp(),
      validation_details = to_jsonb(result)
    WHERE id;
END
$validate$;

SELECT row_to_json(membership_validation) FROM membership_validation;
COMMIT;

SELECT row_to_json(state) FROM output_membership.state;
SELECT pg_advisory_unlock(20261001, 1);
