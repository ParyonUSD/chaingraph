#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
test_root="$(mktemp -d "/tmp/output-membership-backfill.XXXXXX")"
data_dir="$test_root/data"
socket_dir="$test_root/socket"
port=$((55000 + ($$ % 1000)))

cleanup() {
  if [[ -d "$data_dir" ]]; then
    pg_ctl -D "$data_dir" -m immediate stop >/dev/null 2>&1 || true
  fi
  rm -rf "$test_root"
}
trap cleanup EXIT

mkdir -p "$socket_dir"
initdb -D "$data_dir" --no-locale --encoding=UTF8 --auth=trust >/dev/null
pg_ctl -D "$data_dir" -o "-F -k $socket_dir -p $port" -w start >/dev/null

psql_args=(-X -v ON_ERROR_STOP=1 -h "$socket_dir" -p "$port" -d postgres)

psql "${psql_args[@]}" <<'SQL' >/dev/null
CREATE TABLE node (internal_id integer PRIMARY KEY, name text NOT NULL);
CREATE TABLE transaction (internal_id bigint PRIMARY KEY, hash bytea UNIQUE NOT NULL);
CREATE TABLE block (internal_id bigint PRIMARY KEY);
CREATE TABLE node_block (
  node_internal_id integer NOT NULL,
  block_internal_id bigint NOT NULL,
  PRIMARY KEY (node_internal_id, block_internal_id)
);
CREATE TABLE block_transaction (
  block_internal_id bigint NOT NULL,
  transaction_internal_id bigint NOT NULL,
  PRIMARY KEY (block_internal_id, transaction_internal_id)
);
CREATE TABLE node_transaction (
  node_internal_id integer NOT NULL,
  transaction_internal_id bigint NOT NULL,
  PRIMARY KEY (node_internal_id, transaction_internal_id)
);
CREATE TABLE output (
  transaction_hash bytea NOT NULL,
  output_index bigint NOT NULL,
  locking_bytecode bytea NOT NULL,
  accepted_node_ids integer[] NOT NULL DEFAULT ARRAY[]::integer[],
  unspent_node_ids integer[] NOT NULL DEFAULT ARRAY[]::integer[],
  PRIMARY KEY (transaction_hash, output_index)
);
CREATE TABLE input (
  transaction_internal_id bigint NOT NULL,
  input_index bigint NOT NULL,
  outpoint_transaction_hash bytea NOT NULL,
  outpoint_index bigint NOT NULL,
  PRIMARY KEY (transaction_internal_id, input_index)
);

CREATE SCHEMA output_membership;
CREATE TABLE output_membership.state (
  id boolean PRIMARY KEY DEFAULT true CHECK (id),
  phase text NOT NULL,
  ready boolean NOT NULL DEFAULT false,
  default_node_internal_id integer,
  original_heap_blocks bigint NOT NULL,
  next_heap_block bigint NOT NULL DEFAULT 0,
  rows_updated bigint NOT NULL DEFAULT 0,
  started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  validated_at timestamptz,
  validation_details jsonb NOT NULL DEFAULT '{}'::jsonb
);
INSERT INTO output_membership.state (
  id, phase, default_node_internal_id, original_heap_blocks,
  next_heap_block, rows_updated
) VALUES (true, 'backfilling', 1, 1000, 9, 17);

CREATE FUNCTION output_membership.test_backfill_guc() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF current_setting('output_membership.backfill', true) <> 'on' THEN
    RAISE EXCEPTION 'backfill GUC was not enabled';
  END IF;
  RETURN NULL;
END
$$;
CREATE FUNCTION output_membership.test_writer_lock() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(20261001, 1);
  RETURN NULL;
END
$$;
CREATE TRIGGER test_writer_lock
BEFORE UPDATE ON output
FOR EACH STATEMENT EXECUTE FUNCTION output_membership.test_writer_lock();
CREATE TRIGGER test_backfill_guc
AFTER UPDATE ON output
FOR EACH STATEMENT EXECUTE FUNCTION output_membership.test_backfill_guc();

INSERT INTO node VALUES (1, 'alpha'), (2, 'beta');
INSERT INTO transaction VALUES
  (1, decode(repeat('01', 32), 'hex')),
  (2, decode(repeat('02', 32), 'hex')),
  (3, decode(repeat('03', 32), 'hex')),
  (4, decode(repeat('04', 32), 'hex')),
  (5, decode(repeat('05', 32), 'hex')),
  (6, decode(repeat('06', 32), 'hex')),
  (7, decode(repeat('07', 32), 'hex')),
  (8, decode(repeat('08', 32), 'hex'));
INSERT INTO transaction
SELECT 100000 + value,
       decode(lpad(to_hex(100000 + value), 64, '0'), 'hex')
FROM generate_series(1, 6000) AS value;
INSERT INTO transaction
SELECT 120050 + value,
       decode(lpad(to_hex(120050 + value), 64, '0'), 'hex')
FROM generate_series(1, 6000) AS value;
INSERT INTO transaction
SELECT 400000 + value,
       decode(lpad(to_hex(400000 + value), 64, '0'), 'hex')
FROM generate_series(1, 12000) AS value;
INSERT INTO block VALUES (1), (2), (3);
INSERT INTO node_block VALUES (1, 1), (1, 2), (2, 3);
INSERT INTO block_transaction VALUES (1, 1), (2, 2), (2, 5), (3, 5);
INSERT INTO node_transaction VALUES
  (2, 1),
  (2, 3),
  (2, 5),
  (1, 6),
  (2, 7);
INSERT INTO node_transaction
SELECT 1, 100000 + value FROM generate_series(1, 6000) AS value;
INSERT INTO node_transaction
SELECT 1, 120050 + value FROM generate_series(1, 6000) AS value;
INSERT INTO node_transaction
SELECT 1, 400000 + value FROM generate_series(1, 12000) AS value;
INSERT INTO input VALUES
  (6, 0, decode(repeat('01', 32), 'hex'), 0),
  (7, 0, decode(repeat('01', 32), 'hex'), 0),
  (7, 1, decode(repeat('05', 32), 'hex'), 0);
INSERT INTO input
SELECT 400000 + value, 0,
       decode(lpad(to_hex(100000 + value), 64, '0'), 'hex'), 0
FROM generate_series(1, 6000) AS value;
INSERT INTO input
SELECT 406000 + value, 0,
       decode(lpad(to_hex(120050 + value), 64, '0'), 'hex'), 0
FROM generate_series(1, 6000) AS value;

/* Wrong default state, OP_RETURN state, and one already-correct output. */
INSERT INTO output VALUES
  (decode(repeat('01', 32), 'hex'), 0, decode('51', 'hex'), ARRAY[1], ARRAY[]::integer[]);
INSERT INTO output
SELECT decode(lpad(to_hex(100000 + value), 64, '0'), 'hex'), 0,
       decode('51', 'hex'), ARRAY[1], ARRAY[]::integer[]
FROM generate_series(1, 6000) AS value;
INSERT INTO output VALUES
  (decode(repeat('02', 32), 'hex'), 0, decode('6a01', 'hex'), ARRAY[1], ARRAY[1]);
INSERT INTO output
SELECT decode(lpad(to_hex(120050 + value), 64, '0'), 'hex'), 0,
       decode('51', 'hex'), ARRAY[1], ARRAY[]::integer[]
FROM generate_series(1, 6000) AS value;
INSERT INTO output VALUES
  (decode(repeat('03', 32), 'hex'), 0, decode('51', 'hex'), ARRAY[2], ARRAY[2]),
  (decode(repeat('04', 32), 'hex'), 0, decode('51', 'hex'), ARRAY[1], ARRAY[]::integer[]),
  (decode(repeat('05', 32), 'hex'), 0, decode('51', 'hex'), ARRAY[1], ARRAY[]::integer[]);
SQL

psql "${psql_args[@]}" -f "$script_dir/install-fast-backfill.sql" >/dev/null
psql "${psql_args[@]}" -f "$script_dir/install-fast-backfill.sql" \
  >/dev/null 2>&1

if psql "${psql_args[@]}" -c \
  "CALL output_membership_backfill.run(10, 'missing', 107374182400, 10, false)" \
  >/dev/null 2>&1; then
  echo "expected offline confirmation failure" >&2
  exit 1
fi

if psql "${psql_args[@]}" -c \
  "CALL output_membership_backfill.run(10, 'WRITERS_PAUSED', 1073741824, 10, false)" \
  >/dev/null 2>&1; then
  echo "expected scratch-space gate failure" >&2
  exit 1
fi

if psql "${psql_args[@]}" >/dev/null 2>&1 <<'SQL'
SET output_membership_backfill.test_fail_after_acceptance = on;
CALL output_membership_backfill.run(
  10, 'WRITERS_PAUSED', 107374182400, 10, false
);
SQL
then
  echo "expected injected acceptance-build failure" >&2
  exit 1
fi

psql "${psql_args[@]}" <<'SQL' >/dev/null
DO $$
BEGIN
  IF (SELECT phase FROM output_membership_backfill.state WHERE id) <> 'acceptance-ready' THEN
    RAISE EXCEPTION 'acceptance stage was not durable across the injected failure';
  END IF;
  IF to_regclass('output_membership_backfill.accepted_transaction') IS NULL
    OR to_regclass('output_membership_backfill.desired_output') IS NOT NULL THEN
    RAISE EXCEPTION 'acceptance-stage recovery relations are incorrect';
  END IF;
END
$$;
SQL

psql "${psql_args[@]}" <<'SQL' >/dev/null
CREATE TABLE output_membership_backfill.desired_output (
  target_ctid tid PRIMARY KEY,
  transaction_hash bytea NOT NULL,
  output_index bigint NOT NULL,
  accepted_node_ids integer[] NOT NULL,
  unspent_node_ids integer[] NOT NULL
);
UPDATE output_membership_backfill.state
SET phase='target-building', next_target_heap_block=2000,
    next_output_heap_block=0, source_rows=0, rows_updated=0,
    target_stage=NULL, target_scratch_budget_bytes=NULL
WHERE id;
SQL

if psql "${psql_args[@]}" -c \
  "CALL output_membership_backfill.run(10, 'WRITERS_PAUSED', 68719476736, 10, false)" \
  >/dev/null 2>&1; then
  echo "expected replacement scratch-space gate failure" >&2
  exit 1
fi

psql "${psql_args[@]}" <<'SQL' >/dev/null
DO $$
BEGIN
  IF (SELECT phase FROM output_membership_backfill.state WHERE id) <> 'acceptance-ready'
    OR (SELECT target_scratch_budget_bytes FROM output_membership_backfill.state WHERE id) IS NOT NULL THEN
    RAISE EXCEPTION 'replacement scratch failure changed durable state';
  END IF;
  IF (SELECT next_heap_block FROM output_membership.state WHERE id) <> 9
    OR (SELECT rows_updated FROM output_membership.state WHERE id) <> 17 THEN
    RAISE EXCEPTION 'legacy empty-target transition changed the canonical cursor or count';
  END IF;
  IF to_regclass('output_membership_backfill.acceptance_exception') IS NOT NULL
    OR to_regclass('output_membership_backfill.excluded_default_output') IS NOT NULL
    OR to_regclass('output_membership_backfill.desired_nondefault') IS NOT NULL
    OR to_regclass('output_membership_backfill.desired_output') IS NOT NULL THEN
    RAISE EXCEPTION 'replacement scratch failure retained partial relations';
  END IF;
END
$$;
SQL

if psql "${psql_args[@]}" >/dev/null 2>&1 <<'SQL'
SET output_membership_backfill.test_fail_during_node = on;
CALL output_membership_backfill.run(
  10, 'WRITERS_PAUSED', 107374182400, 10, false
);
SQL
then
  echo "expected injected node-build failure" >&2
  exit 1
fi

psql "${psql_args[@]}" <<'SQL' >/dev/null
DO $$
BEGIN
  IF (SELECT target_stage FROM output_membership_backfill.state WHERE id) <> 'nodes-building' THEN
    RAISE EXCEPTION 'node-build stage was not durable across the injected failure';
  END IF;
  IF (SELECT count(*) FROM output_membership_backfill.target_node WHERE status = 'complete') <> 1
    OR (SELECT count(*) FROM output_membership_backfill.target_node WHERE status = 'pending') <> 1 THEN
    RAISE EXCEPTION 'node-build resume markers are incorrect';
  END IF;
  IF (SELECT next_target_heap_block FROM output_membership_backfill.state WHERE id) <> 0 THEN
    RAISE EXCEPTION 'final target cursor advanced during node construction';
  END IF;
  IF EXISTS (
    SELECT 1 FROM output_membership_backfill.state
    WHERE id AND required_scratch_bytes > target_scratch_budget_bytes
  ) THEN
    RAISE EXCEPTION 'replacement scratch bound exceeds the current-free-space budget';
  END IF;
  IF (SELECT required_scratch_bytes FROM output_membership_backfill.state WHERE id) < 68719476736 THEN
    RAISE EXCEPTION 'replacement scratch bound omitted the fixed WAL reserve';
  END IF;
  IF to_regclass('output_membership_backfill.acceptance_exception') IS NULL
    OR to_regclass('output_membership_backfill.excluded_default_output') IS NULL
    OR to_regclass('output_membership_backfill.desired_nondefault') IS NULL
    OR to_regclass('output_membership_backfill.node_utxo_stage') IS NULL
    OR to_regclass('output_membership_backfill.desired_output') IS NULL THEN
    RAISE EXCEPTION 'node-build recovery relations are incorrect';
  END IF;
END
$$;
SQL

target_plan="$(psql "${psql_args[@]}" -At <<'SQL'
SET work_mem = '4GB';
SET jit = off;
SET max_parallel_workers_per_gather = 0;
SET join_collapse_limit = 1;
SET from_collapse_limit = 1;
SET enable_nestloop = off;
SET enable_hashjoin = off;
SET enable_mergejoin = on;
SET enable_seqscan = off;
SET enable_sort = off;
SET enable_hashagg = off;
EXPLAIN (COSTS off)
WITH spender_acceptance AS NOT MATERIALIZED (
  SELECT accepted.transaction_internal_id
  FROM output_membership_backfill.accepted_transaction accepted
  LEFT JOIN (
    SELECT transaction_internal_id
    FROM output_membership_backfill.acceptance_exception
    WHERE NOT 1 = ANY(accepted_node_ids)
  ) excluded USING (transaction_internal_id)
  WHERE excluded.transaction_internal_id IS NULL
),
created_output AS NOT MATERIALIZED (
  SELECT output.transaction_hash, output.output_index
  FROM output
  INNER JOIN output_membership_backfill.accepted_transaction accepted
    USING (transaction_hash)
  LEFT JOIN output_membership_backfill.excluded_default_output excluded
    USING (transaction_hash, output_index)
  WHERE excluded.transaction_hash IS NULL
),
spent_output AS NOT MATERIALIZED (
  SELECT input.outpoint_transaction_hash AS transaction_hash,
         input.outpoint_index AS output_index
  FROM input INNER JOIN spender_acceptance USING (transaction_internal_id)
  GROUP BY input.outpoint_transaction_hash, input.outpoint_index
  HAVING count(*) > 0
)
SELECT created_output.transaction_hash, created_output.output_index
FROM created_output
LEFT JOIN spent_output USING (transaction_hash, output_index)
WHERE spent_output.transaction_hash IS NULL;
SQL
)"
for expected_plan_node in "Merge Anti Join" "GroupAggregate" "input_pkey" "output_pkey"; do
  if [[ "$target_plan" != *"$expected_plan_node"* ]]; then
    echo "target plan is missing $expected_plan_node" >&2
    echo "$target_plan" >&2
    exit 1
  fi
done
spent_plan="${target_plan#*GroupAggregate}"
if [[ "$spent_plan" == *"Materialize"* || "$target_plan" == *"LATERAL"* ]]; then
  echo "target plan retained a materialized spent stream or lateral output probes" >&2
  echo "$target_plan" >&2
  exit 1
fi

if psql "${psql_args[@]}" >/dev/null 2>&1 <<'SQL'
SET output_membership_backfill.test_fail_after_source = on;
CALL output_membership_backfill.run(
  10, 'WRITERS_PAUSED', 107374182400, 10, false
);
SQL
then
  echo "expected injected sparse-source failure" >&2
  exit 1
fi

psql "${psql_args[@]}" <<'SQL' >/dev/null
DO $$
BEGIN
  IF (SELECT target_stage FROM output_membership_backfill.state WHERE id) <> 'source-ready' THEN
    RAISE EXCEPTION 'sparse source was not durable across the injected failure';
  END IF;
  IF (SELECT count(*) FROM output_membership_backfill.target_node WHERE status <> 'complete') <> 0 THEN
    RAISE EXCEPTION 'node stages were not complete before sparse source commit';
  END IF;
  IF (SELECT count(*) FROM output_membership_backfill.desired_output) <> 0 THEN
    RAISE EXCEPTION 'final target began before the sparse source committed';
  END IF;
END
$$;
SQL

final_plan="$(psql "${psql_args[@]}" -At <<'SQL'
SET work_mem = '4GB';
SET hash_mem_multiplier = 2;
SET max_parallel_workers_per_gather = 0;
SET enable_nestloop = off;
SET enable_mergejoin = off;
SET enable_hashjoin = on;
SET enable_seqscan = on;
EXPLAIN (COSTS off)
SELECT output.ctid,
       coalesce(desired.accepted_node_ids, ARRAY[1]::integer[])
FROM output
LEFT JOIN output_membership_backfill.desired_nondefault desired
  USING (transaction_hash, output_index);
SQL
)"
if [[ "$final_plan" != *"Hash Left Join"* \
  || "$final_plan" != *"Seq Scan on output"* \
  || "$final_plan" == *"Materialize"* ]]; then
  echo "final target plan is not one sequential output scan with a source hash" >&2
  echo "$final_plan" >&2
  exit 1
fi

psql "${psql_args[@]}" -c \
  "UPDATE output_membership_backfill.state SET desired_row_ceiling=3 WHERE id" \
  >/dev/null
if psql "${psql_args[@]}" -c \
  "CALL output_membership_backfill.run(10, 'WRITERS_PAUSED', 107374182400, 3, false)" \
  >/dev/null 2>&1; then
  echo "expected final target ceiling failure" >&2
  exit 1
fi
psql "${psql_args[@]}" <<'SQL' >/dev/null
DO $$
BEGIN
  IF (SELECT target_stage FROM output_membership_backfill.state WHERE id) <> 'source-ready'
    OR (SELECT count(*) FROM output_membership_backfill.desired_output) <> 0 THEN
    RAISE EXCEPTION 'target ceiling failure did not roll back atomically';
  END IF;
  UPDATE output_membership_backfill.state SET desired_row_ceiling=10 WHERE id;
END
$$;
SQL

if psql "${psql_args[@]}" >/dev/null 2>&1 <<'SQL'
SET output_membership_backfill.test_fail_during_target = on;
CALL output_membership_backfill.run(
  10, 'WRITERS_PAUSED', 107374182400, 10, false
);
SQL
then
  echo "expected injected target-build failure" >&2
  exit 1
fi

psql "${psql_args[@]}" <<'SQL' >/dev/null
DO $$
BEGIN
  IF (SELECT target_stage FROM output_membership_backfill.state WHERE id) <> 'target-built' THEN
    RAISE EXCEPTION 'target build was not durable across the injected failure';
  END IF;
  IF EXISTS (
    SELECT 1 FROM output_membership_backfill.state
    WHERE id AND next_target_heap_block <> output_heap_blocks
  ) THEN
    RAISE EXCEPTION 'completed target cursor was not persisted';
  END IF;
  IF (SELECT count(*) FROM output_membership_backfill.desired_output) <> 4 THEN
    RAISE EXCEPTION 'target did not remain semantically sparse';
  END IF;
END
$$;
SQL

if psql "${psql_args[@]}" -c \
  "CALL output_membership_backfill.run(10, 'WRITERS_PAUSED', 107374182400, 10, true)" \
  >/dev/null 2>&1; then
  echo "expected injected backfill failure" >&2
  exit 1
fi

psql "${psql_args[@]}" <<'SQL' >/dev/null
DO $$
BEGIN
  IF (SELECT phase FROM output_membership_backfill.state WHERE id) <> 'target-ready' THEN
    RAISE EXCEPTION 'materialized phase was not durable across the injected failure';
  END IF;
  IF (SELECT next_output_heap_block FROM output_membership_backfill.state WHERE id) <> 0 THEN
    RAISE EXCEPTION 'cursor advanced despite the rolled-back update';
  END IF;
  IF (SELECT accepted_node_ids FROM output WHERE transaction_hash = decode(repeat('01', 32), 'hex')) <> ARRAY[1] THEN
    RAISE EXCEPTION 'output update did not roll back';
  END IF;
END
$$;
SQL

psql "${psql_args[@]}" \
  --set=batch_heap_blocks=10 \
  --set=operator_confirmation=WRITERS_PAUSED \
  --set=scratch_budget_bytes=107374182400 \
  --set=desired_row_ceiling=10 \
  --file "$script_dir/run-fast-backfill.sql" >/dev/null

psql "${psql_args[@]}" <<'SQL' >/dev/null
DO $$
DECLARE
  actual jsonb;
  mismatch_count bigint;
  expected jsonb := '[
    {"hash":"01","accepted":[1,2],"unspent":[]},
    {"hash":"02","accepted":[1],"unspent":[]},
    {"hash":"03","accepted":[2],"unspent":[2]},
    {"hash":"04","accepted":[],"unspent":[]},
    {"hash":"05","accepted":[1,2],"unspent":[1]}
  ]'::jsonb;
BEGIN
  SELECT jsonb_agg(jsonb_build_object(
    'hash', left(encode(transaction_hash, 'hex'), 2),
    'accepted', accepted_node_ids,
    'unspent', unspent_node_ids
  ) ORDER BY transaction_hash)
  INTO actual
  FROM output
  WHERE transaction_hash IN (
    decode(repeat('01', 32), 'hex'),
    decode(repeat('02', 32), 'hex'),
    decode(repeat('03', 32), 'hex'),
    decode(repeat('04', 32), 'hex'),
    decode(repeat('05', 32), 'hex')
  );

  IF actual <> expected THEN
    RAISE EXCEPTION 'membership mismatch: %', actual;
  END IF;
  WITH accepted AS (
    SELECT node_internal_id, transaction_internal_id FROM node_transaction
    UNION ALL
    SELECT node_block.node_internal_id, block_transaction.transaction_internal_id
    FROM block_transaction INNER JOIN node_block USING (block_internal_id)
  ),
  accepted_transaction AS (
    SELECT transaction.hash AS transaction_hash,
           array_agg(DISTINCT accepted.node_internal_id
                     ORDER BY accepted.node_internal_id)::integer[] AS accepted_node_ids
    FROM accepted INNER JOIN transaction
      ON transaction.internal_id = accepted.transaction_internal_id
    GROUP BY transaction.hash
  ),
  spent_output AS (
    SELECT input.outpoint_transaction_hash AS transaction_hash,
           input.outpoint_index AS output_index,
           array_agg(DISTINCT node_id ORDER BY node_id)::integer[] AS spent_node_ids
    FROM input
    INNER JOIN transaction ON transaction.internal_id = input.transaction_internal_id
    INNER JOIN accepted_transaction
      ON accepted_transaction.transaction_hash = transaction.hash
    CROSS JOIN LATERAL unnest(accepted_node_ids) AS node_id
    GROUP BY input.outpoint_transaction_hash, input.outpoint_index
  ),
  normalized AS (
    SELECT output.transaction_hash, output.output_index,
           coalesce(accepted_transaction.accepted_node_ids, ARRAY[]::integer[])
             AS accepted_node_ids,
           CASE WHEN get_byte(output.locking_bytecode, 0) = 106
             THEN ARRAY[]::integer[]
             ELSE ARRAY(
               SELECT node_id
               FROM unnest(coalesce(
                 accepted_transaction.accepted_node_ids,
                 ARRAY[]::integer[]
               )) AS node_id
               WHERE NOT node_id = ANY(coalesce(
                 spent_output.spent_node_ids,
                 ARRAY[]::integer[]
               ))
               ORDER BY node_id
             )::integer[]
           END AS unspent_node_ids
    FROM output
    LEFT JOIN accepted_transaction USING (transaction_hash)
    LEFT JOIN spent_output USING (transaction_hash, output_index)
  )
  SELECT count(*) INTO mismatch_count
  FROM output INNER JOIN normalized USING (transaction_hash, output_index)
  WHERE ROW(output.accepted_node_ids, output.unspent_node_ids)
    IS DISTINCT FROM ROW(normalized.accepted_node_ids, normalized.unspent_node_ids);
  IF mismatch_count <> 0 THEN
    RAISE EXCEPTION '% outputs differ from normalized truth', mismatch_count;
  END IF;
  IF (SELECT phase FROM output_membership_backfill.state WHERE id) <> 'backfilled' THEN
    RAISE EXCEPTION 'fast backfill did not complete';
  END IF;
  IF (SELECT ready FROM output_membership.state WHERE id) THEN
    RAISE EXCEPTION 'backfill exposed readiness before finalization';
  END IF;
  IF (SELECT phase FROM output_membership.state WHERE id) <> 'backfilled' THEN
    RAISE EXCEPTION 'canonical phase is not compatible with finalizer';
  END IF;
  IF EXISTS (
    SELECT 1 FROM output_membership_backfill.batch
    WHERE source_rows <> rows_updated
  ) THEN
    RAISE EXCEPTION 'a batch source and update count differ';
  END IF;
  IF (
    SELECT coalesce(sum(rows_updated), 0)
    FROM output_membership_backfill.batch
  ) <> (SELECT source_rows FROM output_membership_backfill.state WHERE id) THEN
    RAISE EXCEPTION 'batch totals differ from the materialized source';
  END IF;
  IF (SELECT source_rows FROM output_membership_backfill.state WHERE id) <> 4 THEN
    RAISE EXCEPTION 'the already-correct partial row was not excluded';
  END IF;
  IF (SELECT rows_updated FROM output_membership.state WHERE id) <> 21 THEN
    RAISE EXCEPTION 'canonical historical update count was not preserved';
  END IF;
END
$$;
SQL

psql "${psql_args[@]}" -f "$script_dir/status-fast-backfill.sql" >/dev/null
psql "${psql_args[@]}" -c \
  "DELETE FROM output WHERE transaction_hash = decode(repeat('01', 32), 'hex')" \
  >/dev/null
if psql "${psql_args[@]}" -c \
  "CALL output_membership_backfill.run(10, 'WRITERS_PAUSED', 107374182400, 10, false)" \
  >/dev/null 2>&1; then
  echo "expected missing target row failure" >&2
  exit 1
fi

psql "${psql_args[@]}" -f "$script_dir/cleanup-fast-backfill.sql" \
  >/dev/null 2>&1
psql "${psql_args[@]}" <<'SQL' >/dev/null
DO $$
BEGIN
  IF to_regclass('output_membership_backfill.desired_output') IS NOT NULL
    OR to_regclass('output_membership_backfill.desired_nondefault') IS NOT NULL
    OR to_regclass('output_membership_backfill.node_utxo_stage') IS NOT NULL
    OR to_regclass('output_membership_backfill.excluded_default_output') IS NOT NULL
    OR to_regclass('output_membership_backfill.acceptance_exception') IS NOT NULL
    OR to_regclass('output_membership_backfill.accepted_transaction') IS NOT NULL THEN
    RAISE EXCEPTION 'large source tables remain after cleanup';
  END IF;
  IF to_regclass('output_membership_backfill.state') IS NULL
    OR to_regclass('output_membership_backfill.batch') IS NULL THEN
    RAISE EXCEPTION 'cleanup removed durable backfill history';
  END IF;
END
$$;
SQL

echo "fast backfill fixture passed"
