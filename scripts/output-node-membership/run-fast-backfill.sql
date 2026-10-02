\set ON_ERROR_STOP on
\timing on

\if :{?batch_heap_blocks}
\else
  \set batch_heap_blocks 20000
\endif

\if :{?operator_confirmation}
\else
  \set operator_confirmation missing
\endif

\if :{?scratch_budget_bytes}
\else
  \set scratch_budget_bytes 0
\endif

\if :{?desired_row_ceiling}
\else
  \set desired_row_ceiling 0
\endif

SET statement_timeout = 0;
SET lock_timeout = '5s';
SET work_mem = '256MB';
SET maintenance_work_mem = '1GB';
SET jit = off;
SET wal_compression = 'lz4';
SET max_parallel_workers_per_gather = 4;

CALL output_membership_backfill.run(
  :batch_heap_blocks,
  :'operator_confirmation',
  :scratch_budget_bytes,
  :desired_row_ceiling
);

SELECT row_to_json(state) FROM output_membership_backfill.state;
SELECT json_build_object(
  'batches', count(*),
  'sourceRows', coalesce(sum(source_rows), 0),
  'rowsUpdated', coalesce(sum(rows_updated), 0),
  'duration', coalesce(sum(duration), interval '0')
)
FROM output_membership_backfill.batch;
