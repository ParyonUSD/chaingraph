\set ON_ERROR_STOP on

SELECT row_to_json(state) FROM output_membership.state;
SELECT row_to_json(state) FROM output_membership_backfill.state;
SELECT json_build_object(
  'batches', count(*),
  'sourceRows', coalesce(sum(source_rows), 0),
  'rowsUpdated', coalesce(sum(rows_updated), 0),
  'duration', coalesce(sum(duration), interval '0'),
  'lastFinishedAt', max(finished_at)
)
FROM output_membership_backfill.batch;
