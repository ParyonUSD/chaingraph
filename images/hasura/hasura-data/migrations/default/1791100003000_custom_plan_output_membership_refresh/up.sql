-- output_membership.refresh was pinned to a generic plan, which estimates
-- `= ANY(transaction_internal_ids)` at 10 elements regardless of the array.
-- Every CTE estimate is derived from that guess, so batches of thousands of
-- transactions (e.g. a node catching up via headers) can get nested loops over
-- CTEs and run quadratically (minutes instead of seconds). Plan each call with
-- the actual arrays instead; planning is cheap relative to the refresh itself.
ALTER FUNCTION output_membership.refresh(bigint[], jsonb)
  SET plan_cache_mode = 'force_custom_plan';
