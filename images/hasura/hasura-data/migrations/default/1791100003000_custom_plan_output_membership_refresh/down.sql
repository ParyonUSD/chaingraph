ALTER FUNCTION output_membership.refresh(bigint[], jsonb)
  SET plan_cache_mode = 'force_generic_plan';
