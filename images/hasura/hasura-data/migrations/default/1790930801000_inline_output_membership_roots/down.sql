CREATE OR REPLACE FUNCTION public.unspent_output(node_name text)
  RETURNS SETOF public.output
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

CREATE OR REPLACE FUNCTION public.accepted_output(node_name text)
  RETURNS SETOF public.output
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

DROP FUNCTION output_membership.require_ready();
