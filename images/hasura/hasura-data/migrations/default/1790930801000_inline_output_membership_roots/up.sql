CREATE FUNCTION output_membership.require_ready() RETURNS boolean
  LANGUAGE plpgsql
  STABLE
  PARALLEL RESTRICTED
AS $require_ready$
BEGIN
  IF NOT coalesce((
    SELECT state.ready
      FROM output_membership.state AS state
      WHERE state.id
  ), false) THEN
    RAISE EXCEPTION 'output node membership is not ready'
      USING ERRCODE = 'object_not_in_prerequisite_state';
  END IF;
  RETURN true;
END
$require_ready$;

/*
 * These table functions must remain single-statement SQL functions without a
 * SET clause. PostgreSQL can then inline them and push Hasura's outer filters
 * into the output scan, allowing the partial category and bytecode indexes to
 * serve general GraphQL queries.
 */
CREATE OR REPLACE FUNCTION public.unspent_output(node_name text)
  RETURNS SETOF public.output
  LANGUAGE sql
  STABLE
  PARALLEL RESTRICTED
AS $unspent_output$
SELECT output.*
  FROM public.output
  CROSS JOIN public.node
  WHERE output_membership.require_ready()
    AND node.name = $1
    AND cardinality(output.unspent_node_ids) > 0
    AND node.internal_id = ANY(output.unspent_node_ids);
$unspent_output$;

CREATE OR REPLACE FUNCTION public.accepted_output(node_name text)
  RETURNS SETOF public.output
  LANGUAGE sql
  STABLE
  PARALLEL RESTRICTED
AS $accepted_output$
SELECT output.*
  FROM public.output
  CROSS JOIN public.node
  WHERE output_membership.require_ready()
    AND node.name = $1
    AND cardinality(output.accepted_node_ids) > 0
    AND node.internal_id = ANY(output.accepted_node_ids);
$accepted_output$;
