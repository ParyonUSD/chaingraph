CREATE OR REPLACE FUNCTION output_membership.refresh(
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
