-- unspent_output(node_name): a query root for "unspent outputs as seen by one
-- node", tracked in Hasura so clients can write
--   unspent_output(args: {node_name: "..."}, where: {token_category: {_eq: ...}})
-- instead of the generic
--   output(where: {transaction: {<accepted by node>}, _not: {spent_by: {transaction: {<accepted by node>}}}})
-- which Hasura compiles into nested EXISTS chains via node/block joins.
--
-- Semantics are identical to that generic where clause: an output is returned
-- when (a) no input spending it belongs to a transaction the node accepted
-- (in a block the node accepted, or in the node's mempool) and (b) the
-- transaction creating it was accepted by the node (same two tests).
--
-- As a single-SELECT LANGUAGE sql STABLE function it is inlined by PostgreSQL,
-- so the caller's where/order_by/limit (and Hasura's permission row limit)
-- apply inside the query and drive the scan of `output`.
--
-- CALLERS MUST FILTER BY token_category AND/OR nonfungible_token_capability
-- (served by token_category_index). The function itself only has the
-- per-row spend/acceptance probes; a call filtered only by locking_bytecode
-- (or not filtered at all) sequentially scans `output`.
--
-- Why CASE: PostgreSQL does not guarantee evaluation order of AND-ed EXISTS
-- subplans and, given the small selectivity estimates, often evaluates the
-- creating-transaction test first. CASE fixes the order: the spender test runs
-- first (it removes ~99% of rows for long-lived token categories) and the
-- creating-transaction test runs only on the survivors.
--
-- Why block-first: almost every spender (and creating transaction) is mined,
-- so the block_transaction/node_block probe answers most rows and the
-- node_transaction (mempool) probe is only reached for the remainder.
--
-- The node id is resolved by an uncorrelated subquery (an InitPlan, evaluated
-- once per query), so each probe is an exact primary-key lookup on
-- node_block (node_internal_id, block_internal_id) and node_transaction
-- (transaction_internal_id, node_internal_id).
--
-- Measured on a mainnet golden snapshot (Postgres 18.3, warm cache): the
-- AllHolders query for a long-lived token category went from 7,086 ms to
-- 3,371 ms and the price-contract lookup from 3,430 ms to 2,176 ms, with
-- byte-identical results. This is an interim root: the remaining cost is the
-- per-row spend probe, which a stored spend marker would remove.
CREATE OR REPLACE FUNCTION unspent_output (node_name text)
  RETURNS SETOF output
  LANGUAGE sql STABLE
AS $$
  SELECT o.* FROM output o
  WHERE CASE
    -- (a) spent by a transaction this node accepted: exclude
    WHEN EXISTS (
      SELECT 1 FROM input i
      WHERE i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
        AND (EXISTS (
               SELECT 1 FROM block_transaction bt
               JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id
               WHERE bt.transaction_internal_id = i.transaction_internal_id
                 AND nb.node_internal_id = (SELECT n.internal_id FROM node n WHERE n.name = node_name))
          OR EXISTS (
               SELECT 1 FROM node_transaction nt
               WHERE nt.transaction_internal_id = i.transaction_internal_id
                 AND nt.node_internal_id = (SELECT n.internal_id FROM node n WHERE n.name = node_name))))
    THEN false
    -- (b) only for survivors: created by a transaction this node accepted
    ELSE EXISTS (
      SELECT 1 FROM transaction t
      WHERE t.hash = o.transaction_hash
        AND (EXISTS (
               SELECT 1 FROM block_transaction bt
               JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id
               WHERE bt.transaction_internal_id = t.internal_id
                 AND nb.node_internal_id = (SELECT n.internal_id FROM node n WHERE n.name = node_name))
          OR EXISTS (
               SELECT 1 FROM node_transaction nt
               WHERE nt.transaction_internal_id = t.internal_id
                 AND nt.node_internal_id = (SELECT n.internal_id FROM node n WHERE n.name = node_name))))
  END
$$;
COMMENT ON FUNCTION unspent_output (text) IS 'Return the outputs which are unspent according to the named node: created by a transaction the node accepted (in a block it accepted or in its mempool) and not spent by any transaction the node accepted. Filter by token_category and/or nonfungible_token_capability; a call filtered only by locking_bytecode scans every output.';
