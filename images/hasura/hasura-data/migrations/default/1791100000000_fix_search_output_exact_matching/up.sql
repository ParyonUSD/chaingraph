-- search_output previously compared each input, unchanged, with the 25-byte
-- prefix stored by output_search_index, so inputs longer than 25 bytes (e.g.
-- P2SH32) never matched and 25-byte inputs also matched longer scripts sharing
-- that prefix. Truncate each input to 25 bytes for the indexed comparison, then
-- recheck full equality. As a single-statement SQL function, PostgreSQL can
-- inline it, so Hasura's where/limit/order_by apply inside the query.
-- Note: this method expects an index: CREATE INDEX output_search_index ON output USING btree (substring(locking_bytecode, 0, 26));
CREATE OR REPLACE FUNCTION search_output (locking_bytecode_hex text[])
  RETURNS SETOF output
  LANGUAGE sql STABLE
AS $$
  SELECT * FROM output
  -- use output_search_index (first 25 bytes), then compare the full bytecode
  WHERE substring(locking_bytecode from 0 for 26) = ANY (ARRAY(
      SELECT substring(decode(hex, 'hex') from 1 for 25) FROM unnest(locking_bytecode_hex) AS hex))
    AND locking_bytecode = ANY (ARRAY(
      SELECT decode(hex, 'hex') FROM unnest(locking_bytecode_hex) AS hex))
  ORDER BY locking_bytecode ASC
$$;
COMMENT ON FUNCTION search_output (text[]) IS 'Return a list of outputs whose locking bytecode exactly matches any of the provided locking bytecode hex values (of any length, e.g. P2PKH, P2SH20 and P2SH32).';
