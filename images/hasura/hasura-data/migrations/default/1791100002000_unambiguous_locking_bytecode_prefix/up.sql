-- The locking bytecode prefix indexes were defined on
-- substring(locking_bytecode, 0, 26), which returns the first 25 bytes because
-- PostgreSQL string positions start at 1 (positions 0–25; position 0 is empty),
-- but reads as 26 bytes. The agent now defines output_search_index and
-- unspent_output_search_index on the equivalent, unambiguous
-- substring(locking_bytecode from 1 for 25). PostgreSQL only uses an
-- expression index for the identical expression; see README "Upgrade
-- Chaingraph" for the maintainer steps on existing deployments.

-- Note: this method expects an index: CREATE INDEX output_search_index ON output USING btree (substring(locking_bytecode from 1 for 25));
CREATE OR REPLACE FUNCTION search_output (locking_bytecode_hex text[])
  RETURNS SETOF output
  LANGUAGE sql STABLE
AS $$
  SELECT * FROM output
  -- use output_search_index (first 25 bytes), then compare
  -- the full locking bytecode
  WHERE substring(locking_bytecode from 1 for 25) = ANY (ARRAY(
      SELECT substring(decode(hex, 'hex') from 1 for 25) FROM unnest(locking_bytecode_hex) AS hex))
    AND locking_bytecode = ANY (ARRAY(
      SELECT decode(hex, 'hex') FROM unnest(locking_bytecode_hex) AS hex))
  ORDER BY locking_bytecode ASC
$$;
COMMENT ON FUNCTION search_output (text[]) IS 'Return a list of outputs whose locking bytecode exactly matches any of the provided locking bytecode hex values (of any length, e.g. P2PKH, P2SH20 and P2SH32).';

-- Note: this method expects an index: CREATE INDEX output_search_index ON output USING btree (substring(locking_bytecode from 1 for 25));
CREATE OR REPLACE FUNCTION search_output_prefix (locking_bytecode_prefix_hex text)
  RETURNS SETOF output
  LANGUAGE sql STABLE
AS $$
  SELECT * FROM output
  -- use output_search_index: every 25-byte value starting with
  -- the prefix lies between the prefix and the prefix padded with 0xff bytes to
  -- 25 bytes; then compare the exact prefix against the full locking bytecode
  WHERE substring(locking_bytecode from 1 for 25)
      BETWEEN substring(decode(locking_bytecode_prefix_hex, 'hex') from 1 for 25)
      AND substring(decode(locking_bytecode_prefix_hex, 'hex') from 1 for 25)
        || decode(repeat('ff', 25 - least(length(decode(locking_bytecode_prefix_hex, 'hex')), 25)), 'hex')
    AND substring(locking_bytecode from 1 for length(decode(locking_bytecode_prefix_hex, 'hex')))
      = decode(locking_bytecode_prefix_hex, 'hex')
  ORDER BY locking_bytecode ASC
$$;
COMMENT ON FUNCTION search_output_prefix (text) IS 'Return a list of outputs whose locking bytecode begins with the provided prefix hex (of any length; every byte is matched literally).';
