-- Restore the 0, 26 form of both functions (as defined by the two previous
-- migrations). They need output_search_index on substring(locking_bytecode, 0,
-- 26): roll the agent back and rebuild that index (drop the 1-based one so the
-- previous agent recreates it).

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

CREATE OR REPLACE FUNCTION search_output_prefix (locking_bytecode_prefix_hex text)
  RETURNS SETOF output
  LANGUAGE sql STABLE
AS $$
  SELECT * FROM output
  -- use output_search_index: every 25-byte value starting with the prefix lies
  -- between the prefix and the prefix padded with 0xff bytes to 25 bytes
  WHERE substring(locking_bytecode from 0 for 26)
      BETWEEN substring(decode(locking_bytecode_prefix_hex, 'hex') from 1 for 25)
      AND substring(decode(locking_bytecode_prefix_hex, 'hex') from 1 for 25)
        || decode(repeat('ff', 25 - least(length(decode(locking_bytecode_prefix_hex, 'hex')), 25)), 'hex')
    AND substring(locking_bytecode from 1 for length(decode(locking_bytecode_prefix_hex, 'hex')))
      = decode(locking_bytecode_prefix_hex, 'hex')
  ORDER BY locking_bytecode ASC
$$;
COMMENT ON FUNCTION search_output_prefix (text) IS 'Return a list of outputs whose locking bytecode begins with the provided prefix hex (of any length; every byte is matched literally).';
