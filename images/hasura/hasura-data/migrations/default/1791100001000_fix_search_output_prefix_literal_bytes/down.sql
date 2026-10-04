-- Restore the original search_output_prefix definition from 1616195337538_init.
CREATE OR REPLACE FUNCTION search_output_prefix (locking_bytecode_prefix_hex text)
  RETURNS SETOF output
  LANGUAGE sql STABLE
AS $$
  SELECT * FROM output
  -- use output_locking_bytecode_prefix_index
  WHERE substring(locking_bytecode from 0 for 26)
  LIKE decode(locking_bytecode_prefix_hex, 'hex') || '%'
  ORDER BY locking_bytecode ASC
$$;
COMMENT ON FUNCTION search_output_prefix (text) IS 'Return a list of outputs in which the first 25 bytes of the locking bytecode match the provided prefix hex.';
