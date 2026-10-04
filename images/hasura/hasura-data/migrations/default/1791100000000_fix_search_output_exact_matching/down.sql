-- Restore the original search_output definition from 1616195337538_init.
CREATE OR REPLACE FUNCTION search_output (locking_bytecode_hex text[])
  RETURNS SETOF output
  LANGUAGE plpgsql STABLE
AS $$
DECLARE
  decoded bytea[];
BEGIN
  SELECT array_agg(decode(hex, 'hex')) into decoded FROM unnest(locking_bytecode_hex) as hex;
  RETURN QUERY (SELECT * FROM output
  -- use output_locking_bytecode_prefix_index
  WHERE substring(locking_bytecode from 0 for 26)
  = ANY(decoded)
  ORDER BY locking_bytecode ASC);
END;
$$;
COMMENT ON FUNCTION search_output (text[]) IS 'Return a list of outputs which match the provided locking bytecode hex (up to 25 bytes, supporting both P2PKH and P2SH outputs).';
