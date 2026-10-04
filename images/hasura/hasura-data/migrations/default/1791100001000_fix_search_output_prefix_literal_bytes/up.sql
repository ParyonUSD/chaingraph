-- search_output_prefix previously matched with LIKE, so bytes 0x5c (escape),
-- 0x25 (%) and 0x5f (_) in the decoded prefix were treated as pattern syntax,
-- and prefixes longer than 25 bytes never matched. Select a byte range on the
-- 25-byte expression indexed by output_search_index, then recheck the exact
-- prefix against the full locking bytecode.
-- Note: this method expects an index: CREATE INDEX output_search_index ON output USING btree (substring(locking_bytecode, 0, 26));
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
