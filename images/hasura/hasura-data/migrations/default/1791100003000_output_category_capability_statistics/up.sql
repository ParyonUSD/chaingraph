-- Extended statistics on output (token_category, nonfungible_token_capability).
--
-- The planner treats these two columns as independent, so it badly
-- underestimates selective (category, capability) combinations such as a
-- protocol's "mutable" state NFTs. Measured on a 1.16 B-output mainnet
-- database, the (Paryon category, 'mutable') estimate was 2,900 rows vs
-- 179,188 actual, which pushed wallet-path queries onto slow plans (~1 s).
--
-- The MCV list needs a large sample to capture rare-but-important
-- combinations: at the default statistics target the combination was not
-- sampled at all; at 10000 (a 3 M-row sample) the estimate is 185,752.
--
-- This migration only defines the statistics object. It deliberately does
-- NOT run ANALYZE: "ANALYZE output" takes ~4 minutes on a mainnet-sized
-- database, which is too long to hold inside the Hasura migration runner.
-- After applying, run "ANALYZE output;" manually (see README "Upgrade
-- Chaingraph"); otherwise autovacuum's auto-analyze will populate the
-- statistics eventually. Until then the object exists but has no data and
-- the planner behaves exactly as before.
CREATE STATISTICS IF NOT EXISTS output_category_capability_stats (ndistinct, dependencies, mcv)
  ON token_category, nonfungible_token_capability
  FROM output;

ALTER STATISTICS output_category_capability_stats SET STATISTICS 10000;
