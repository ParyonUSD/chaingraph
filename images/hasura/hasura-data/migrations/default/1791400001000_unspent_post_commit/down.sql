DROP FUNCTION IF EXISTS unspent_tracking_post_commit_release_blocks(text, bytea[]);
DROP FUNCTION IF EXISTS unspent_tracking_post_commit_release(text, bigint[]);
DROP FUNCTION IF EXISTS unspent_tracking_post_commit_blocks(text, bytea[]);
DROP FUNCTION IF EXISTS unspent_tracking_post_commit_transactions(text, bytea[]);
DROP FUNCTION IF EXISTS unspent_tracking_post_commit(text, bigint[], boolean);
DROP FUNCTION IF EXISTS unspent_bits_resolve_outputs(bytea[]);
DROP FUNCTION IF EXISTS unspent_bits_clear_spent(bigint, bigint[]);
-- unspent_bits_accept, unspent_bits_resolve, unspent_bits_release and
-- unspent_tracking_release_spenders keep their pinned-plan bodies; the
-- 1791400000000_unspent_tracking down migration drops them.
