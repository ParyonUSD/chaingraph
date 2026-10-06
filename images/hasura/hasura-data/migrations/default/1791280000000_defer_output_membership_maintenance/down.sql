/* Rollback deliberately does not restore the rejected advisory-lock path.
 * Rebuilding membership and selecting a compatible agent is an explicit
 * operational action. Automatic rollback would expose stale arrays. */
DO $rollback$
BEGIN
  RAISE EXCEPTION 'automatic rollback of deferred membership is unsupported; restore a reviewed database and agent together';
END
$rollback$;
