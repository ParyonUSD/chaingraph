\set ON_ERROR_STOP on
/* The previous finalizer acquired a global advisory lock. It is retired.
 * Use the fenced lock-free finalizer after complete initial sync. */
DO $retired$
BEGIN
  RAISE EXCEPTION 'legacy finalizer is retired; use performance_finalize_lock_free_arrays.sql from the GKE experiment tooling';
END
$retired$;
