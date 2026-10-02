\set ON_ERROR_STOP on

SELECT pg_advisory_lock(20261001, 1);

DO $guard$
BEGIN
  IF (SELECT phase FROM output_membership_backfill.state WHERE id)
    <> 'backfilled' THEN
    RAISE EXCEPTION 'completed fast backfill is required';
  END IF;

  IF (SELECT phase FROM output_membership.state WHERE id)
    NOT IN ('backfilled', 'indexing', 'indexed', 'ready') THEN
    RAISE EXCEPTION 'canonical output membership state is not complete';
  END IF;
END
$guard$;

DROP TABLE IF EXISTS output_membership_backfill.desired_output;
DROP TABLE IF EXISTS output_membership_backfill.accepted_transaction;
SELECT pg_advisory_unlock(20261001, 1);
