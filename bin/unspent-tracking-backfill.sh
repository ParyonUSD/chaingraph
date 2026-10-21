#!/usr/bin/env bash
# Run (and time) the unspent tracking backfill without the agent: one
# preparation, then every partition's batches in parallel, one psql session
# per partition. For measurements; the agent runs the same functions itself when
# CHAINGRAPH_UNSPENT_TRACKING_BACKFILL=true.
#
# Usage: PGURL=postgres://... [PSQL="docker exec <container> psql"] bin/unspent-tracking-backfill.sh [partitions] [prefixes-per-batch]
#
# Tracking must have started (the agent's first job pass, or
# `SELECT unspent_tracking_initialize(<partitions>, <tx id>, <block id>, <event id>)`
# with settled values on a static database). The floors passed to the
# preparation are read here; on a database the agent is writing to, run the
# backfill from the agent instead (it settles them first).
# Progress lines go to stderr; the summary (seconds, WAL bytes) to stdout.
set -euo pipefail
: "${PGURL:?set PGURL}"
partitions=${1:-$(${PSQL:-psql} "$PGURL" -qtAX -c 'SELECT count(*) FROM unspent_tracking_progress')}
prefixes=${2:-64}
log=${LOG:-/dev/stderr}
sql() { ${PSQL:-psql} "$PGURL" -qtAX -v ON_ERROR_STOP=1 "$@"; }
# milliseconds (the server's clock: no bc or GNU date needed), and as seconds
now_ms() { sql -c 'SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint'; }
seconds() { printf '%d.%03d' $(($1 / 1000)) $(($1 % 1000)); }

wal_start=$(sql -c 'SELECT pg_current_wal_lsn()')
start=$(now_ms)
event_floor=$(sql -c 'SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM unspent_tracking_events_id_seq')
# events above the floor are kept until the backfill is complete (published before the preparation's snapshot)
sql -c "SELECT unspent_tracking_backfill_retain_events($event_floor)" >/dev/null
sql -c 'BEGIN ISOLATION LEVEL REPEATABLE READ' -c "
  SELECT unspent_tracking_backfill_prepare(
    (SELECT max(internal_id) FROM transaction),
    (SELECT COALESCE(max(internal_id), 0) FROM block),
    $event_floor)" -c 'COMMIT' >>"$log"
prepared=$(now_ms)

run_partition() {
  local partition=$1 result
  while true; do
    result=$(sql -c 'BEGIN ISOLATION LEVEL REPEATABLE READ' \
      -c "SELECT unspent_tracking_backfill_batch($partition, $partitions, $prefixes)" -c 'COMMIT' 2>&1) || true
    echo "$partition $result" >>"$log"
    case "$result" in
      *'"done": true'*) return 0 ;;
      *serializ*) continue ;;
      *stale*|*unprepared*|*ERROR*) echo "partition $partition stopped: $result" >&2; return 1 ;;
    esac
  done
}
pids=()
for partition in $(seq 0 $((partitions - 1))); do
  run_partition "$partition" &
  pids+=($!)
done
status=0
for pid in "${pids[@]}"; do wait "$pid" || status=1; done
end=$(now_ms)
sql -c 'SELECT unspent_tracking_delete_consumed_events()' >/dev/null
wal=$(sql -c "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '$wal_start')")
echo "prepare_s=$(seconds $((prepared - start))) batches_s=$(seconds $((end - prepared))) total_s=$(seconds $((end - start))) wal_bytes=$wal pending=$(sql -c 'SELECT unspent_tracking_backfill_pending()')"
exit $status
