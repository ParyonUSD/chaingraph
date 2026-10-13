#!/usr/bin/env bash
# Run (and time) the unspent_node_ids backfill without the agent: one
# preparation, then every partition's batches in parallel, one psql session
# per partition. For labs; the agent runs the same functions itself when
# CHAINGRAPH_UNSPENT_NODE_IDS_BACKFILL=true.
#
# Usage: PGURL=postgres://... [PSQL="docker exec <container> psql"] bin/unspent-node-ids-backfill.sh [partitions] [prefixes-per-batch]
#
# Tracking must have started (the agent's first job pass, or
# `SELECT unspent_node_ids_initialize(<partitions>, <tx id>, <block id>, <event id>)`
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

wal_start=$(sql -c 'SELECT pg_current_wal_lsn()')
start=$(date +%s.%N)
sql -c 'BEGIN ISOLATION LEVEL REPEATABLE READ' -c "
  SELECT unspent_node_ids_backfill_prepare(
    (SELECT max(internal_id) FROM transaction),
    (SELECT COALESCE(max(internal_id), 0) FROM block),
    (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM unspent_tracking_events_id_seq))" -c 'COMMIT' >>"$log"
prepared=$(date +%s.%N)

run_partition() {
  local partition=$1 result
  while true; do
    result=$(sql -c 'BEGIN ISOLATION LEVEL REPEATABLE READ' \
      -c "SELECT unspent_node_ids_backfill_batch($partition, $partitions, $prefixes)" -c 'COMMIT' 2>&1) || true
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
end=$(date +%s.%N)
sql -c 'SELECT unspent_node_ids_delete_consumed_events()' >/dev/null
wal=$(sql -c "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '$wal_start')")
echo "prepare_s=$(echo "$prepared - $start" | bc) batches_s=$(echo "$end - $prepared" | bc) total_s=$(echo "$end - $start" | bc) wal_bytes=$wal pending=$(sql -c 'SELECT unspent_node_ids_backfill_pending()')"
exit $status
