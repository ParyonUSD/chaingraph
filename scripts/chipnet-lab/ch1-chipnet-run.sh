#!/usr/bin/env bash
# Run the agent for one store against the local chipnet node, detached (nohup + pid files).
#
#   ch1-chipnet-run.sh pg|ch [start] [--force]   start agent + sampler (from genesis if the store is empty)
#   ch1-chipnet-run.sh pg|ch stop                SIGTERM agent (graceful) and sampler
#   ch1-chipnet-run.sh pg|ch status              pids, latest sample, markers
#   ch1-chipnet-run.sh pg|ch summary [run dir]   (re)write summary.json of a run
#
# Each start writes a run dir $CHIPNET_RESULTS_DIR/<store>-<UTC>/ (symlink <store>-latest):
#   agent.log     agent stdout/stderr (pino JSON, NODE_ENV=production)
#   samples.tsv   every $CHIPNET_SAMPLE_SECONDS s: utc, elapsed_s, node_height, store_height, store_blocks
#                 (+ ch: active_parts, max_parts_per_partition, running_merges, bytes_on_disk; pg: db_bytes)
#   markers.tsv   start, initial_sync_complete (agent log line), tip_reached (store height >= node height)
#   summary.json  wall time to tip, tx count at that height (node RPC), tx/s, max event-loop delay, …
# The sampler stops the agent (SIGTERM, marker disk_guard_stop) if the Docker VM has < $CHIPNET_MIN_RUNNING_FREE_GIB GiB free.
# Start refuses (unless --force) if the store is empty and the Docker VM has < $CHIPNET_MIN_FREE_GIB GiB free.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/ch1-chipnet-env.sh"

store=${1:-}
action=${2:-start}
[[ $action == --force ]] && action=start
force=0
for argument in "$@"; do [[ $argument == --force ]] && force=1; done
case $store in pg | ch) ;; *) echo "usage: $0 pg|ch [start|stop|status|summary] [--force]" >&2; exit 2 ;; esac

process_name=ch1-chipnet-$store
agent_pid_file=$CHIPNET_PID_DIR/$process_name.pid
sampler_pid_file=$CHIPNET_PID_DIR/$process_name-sampler.pid
mkdir -p "$CHIPNET_PID_DIR" "$CHIPNET_RESULTS_DIR"

pid_alive() { [[ -f $1 ]] && kill -0 "$(cat "$1")" 2>/dev/null; }
now_utc() { date -u +%Y-%m-%dT%H:%M:%SZ; }
marker_time() { awk -F'\t' -v k="$2" '$1 == k { print $2; exit }' "$1/markers.tsv" 2>/dev/null; }
epoch_of() { date -j -u -f %Y-%m-%dT%H:%M:%SZ "$1" +%s; }

ch_metrics() {
  chipnet_ch_query "SELECT
      (SELECT count() FROM system.parts WHERE database = '$CHIPNET_CH_DB' AND active),
      (SELECT max(c) FROM (SELECT count() AS c FROM system.parts WHERE database = '$CHIPNET_CH_DB' AND active GROUP BY table, partition_id)),
      (SELECT count() FROM system.merges WHERE database = '$CHIPNET_CH_DB'),
      (SELECT sum(bytes_on_disk) FROM system.parts WHERE database = '$CHIPNET_CH_DB' AND active),
      (SELECT sum(bytes_on_disk) FROM system.parts WHERE database = '$CHIPNET_CH_DB' AND NOT active)
    FORMAT TSV" 2>/dev/null || printf 'NA\tNA\tNA\tNA\tNA\n'
}
# Free bytes of the Docker VM filesystem (ClickHouse's default disk lives on it).
vm_free_bytes() {
  chipnet_ch_query "SELECT free_space FROM system.disks WHERE name = 'default' FORMAT TSV" 2>/dev/null || echo NA
}
pg_metrics() {
  chipnet_psql "$CHIPNET_PG_DB" -At -c "SELECT pg_database_size('$CHIPNET_PG_DB')" 2>/dev/null || echo NA
}

write_summary() {
  local run_dir=$1 start sync_complete tip_reached tip_height txcount=NA tx_per_s=NA wall=NA hash
  start=$(marker_time "$run_dir" start)
  sync_complete=$(marker_time "$run_dir" initial_sync_complete)
  tip_reached=$(marker_time "$run_dir" tip_reached)
  tip_height=$(awk -F'\t' '$1 == "tip_reached" { print $3; exit }' "$run_dir/markers.tsv" 2>/dev/null)
  if [[ -n $tip_reached && -n $start ]]; then
    wall=$(($(epoch_of "$tip_reached") - $(epoch_of "$start")))
    hash=$(chipnet_rpc getblockhash "[$tip_height]" || true)
    if [[ -n $hash ]]; then
      txcount=$(chipnet_rpc getchaintxstats "[1,\"$hash\"]" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).txcount))' || echo NA)
      [[ $txcount != NA && $wall -gt 0 ]] && tx_per_s=$(awk -v t="$txcount" -v w="$wall" 'BEGIN { printf "%.0f", t / w }')
    fi
  fi
  # Max event-loop delay over the run (CHAINGRAPH_EVENT_LOOP_DIAGNOSTIC_MS lines).
  local loop_max loop_windows_over_1s
  loop_max=$(grep -o '"maxMs":[0-9.]*' "$run_dir/agent.log" 2>/dev/null | cut -d: -f2 | sort -g | tail -1 || true)
  loop_windows_over_1s=$(grep -o '"maxMs":[0-9.]*' "$run_dir/agent.log" 2>/dev/null | cut -d: -f2 | awk '$1 >= 1000' | wc -l | tr -d ' ')
  local errors warnings
  errors=$(grep -c '"level":[56]0' "$run_dir/agent.log" 2>/dev/null || true)
  warnings=$(grep -c '"level":40' "$run_dir/agent.log" 2>/dev/null || true)
  local last_sample
  last_sample=$(tail -1 "$run_dir/samples.tsv" 2>/dev/null | tr '\t' ' ')
  cat >"$run_dir/summary.json" <<JSON
{
  "store": "$store",
  "node": "$CHIPNET_NODE_NAME",
  "start": "$start",
  "initial_sync_complete": "${sync_complete:-}",
  "tip_reached": "${tip_reached:-}",
  "tip_height": "${tip_height:-}",
  "wall_seconds_to_tip": "$wall",
  "chain_txcount_at_tip": "$txcount",
  "tx_per_second": "$tx_per_s",
  "event_loop_max_ms": "${loop_max:-}",
  "event_loop_windows_over_1s": "$loop_windows_over_1s",
  "log_error_lines": "${errors:-0}",
  "log_warn_lines": "${warnings:-0}",
  "last_sample": "$last_sample"
}
JSON
  cat "$run_dir/summary.json"
}

sample_loop() {
  local run_dir=$1 agent_pid start_epoch
  agent_pid=$(cat "$agent_pid_file")
  start_epoch=$(epoch_of "$(marker_time "$run_dir" start)")
  while kill -0 "$agent_pid" 2>/dev/null; do
    local node_height store_line metrics now
    now=$(now_utc)
    node_height=$(chipnet_rpc getblockcount || echo NA)
    store_line=$(chipnet_store_height "$store")
    if [[ $store == ch ]]; then metrics=$(ch_metrics); else metrics=$(pg_metrics); fi
    local vm_free
    vm_free=$(vm_free_bytes)
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$now" $(($(date +%s) - start_epoch)) "$node_height" "$store_line" "$metrics" "$vm_free" >>"$run_dir/samples.tsv"
    # Disk guard: the Docker VM is shared with other work; stop the agent before the disk fills.
    if [[ $vm_free != NA ]] && ((vm_free < CHIPNET_MIN_RUNNING_FREE_GIB * 1073741824)); then
      printf 'disk_guard_stop\t%s\t%s\n' "$now" "$vm_free" >>"$run_dir/markers.tsv"
      kill -TERM "$agent_pid" 2>/dev/null || true
    fi
    if [[ -z $(marker_time "$run_dir" initial_sync_complete) ]] && grep -q 'initial sync is complete' "$run_dir/agent.log" 2>/dev/null; then
      local log_ms
      log_ms=$(grep -m1 'initial sync is complete' "$run_dir/agent.log" | grep -o '"time":[0-9]*' | cut -d: -f2)
      printf 'initial_sync_complete\t%s\t%s\n' "$(date -u -r $((log_ms / 1000)) +%Y-%m-%dT%H:%M:%SZ)" "$(cut -f1 <<<"$store_line")" >>"$run_dir/markers.tsv"
    fi
    local store_height store_blocks
    store_height=$(cut -f1 <<<"$store_line")
    store_blocks=$(cut -f2 <<<"$store_line")
    # Tip = every block 0..node height accepted (blocks are saved out of order during initial sync, so max(height)
    # alone reaches the tip early) and the agent has logged the end of initial sync (Postgres builds indexes then).
    if [[ -z $(marker_time "$run_dir" tip_reached) && $node_height != NA && $store_height -ge $node_height &&
      $store_blocks -gt $node_height && -n $(marker_time "$run_dir" initial_sync_complete) ]]; then
      printf 'tip_reached\t%s\t%s\n' "$now" "$store_height" >>"$run_dir/markers.tsv"
      write_summary "$run_dir" >/dev/null || true
    fi
    sleep "$CHIPNET_SAMPLE_SECONDS"
  done
  printf 'agent_exit\t%s\n' "$(now_utc)" >>"$run_dir/markers.tsv"
  write_summary "$run_dir" >/dev/null || true
}

case $action in
  start)
    if pid_alive "$agent_pid_file"; then echo "$process_name already running (pid $(cat "$agent_pid_file"))" >&2; exit 1; fi
    [[ -f $CHIPNET_AGENT_DIR/build/index.js ]] || { echo "no build in $CHIPNET_AGENT_DIR" >&2; exit 1; }
    initial_height=$(chipnet_store_height "$store" | cut -f1)
    if [[ $initial_height == -1 && $force == 0 ]]; then
      free_gib=$(chipnet_docker_free_gib)
      if ((free_gib < CHIPNET_MIN_FREE_GIB)); then
        echo "disk gate: Docker VM has ${free_gib} GiB free, need >= ${CHIPNET_MIN_FREE_GIB} GiB for a sync from genesis (override: --force)" >&2
        exit 3
      fi
    fi
    run_dir=$CHIPNET_RESULTS_DIR/$store-$(date -u +%Y%m%dT%H%M%SZ)
    work_dir=$CHIPNET_LAB_DIR/work-$store
    mkdir -p "$run_dir" "$work_dir"
    : >"$work_dir/.env" # config.ts requires a .env in the working directory
    ln -sfn "$run_dir" "$CHIPNET_RESULTS_DIR/$store-latest"
    agent_env=(
      NODE_ENV=production
      CHAINGRAPH_LOG_PATH=false
      CHAINGRAPH_LOG_LEVEL_STDOUT="${CHIPNET_LOG_LEVEL:-info}"
      CHAINGRAPH_TRUSTED_NODES="$CHIPNET_NODE_NAME:${CHIPNET_NODE_P2P%:*}:${CHIPNET_NODE_P2P##*:}:chipnet"
      CHAINGRAPH_USER_AGENT="/$process_name/"
      CHAINGRAPH_EVENT_LOOP_DIAGNOSTIC_MS="${CHIPNET_EVENT_LOOP_DIAGNOSTIC_MS:-10000}"
    )
    if [[ $store == pg ]]; then
      agent_env+=(
        CHAINGRAPH_STORE=postgres
        CHAINGRAPH_POSTGRES_CONNECTION_STRING="$(chipnet_pg_url)"
        CHAINGRAPH_INTERNAL_API_PORT="$CHIPNET_PG_API_PORT"
      )
    else
      agent_env+=(
        CHAINGRAPH_STORE=clickhouse
        CHAINGRAPH_CLICKHOUSE_URL="$CHIPNET_CH_URL"
        CHAINGRAPH_CLICKHOUSE_DATABASE="$CHIPNET_CH_DB"
        CHAINGRAPH_CLICKHOUSE_UTXO="${CHIPNET_CH_UTXO:-off}"
        CHAINGRAPH_POSTGRES_CONNECTION_STRING=postgres://unused:unused@127.0.0.1:1/unused
        CHAINGRAPH_INTERNAL_API_PORT="$CHIPNET_CH_API_PORT"
      )
      # Set to empty to leave the agent's default (cap: unbounded; blocks per commit: 64, with the batch linger).
      in_flight=${CHIPNET_CH_MAX_IN_FLIGHT_SAVES-16}
      blocks_per_commit=${CHIPNET_CH_MAX_BLOCKS_PER_COMMIT-64}
      if [[ -n $in_flight ]]; then agent_env+=(CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES="$in_flight"); fi
      if [[ -n $blocks_per_commit ]]; then agent_env+=(CHAINGRAPH_CLICKHOUSE_MAX_BLOCKS_PER_COMMIT="$blocks_per_commit"); fi
    fi
    node_options=${CHIPNET_NODE_OPTIONS:---max-old-space-size=8192}
    # Non-secret settings of this run (the Postgres URL holds only the lab's well-known password).
    { printf '%s\n' "${agent_env[@]}"; echo "NODE_OPTIONS=$node_options"; echo "AGENT_DIR=$CHIPNET_AGENT_DIR"; } >"$run_dir/run.env"
    printf 'start\t%s\t%s\n' "$(now_utc)" "$initial_height" >"$run_dir/markers.tsv"
    printf 'utc\telapsed_s\tnode_height\tstore_height\tstore_blocks\t%s\tvm_free_bytes\n' \
      "$([[ $store == ch ]] && printf 'active_parts\tmax_parts_per_partition\trunning_merges\tactive_bytes\tinactive_bytes' || printf 'db_bytes')" >"$run_dir/samples.tsv"
    (
      cd "$work_dir"
      # shellcheck disable=SC2086
      nohup env "${agent_env[@]}" bash -c "exec -a $process_name node --title=$process_name $node_options '$CHIPNET_AGENT_DIR/bin/chaingraph.js'" \
        >"$run_dir/agent.log" 2>&1 </dev/null &
      echo $! >"$agent_pid_file"
    )
    nohup bash -c "exec -a $process_name-sampler bash '${BASH_SOURCE[0]}' $store _sample '$run_dir'" \
      >>"$run_dir/sampler.log" 2>&1 </dev/null &
    echo $! >"$sampler_pid_file"
    echo "$process_name started: agent pid $(cat "$agent_pid_file"), sampler pid $(cat "$sampler_pid_file"), run dir $run_dir"
    ;;
  _sample) sample_loop "$3" ;;
  stop)
    for pid_file in "$agent_pid_file" "$sampler_pid_file"; do
      if pid_alive "$pid_file"; then
        kill -TERM "$(cat "$pid_file")" && echo "sent SIGTERM to $(basename "$pid_file" .pid) (pid $(cat "$pid_file"))"
      fi
    done
    for _ in $(seq 1 60); do pid_alive "$agent_pid_file" || break; sleep 1; done
    pid_alive "$agent_pid_file" && echo "agent still running after 60 s (pid $(cat "$agent_pid_file"))" >&2
    run_dir=$(readlink "$CHIPNET_RESULTS_DIR/$store-latest" || true)
    [[ -n $run_dir ]] && { printf 'stopped\t%s\n' "$(now_utc)" >>"$run_dir/markers.tsv"; write_summary "$run_dir" >/dev/null || true; }
    ;;
  status)
    pid_alive "$agent_pid_file" && echo "agent: running pid $(cat "$agent_pid_file")" || echo "agent: not running"
    pid_alive "$sampler_pid_file" && echo "sampler: running pid $(cat "$sampler_pid_file")" || echo "sampler: not running"
    run_dir=$(readlink "$CHIPNET_RESULTS_DIR/$store-latest" || true)
    if [[ -n $run_dir ]]; then
      echo "run dir: $run_dir"
      cat "$run_dir/markers.tsv"
      head -1 "$run_dir/samples.tsv"; tail -1 "$run_dir/samples.tsv"
    fi
    ;;
  summary) write_summary "${3:-$(readlink "$CHIPNET_RESULTS_DIR/$store-latest")}" ;;
  *) echo "unknown action $action" >&2; exit 2 ;;
esac
