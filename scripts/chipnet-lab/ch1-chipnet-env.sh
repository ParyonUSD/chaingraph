# shellcheck shell=bash
# Shared settings for the local chipnet lab (docs/clickhouse-port/chipnet-lab.md).
# Sourced by the ch1-chipnet-*.sh scripts; every value can be overridden from the environment.

CHIPNET_LAB_DIR=${CHIPNET_LAB_DIR:-/Users/rb/.claude/jobs/2d61cf2d/tmp/chipnet-lab}
# Frozen agent export (git archive of one commit + .yarn, offline install, yarn build).
CHIPNET_AGENT_DIR=${CHIPNET_AGENT_DIR:-$CHIPNET_LAB_DIR/agent-67ffa00}
# Raw results: never committed.
CHIPNET_RESULTS_DIR=${CHIPNET_RESULTS_DIR:-/Users/rb/dv/ParyonUSD/chaingraph-performance/2026-10-10-chipnet-local}
CHIPNET_PID_DIR=${CHIPNET_PID_DIR:-$CHIPNET_LAB_DIR/pids}

# The chipnet node (BCHN on the host). Same node name for both stores so parity compares like with like.
CHIPNET_NODE_NAME=${CHIPNET_NODE_NAME:-chipnet-local}
CHIPNET_NODE_P2P=${CHIPNET_NODE_P2P:-127.0.0.1:48333}
CHIPNET_NODE_RPC_URL=${CHIPNET_NODE_RPC_URL:-http://127.0.0.1:48332/}
# rpcuser / rpcpassword are read from this file into variables only (never printed).
CHIPNET_NODE_CONF=${CHIPNET_NODE_CONF:-/Users/rb/Library/Application Support/Bitcoin/bitcoin.conf}

# Stores.
CHIPNET_PG_CONTAINER=${CHIPNET_PG_CONTAINER:-ch1-pg}
CHIPNET_PG_BASE_URL=${CHIPNET_PG_BASE_URL:-postgres://chaingraph:very_insecure_postgres_password@localhost:15432}
CHIPNET_PG_DB=${CHIPNET_PG_DB:-chipnet_pg}
CHIPNET_PG_USER=${CHIPNET_PG_USER:-chaingraph}
CHIPNET_CH_URL=${CHIPNET_CH_URL:-http://localhost:18123}
CHIPNET_CH_DB=${CHIPNET_CH_DB:-chipnet_ch}

# Agent internal API ports (both agents may run at once while following the tip).
CHIPNET_PG_API_PORT=${CHIPNET_PG_API_PORT:-3201}
CHIPNET_CH_API_PORT=${CHIPNET_CH_API_PORT:-3202}

# Sampler interval (seconds) and the disk gate for starting a sync.
CHIPNET_SAMPLE_SECONDS=${CHIPNET_SAMPLE_SECONDS:-30}
# Gate = 3 x the combined from-genesis estimate (~18 GiB: Postgres ~12 incl. WAL and post-sync indexes, ClickHouse ~2
# + <= 0.5 transient inactive parts, ClickHouse system logs ~3-4), and >= 30 GiB. See docs/clickhouse-port/chipnet-lab.md.
CHIPNET_MIN_FREE_GIB=${CHIPNET_MIN_FREE_GIB:-55}
# While running, the sampler stops the agent below this much free space in the Docker VM.
CHIPNET_MIN_RUNNING_FREE_GIB=${CHIPNET_MIN_RUNNING_FREE_GIB:-4}

CHIPNET_SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

chipnet_pg_url() { printf '%s/%s' "$CHIPNET_PG_BASE_URL" "$CHIPNET_PG_DB"; }

# psql inside the Postgres container: chipnet_psql <db> [psql args…]
chipnet_psql() {
  local database=$1
  shift
  docker exec -i -e PGOPTIONS="-c client_min_messages=warning" "$CHIPNET_PG_CONTAINER" psql -X -q -v ON_ERROR_STOP=1 -U "$CHIPNET_PG_USER" -d "$database" "$@"
}

# ClickHouse HTTP query: chipnet_ch_query '<sql>' (TSV out).
chipnet_ch_query() {
  curl -sS --fail-with-body "$CHIPNET_CH_URL/" --data-binary "$1"
}

# Node RPC call: chipnet_rpc <method> [json params]; prints the JSON result field.
chipnet_rpc() {
  local method=$1 params=${2:-[]} rpc_user rpc_password
  rpc_user=$(grep -E '^rpcuser=' "$CHIPNET_NODE_CONF" | head -1 | cut -d= -f2-)
  rpc_password=$(grep -E '^rpcpassword=' "$CHIPNET_NODE_CONF" | head -1 | cut -d= -f2-)
  curl -sS --max-time 20 --user "$rpc_user:$rpc_password" -H 'content-type: text/plain' \
    --data "{\"jsonrpc\":\"1.0\",\"id\":\"ch1-chipnet\",\"method\":\"$method\",\"params\":$params}" \
    "$CHIPNET_NODE_RPC_URL" |
    node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const r=JSON.parse(s);if(r.error)process.exit(1);console.log(typeof r.result==="object"?JSON.stringify(r.result):String(r.result))})'
}

# Store visible height of the node: prints "<max height>\t<accepted block count>" (-1 / 0 when empty).
chipnet_store_height() {
  case $1 in
    pg)
      chipnet_psql "$CHIPNET_PG_DB" -At -F $'\t' -c "SELECT coalesce(max(b.height), -1), count(*)
        FROM node_block nb JOIN node n ON n.internal_id = nb.node_internal_id
        JOIN block b ON b.internal_id = nb.block_internal_id
        WHERE n.name = '$CHIPNET_NODE_NAME'" 2>/dev/null || printf -- '-1\t0\n'
      ;;
    ch)
      node "$CHIPNET_SCRIPT_DIR/ch1-chipnet-ch-height.mjs" "$CHIPNET_AGENT_DIR" "$CHIPNET_CH_URL" "$CHIPNET_CH_DB" "$CHIPNET_NODE_NAME" 2>/dev/null ||
        printf -- '-1\t0\n'
      ;;
  esac
}

# Free space (GiB, integer) of the Docker VM root filesystem.
chipnet_docker_free_gib() {
  docker run --rm alpine df -k / | awk 'NR == 2 { printf "%d\n", $4 / 1048576 }'
}
