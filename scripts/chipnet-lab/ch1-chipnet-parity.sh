#!/usr/bin/env bash
# Parity between chipnet_pg and chipnet_ch for the lab node at a common height
# (scripts/parity/compare.mjs of the frozen agent export; docs/clickhouse-port/parity-harness.md).
#
#   ch1-chipnet-parity.sh [--height H] [extra compare.mjs args…]
#
# H defaults to min(Postgres tip, ClickHouse tip) of the node. Mempool excluded (compare.mjs default),
# validation/acceptance timestamps excluded (the two stores ingest at different times; pass
# `--timestamps tolerance --ts-tolerance-ms N` to compare them instead). Tables: base + acceptance + history
# (`utxo` is left out: the ClickHouse run uses CHAINGRAPH_CLICKHOUSE_UTXO=off; `node_transaction` is mempool-only).
# Mismatched chunks are diffed (--diff). Output: $CHIPNET_RESULTS_DIR/parity-<UTC>-h<H>/ (parity.tsv,
# summary.json, diff.txt, compare.log). Exit code is compare.mjs's: 0 match, 1 mismatch, 2 error.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/ch1-chipnet-env.sh"

height=
if [[ ${1:-} == --height ]]; then height=$2; shift 2; fi
pg_tip=$(chipnet_store_height pg | cut -f1)
ch_tip=$(chipnet_store_height ch | cut -f1)
node_tip=$(chipnet_rpc getblockcount || echo NA)
if [[ -z $height ]]; then height=$((pg_tip < ch_tip ? pg_tip : ch_tip)); fi
if ((height < 0)); then echo "no common height (pg tip $pg_tip, ch tip $ch_tip)" >&2; exit 2; fi
out_dir=$CHIPNET_RESULTS_DIR/parity-$(date -u +%Y%m%dT%H%M%SZ)-h$height
mkdir -p "$out_dir"
echo "node $CHIPNET_NODE_NAME: chain tip $node_tip, pg tip $pg_tip, ch tip $ch_tip -> compare at height $height; out $out_dir"
# max_parallel_workers_per_gather=0: ch1-pg runs with Docker's default 64 MB /dev/shm, and parallel hash plans of the
# digest queries fail there with 53100 "could not resize shared memory segment".
tables=block,block_transaction,transaction,output,input,input_spent,node_block,tx_acceptance,node_block_history,node_transaction_history
set +e
(
  cd "$CHIPNET_LAB_DIR"
  node "$CHIPNET_AGENT_DIR/scripts/parity/compare.mjs" \
    --pg "$(chipnet_pg_url)?options=-c%20max_parallel_workers_per_gather%3D0" --ch "$CHIPNET_CH_URL" --ch-db "$CHIPNET_CH_DB" \
    --nodes "$CHIPNET_NODE_NAME" --at-height "$height" --tables "$tables" \
    --timestamps exclude --hash-chunks 16 --parallel "${CHIPNET_PARITY_PARALLEL:-4}" --diff \
    --out "$out_dir" "$@"
) 2>&1 | tee "$out_dir/compare.log"
status=${PIPESTATUS[0]}
set -e
printf 'pg_tip\t%s\nch_tip\t%s\nnode_tip\t%s\nheight\t%s\n' "$pg_tip" "$ch_tip" "$node_tip" "$height" >"$out_dir/tips.tsv"
echo "--- ALL rows (node, table, chunk, pg_count, ch_count, pg_md5, ch_md5, match)"
awk -F'\t' 'NR > 1 && ($3 == "ALL" || $8 != "yes")' "$out_dir/parity.tsv" 2>/dev/null | cut -f1-5,8 | head -60
exit "$status"
