#!/usr/bin/env bash
# Stop both lab agents and (with --yes) drop the lab databases chipnet_pg / chipnet_ch.
# Raw results in $CHIPNET_RESULTS_DIR and the frozen agent export are kept
# (--remove-export also deletes $CHIPNET_AGENT_DIR).
#
#   ch1-chipnet-cleanup.sh            stop agents + samplers only
#   ch1-chipnet-cleanup.sh --yes      … and drop both databases
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/ch1-chipnet-env.sh"

drop=0 remove_export=0
for argument in "$@"; do
  case $argument in
    --yes) drop=1 ;;
    --remove-export) remove_export=1 ;;
    *) echo "usage: $0 [--yes] [--remove-export]" >&2; exit 2 ;;
  esac
done

for store in pg ch; do "$CHIPNET_SCRIPT_DIR/ch1-chipnet-run.sh" "$store" stop || true; done
if ((drop)); then
  chipnet_psql postgres -c "DROP DATABASE IF EXISTS $CHIPNET_PG_DB WITH (FORCE)"
  echo "postgres: dropped $CHIPNET_PG_DB"
  chipnet_ch_query "DROP DATABASE IF EXISTS $CHIPNET_CH_DB SYNC" >/dev/null
  echo "clickhouse: dropped $CHIPNET_CH_DB"
fi
rm -f "$CHIPNET_PID_DIR"/ch1-chipnet-*.pid
rm -rf "$CHIPNET_LAB_DIR/work-pg" "$CHIPNET_LAB_DIR/work-ch"
if ((remove_export)); then rm -rf "$CHIPNET_AGENT_DIR"; echo "removed $CHIPNET_AGENT_DIR"; fi
echo "results kept in $CHIPNET_RESULTS_DIR"
