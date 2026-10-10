#!/usr/bin/env bash
# Create the two chipnet lab databases (idempotent):
#   Postgres  $CHIPNET_PG_DB (chipnet_pg) on ch1-pg, every hasura migration applied once, in order
#   ClickHouse $CHIPNET_CH_DB (chipnet_ch) on ch1-local, the agent's DDL (bin/chaingraph-clickhouse-ddl.js)
# Migrations and DDL come from the frozen agent export ($CHIPNET_AGENT_DIR), not the live worktree.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/ch1-chipnet-env.sh"

mkdir -p "$CHIPNET_RESULTS_DIR"
migrations_dir=$CHIPNET_AGENT_DIR/images/hasura/hasura-data/migrations/default
[[ -d $migrations_dir ]] || { echo "no migrations at $migrations_dir (build the frozen export first)" >&2; exit 1; }
[[ -f $CHIPNET_AGENT_DIR/build/store/clickhouse/ddl-apply.js ]] || { echo "no build in $CHIPNET_AGENT_DIR (run yarn build there)" >&2; exit 1; }

# ---- Postgres
if [[ $(chipnet_psql postgres -At -c "SELECT 1 FROM pg_database WHERE datname = '$CHIPNET_PG_DB'") != 1 ]]; then
  chipnet_psql postgres -c "CREATE DATABASE $CHIPNET_PG_DB"
  echo "postgres: created $CHIPNET_PG_DB"
fi
# Applied migrations are recorded in a lab-only table, so a re-run skips them.
chipnet_psql "$CHIPNET_PG_DB" -c 'CREATE TABLE IF NOT EXISTS chipnet_lab_migration (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())'
applied=0 skipped=0
for migration_path in $(ls -d "$migrations_dir"/*/ | sort); do
  migration_name=$(basename "$migration_path")
  if [[ $(chipnet_psql "$CHIPNET_PG_DB" -At -c "SELECT 1 FROM chipnet_lab_migration WHERE name = '$migration_name'") == 1 ]]; then
    skipped=$((skipped + 1))
    continue
  fi
  # One transaction per migration, like the e2e harness (one multi-statement query per up.sql).
  { cat "$migration_path/up.sql"; printf "\n;INSERT INTO chipnet_lab_migration (name) VALUES ('%s');\n" "$migration_name"; } |
    chipnet_psql "$CHIPNET_PG_DB" -1 >/dev/null
  applied=$((applied + 1))
done
echo "postgres: $CHIPNET_PG_DB migrations applied=$applied already=$skipped"

# ---- ClickHouse
(cd "$CHIPNET_AGENT_DIR" &&
  CHAINGRAPH_CLICKHOUSE_URL=$CHIPNET_CH_URL CHAINGRAPH_CLICKHOUSE_DATABASE=$CHIPNET_CH_DB \
    node bin/chaingraph-clickhouse-ddl.js) 2>&1 | tee "$CHIPNET_RESULTS_DIR/ddl-$(date -u +%Y%m%dT%H%M%SZ).log"
# Lab-only part cleanup (server defaults: old_parts_lifetime 480 s, cleanup_delay_period 30 s). A from-genesis sync of
# tiny chipnet blocks commits ~15 times/s; every commit and merge leaves inactive parts (~95 KB each per table) on disk
# until cleanup. With the defaults the first smoke run reached ~8k inactive parts per table (~7 GB) in two minutes and
# filled the Docker VM; with old_parts_lifetime 30 s it still reached 3.5 GB in 40 s. With the settings below inactive
# bytes stayed at 0.2-0.5 GB at the same rate (~880 blocks/s). Set CHIPNET_CH_PART_CLEANUP= (empty) to keep the defaults.
part_cleanup=${CHIPNET_CH_PART_CLEANUP-old_parts_lifetime = 5, cleanup_delay_period = 1, max_cleanup_delay_period = 5, cleanup_delay_period_random_add = 1}
if [[ -n $part_cleanup ]]; then
  for table in $(chipnet_ch_query "SELECT name FROM system.tables WHERE database = '$CHIPNET_CH_DB' AND engine LIKE '%MergeTree' FORMAT TSV"); do
    chipnet_ch_query "ALTER TABLE $CHIPNET_CH_DB.$table MODIFY SETTING $part_cleanup" >/dev/null
  done
  echo "clickhouse: $CHIPNET_CH_DB MergeTree tables: $part_cleanup (lab-only)"
fi
echo "clickhouse: $CHIPNET_CH_DB ready"
