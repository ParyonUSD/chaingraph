#!/usr/bin/env bash
# Apply the ClickHouse DDL (NNN_*.sql, in order) to a ClickHouse HTTP endpoint.
#
#   apply.sh <url> [database]
#
#   url       e.g. http://localhost:18123 or https://<host>:8443
#   database  optional; rewrites the literal `cg` database name (e.g. a scratch db)
#   CH_USER / CH_PASSWORD  optional credentials (passed to curl via a config on stdin, never argv)
#   DDL items (optional/<name>.sql) applied after the numbered files: the default ones (p_node_tx) unless named in
#   CHAINGRAPH_CLICKHOUSE_DDL_SKIP (comma-separated), plus any named in CHAINGRAPH_CLICKHOUSE_DDL_OPTIONAL.
#             No MATERIALIZE: meant for new databases.
#
# Statements are split on a ';' at end of line; full-line '--' comments are dropped.
# Every statement is idempotent (IF NOT EXISTS), so re-running is safe.
set -euo pipefail

url="${1:?usage: apply.sh <clickhouse-http-url> [database]}"
database="${2:-cg}"
ddl_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

post_statement() {
  local statement="$1"
  if [[ -n "${CH_USER:-}" ]]; then
    printf 'user = "%s:%s"\n' "$CH_USER" "${CH_PASSWORD:-}" |
      curl -sS --fail-with-body --max-time 300 -K - --data-binary "$statement" "$url"
  else
    curl -sS --fail-with-body --max-time 300 --data-binary "$statement" "$url"
  fi
}

sql_files=("$ddl_dir"/[0-9][0-9][0-9]_*.sql)
default_items="p_node_tx"
IFS=',' read -r -a optional_items <<<"${default_items},${CHAINGRAPH_CLICKHOUSE_DDL_OPTIONAL:-}"
IFS=',' read -r -a skipped_items <<<"${CHAINGRAPH_CLICKHOUSE_DDL_SKIP:-}"
applied_items=","
for item in ${skipped_items[@]+"${skipped_items[@]}"}; do
  item="${item// /}"
  [[ -z "$item" ]] && continue
  if [[ ! -f "$ddl_dir/optional/$item.sql" ]]; then
    echo "unknown optional DDL item: $item" >&2
    exit 1
  fi
done
for item in ${optional_items[@]+"${optional_items[@]}"}; do
  item="${item// /}"
  [[ -z "$item" ]] && continue
  if [[ ! -f "$ddl_dir/optional/$item.sql" ]]; then
    echo "unknown optional DDL item: $item" >&2
    exit 1
  fi
  [[ ",${CHAINGRAPH_CLICKHOUSE_DDL_SKIP:-}," == *",$item,"* ]] && continue
  [[ "$applied_items" == *",$item,"* ]] && continue
  applied_items="${applied_items}${item},"
  sql_files+=("$ddl_dir/optional/$item.sql")
done

for sql_file in "${sql_files[@]}"; do
  echo "== $(basename "$sql_file")"
  statement_count=0
  while IFS= read -r -d '' statement; do
    if ! response="$(post_statement "$statement" 2>&1)"; then
      echo "FAILED in $(basename "$sql_file"): $response" >&2
      echo "$statement" | head -5 >&2
      exit 1
    fi
    statement_count=$((statement_count + 1))
  done < <(DB="$database" perl -0777 -ne '
      s/^\s*--.*\n//mg;
      s/\bcg\./$ENV{DB}./g;
      s/(DATABASE IF NOT EXISTS )cg\b/$1$ENV{DB}/g;
      for my $s (split /;[ \t]*(?:\n|\z)/) { print "$s\0" if $s =~ /\S/ }
    ' "$sql_file")
  echo "   $statement_count statement(s) ok"
done
