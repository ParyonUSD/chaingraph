#!/usr/bin/env bash
# Run a server-fact check script against a ClickHouse HTTP endpoint, printing each statement's result.
# Unlike ddl/apply.sh it continues after errors (several checks expect errors) and uses one HTTP
# session for the whole file (needed for BEGIN/COMMIT).
#
#   run.sh <url> <check.sql>     CH_USER / CH_PASSWORD optional (sent via curl config on stdin)
set -uo pipefail

url="${1:?usage: run.sh <url> <check.sql>}"
check_file="${2:?usage: run.sh <url> <check.sql>}"
session_id="wp2check_$$_$RANDOM"
if [[ "$url" == *\?* ]]; then separator='&'; else separator='?'; fi
session_url="${url}${separator}session_id=${session_id}&session_timeout=120"

post_statement() {
  if [[ -n "${CH_USER:-}" ]]; then
    printf 'user = "%s:%s"\n' "$CH_USER" "${CH_PASSWORD:-}" |
      curl -sS --max-time 300 -K - --data-binary "$1" "$session_url"
  else
    curl -sS --max-time 300 --data-binary "$1" "$session_url"
  fi
}

statement_number=0
while IFS= read -r -d '' statement; do
  statement_number=$((statement_number + 1))
  first_line="$(printf '%s' "$statement" | grep -v '^[[:space:]]*$' | head -1)"
  printf '[%02d] %s\n' "$statement_number" "$first_line"
  post_statement "$statement" | LC_ALL=C sed 's/^/     /' | LC_ALL=C cut -c1-400
done < <(perl -0777 -ne '
    s/^\s*--.*\n//mg;
    for my $s (split /;[ \t]*(?:\n|\z)/) { print "$s\0" if $s =~ /\S/ }
  ' "$check_file")
