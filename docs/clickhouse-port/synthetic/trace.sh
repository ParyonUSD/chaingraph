#!/usr/bin/env bash
# usage: trace.sh <db> <prefix>  -> per-read "marks by primary key" debug lines (clickhouse-client in the container)
db=$1; prefix=$2
args=$(node -e '
const p = JSON.parse(require("fs").readFileSync(process.argv[1] + ".params.json", "utf8"));
const out = [];
for (const [k, v] of Object.entries(p)) {
  const value = Array.isArray(v) ? `[${v.map((x) => (typeof x === "string" && !/^\d+$/u.test(x) ? `\x27${x}\x27` : x)).join(",")}]` : String(v);
  out.push(`--param_${k}=${value}`);
}
console.log(out.map((a) => JSON.stringify(a)).join(" "));
' "$prefix")
eval "set -- $args"
docker exec -i ch1-local clickhouse-client -d "$db" --send_logs_level=debug "$@" --query "$(cat $prefix.sql) FORMAT Null" 2>&1 | grep -E 'marks by primary key|Reading approx|projection|Selected .* parts' | sed -E 's/^.*<Debug> //' 
