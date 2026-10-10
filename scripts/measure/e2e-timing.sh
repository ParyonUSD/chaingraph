#!/usr/bin/env bash
# WP6: time an ava spec run N times on one backend (see docs/clickhouse-port/wp6-local-measurement.md).
#   scripts/measure/e2e-timing.sh <agent-dir> <out-dir> <label> <runs> -- <ava args...>
# Waits until the e2e mock-node port 19333 is free (other checkouts run the same e2e on fixed ports),
# then runs `npx ava <args>` in <agent-dir>; logs to <out-dir>/<label>-<n>.log, prints one
# "label run wall_s passed skipped failed exit" line per run.
set -u
agentDirectory=$1 outputDirectory=$2 label=$3 runs=$4
shift 5
mkdir -p "$outputDirectory"
for run in $(seq 1 "$runs"); do
  while lsof -nP -iTCP:19333 -sTCP:LISTEN >/dev/null 2>&1 || lsof -nP -iTCP:3200 -sTCP:LISTEN >/dev/null 2>&1; do sleep 2; done
  "$(dirname "$0")/wait-quiet.sh" "${WAIT_QUIET_S:-900}"
  logFile="$outputDirectory/$label-$run.log"
  started=$(perl -MTime::HiRes=time -e 'printf "%.3f", time')
  (cd "$agentDirectory" && npx ava "$@") >"$logFile" 2>&1
  exitCode=$?
  finished=$(perl -MTime::HiRes=time -e 'printf "%.3f", time')
  passed=$(grep -Eo '[0-9]+ tests? passed' "$logFile" | grep -Eo '^[0-9]+' | tail -1)
  skipped=$(grep -Eo '[0-9]+ tests? skipped' "$logFile" | grep -Eo '^[0-9]+' | tail -1)
  failed=$(grep -Eo '[0-9]+ (tests? failed|uncaught exceptions?)' "$logFile" | grep -Eo '^[0-9]+' | paste -sd+ - | bc 2>/dev/null)
  echo "$label run=$run wall_s=$(echo "$finished - $started" | bc) passed=${passed:-0} skipped=${skipped:-0} failed=${failed:-0} exit=$exitCode"
done
