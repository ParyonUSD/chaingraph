#!/usr/bin/env bash
# WP6: one ingestion-gate run, optionally with the torn-read poller alongside.
#   scripts/measure/gate-with-poller.sh <agent-dir> <out-dir> <label> <poller:yes|no> -- <run.mjs args...>
# Writes <out-dir>/<label>.json (gate report), <label>.log (gate stdout) and, with the poller,
# <label>.poller.json. Prints "label wall_s=… exit=…".
set -u
agentDirectory=$1 outputDirectory=$2 label=$3 withPoller=$4
shift 5
harnessDirectory=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$outputDirectory"
pollerPid=
if [ "$withPoller" = yes ]; then
  node "$harnessDirectory/measure/torn-read-poller.mjs" --agent-dir "$agentDirectory" --out "$outputDirectory/$label.poller.json" \
    >"$outputDirectory/$label.poller.log" 2>&1 &
  pollerPid=$!
fi
"$harnessDirectory/measure/wait-quiet.sh" "${WAIT_QUIET_S:-900}"
started=$(perl -MTime::HiRes=time -e 'printf "%.3f", time')
node "$harnessDirectory/ingestion-gate/run.mjs" --agent-dir "$agentDirectory" --out "$outputDirectory/$label.json" --label "$label" "$@" \
  >"$outputDirectory/$label.log" 2>&1
exitCode=$?
finished=$(perl -MTime::HiRes=time -e 'printf "%.3f", time')
if [ -n "$pollerPid" ]; then
  kill -INT "$pollerPid"
  wait "$pollerPid"
  tail -1 "$outputDirectory/$label.poller.log"
fi
echo "$label wall_s=$(echo "$finished - $started" | bc) exit=$exitCode"
