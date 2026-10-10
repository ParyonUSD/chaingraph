#!/usr/bin/env bash
# WP6: wait (max $1 s, default 900) until no other ingestion-gate agent or ava run is active on this machine,
# so timing runs are not disturbed by other checkouts. Read-only (ps); prints what it waited for.
limit=${1:-900}
waited=0
while [ "$waited" -lt "$limit" ]; do
  # orphaned agents (parent pid 1, left behind by a killed gate) are idle and ignored
  busy=$(ps -A -o pid=,ppid=,command= | awk '$2 != 1' | grep -E 'ingestion-gate/lib/heap-sampler.mjs|node_modules/.bin/ava' | grep -v grep | cut -c1-160)
  [ -z "$busy" ] && break
  [ "$waited" -eq 0 ] && echo "wait-quiet: waiting for: $busy" >&2
  sleep 5
  waited=$((waited + 5))
done
[ "$waited" -ge "$limit" ] && echo "wait-quiet: still busy after ${limit}s, continuing: $busy" >&2
echo "wait-quiet: waited ${waited}s" >&2
