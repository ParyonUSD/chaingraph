# WP6b-C: deterministic ClickHouse e2e

WP6 (`wp6-local-measurement.md` §2) found the ClickHouse e2e green in 4 of 9 runs. This page lists every failure
mode, what the harness now does about each one, where the run time goes, and the stability proof.

Code: `src/e2e/e2e.spec.ts`, `src/e2e/e2e.spec.store.helper.ts`, `src/store/eventually.ts`. The store write path
(A) and visibility/views (B) changed in parallel; their commits are cited where they matter.

## 1. Failure catalogue

Sources:
- the 9 WP6 ClickHouse runs (`e2e-ch-{1,2,3}`, `-r2-*`, `-r3-*`, `-r4-*`; raw logs stay in the job scratch directory);
- the WP6b runs before the fixes.

| # | Test | Assertion / symptom | Seen | Root cause | Class |
|---|---|---|---|---|---|
| 1 | `saves block transactions if previously announced tx is seen but not yet saved` | `t.deepEqual(count, 3)`: actual 0 (`e2e.spec.ts:2346` at `bc42695`) | 5 of 9 (ch-1, ch-3, r2-1, r2-3, r4-3) | The block waits for a pending spend. Its commit is `incomplete` (parked), but the store logged "Saved new block", and the test read at once | log-before-visible |
| 2 | `syncs remaining blocks one-by-one` | uncaught `waited 10s for … height: 3200 … nodes: node2` | 2 (ch-3, r2-3) | 37 blocks per node fed one by one; ClickHouse needed more than 10 s while other agents' gates ran | fixed wait too short |
| 3 | `records stale blocks` | uncaught `waited 10s for … height: 3160 … nodes: node2` | 1 (r2-2) | 60 blocks per node in 6-block feeds, same as 2 | fixed wait too short |
| 4 | `restores sync-state from database on restart (during initial sync)` | uncaught `waited 10s for … Exiting...` | 1 (r3-3) | SIGINT during initial sync: shutdown drains every in-flight save (thousands on ClickHouse) before "Exiting..." | fixed wait too short |
| 5 | `completes initial sync` | uncaught `waited 10s for … Agent: initial sync is complete.` | 1 (r4-1) | Saves are concurrent, so "height 3000" is logged while lower heights are still in flight; completion came more than 10 s later (sync about 29 s in WP6) | fixed wait too short |
| 6 | all of 2–5 | each timeout was an **uncaught exception** (the timer threw) | 4 | It ended the test file. The remaining tests never ran, and `test.after.always` never dropped `cg_e2e_<pid>` (1.6 GB each; WP6 filled the Docker disk twice) | other (harness) |
| 7 | `ignores inbound transactions before initial sync is complete` | `sleep(1000)` then one read | 0 | Fixed sleep with a single read | fixed sleep |
| 8 | latent: `catches up a new node via headers`, `restores … (after initial sync)` | "enabled mempool tracking" / "Restored chain for node …" matched the **previous** agent's lines | 0 (no failure seen) | On ClickHouse the stdout buffer was never cleared (`clearStdoutBuffer` ran only in a `[postgres]` test, which is skipped there), so waits could pass on stale output. The next test could then start before the restarted agent tracked the mempool | other (harness) |
| 9 | latent: `syncs blocks as they arrive, handles multiple chain tips` | checks "new txs: 3/4" for block 3001 | 0 | Sends tx1, then announces the block at once. If the mempool commit (batched about 1 s on ClickHouse) lands after the block, the result is 4/4 | race (fixed sleep's absence) |
| 10 | WP6b, before d2a1c12 landed | `UNKNOWN_QUERY_PARAMETER: Substitution fence is not set`, 25 failures, then `RangeError: Invalid string length` | 1 (working tree with B's uncommitted views) | B's in-progress `050_views.sql` (new `fence`/`void` view parameters) did not match the committed checker. The agent then looped on errors, and the unbounded stdout buffer crashed the worker | A/B in-progress (B), plus a harness amplifier |
| 11 | WP6b, frozen `66a1620` | `restores … (during initial sync)`: "Exiting..." in the agent's log file at +1.9 s, never on stdout (60 s wait), so every later test cascaded | 1 | The agent logs through a pino transport worker, and the last stdout line can be lost when the process exits. Not reproduced in isolation or in the 12 proof runs | other (agent logging) |

Not a harness issue, recorded for A: `CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES=16` (tried at `92cff94` +
A's uncommitted `node-state.ts`) hung after "Agent: initial sync is complete.". "enabled mempool tracking" never came
within 60 s, and nothing more was logged. This looks like `finishInitialSync`'s drain waiting on a slot that is never
released. Cap 64 completed, but no faster than unbounded (26 s vs 22 s). The e2e does not set the cap.

## 2. Harness changes

| Change | Fixes |
|---|---|
| `waitForStdout` **rejects** on timeout and removes its waiter, so the timeout is a test failure instead of an uncaught exception. The run continues and `after.always` drops the database | 6 |
| Per-backend wait classes (Postgres keeps its old values; table below). `serialTest` gives each agent-driving test a 180 s AVA timeout on ClickHouse (`t.timeout` overrides the CLI `--timeout`) | 2–5 |
| Initial-sync and catch-up waits use `syncTimeoutMs` (120 s cap on ClickHouse) for "height 3000", "initial sync is complete", "enabled mempool tracking" and "accepted 2000/1162 existing blocks" | 5 |
| Every store read after a log line or agent event goes through `eventually` / `eventuallyEqual` with a per-backend timeout. Expected values are unchanged, and a timeout returns the last read so the assertion reports it. A later read of the same commit needs no poll (visibility is monotonic) | 1 and any future log-before-visible |
| `readTwice`: the negative check reads, waits 1 s and reads again, and asserts `[false, false]` | 7 |
| The stdout buffer is cleared after initial sync on ClickHouse as on Postgres, and on both backends before the first restart (so "Restored chain …" must come from the new agent) | 8 |
| "syncs blocks as they arrive" waits until tx1 is in node1's mempool before announcing block 3001 | 9 |
| Shutdown is asserted by the agent's exit code (0 = graceful; the forced-exit path exits 1). The process is SIGKILLed after `batchTimeoutMs`. A missing "Exiting..." line is logged, not failed | 4, 11 |
| The stdout buffer is capped at 64 MiB (keeps the newest half) | 10 (amplifier) |
| Startup cleanup drops `cg_e2e_*` databases whose pid is gone **or** that are older than 1 h (oldest table's `metadata_modification_time`; covers pid reuse) | 6 (leftover databases) |

| Wait class | Postgres | ClickHouse | Used for |
|---|---|---|---|
| `stdoutTimeoutMs` | 10 s | 30 s | one log line after one action |
| `batchTimeoutMs` | 10 s | 60 s | lines after tens of blocks (100-block re-org, stale blocks, one-by-one), shutdown drain |
| `syncTimeoutMs` | 60 s | 120 s | initial sync, catch-up of node4 |
| `visibleTimeoutMs` | 3 s | 10 s | a read after a save (default `eventually` was 3 s on both) |
| `syncVisibleTimeoutMs` | 10 s | 60 s | node4's accepted blocks, incomplete-block repair |

These are caps: a passing wait returns as soon as the line or value appears. The only remaining fixed delay is
`readTwice`'s 1 s gap in the negative check (it was `sleep(1000)` before).

## 3. Run time

| Build | ClickHouse e2e wall | `completes initial sync` | Postgres e2e |
|---|---|---|---|
| WP6 (`bc42695`), median of the green runs | 65.9 s | 28.9 s | 6.7 s |
| `92cff94` + A/B uncommitted, old harness (WP6b baseline) | 57 s | 22.5 s | — |
| `5c5413f` (A's multi-block commits) + new harness | 24 s (23–29 s, 9 runs) | 1.3–2.9 s | 6–7 s |

What dominated the old 29 s: the restarted agent re-saves all 3,001 blocks for 3 nodes with every save in flight at
once. Each block was about 11 inserts, so the ClickHouse insert path ran at about 165 blocks/s, and each save showed
13–14 s latency in the log. The other phases are small:

| Phase | Time |
|---|---|
| DDL apply (57–58 statements) | 0.3 s |
| Bulk-horizon UTXO build (`finishInitialSync`, 3 nodes) | 1.0–3.5 s |
| Projection materialisation | not separately visible (inside the inserts) |

No env the store honours skips or shrinks the horizon. The only knobs are `CHAINGRAPH_CLICKHOUSE_PENDING_SPEND_TIMEOUT_MS`
(already 1 ms in e2e) and `CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES` (no gain; 16 hangs, §1). Shortening the mockchain
would change the asserted heights, so the harness does not.

The per-block cost was fixed in the store instead: A's `5c5413f` "coalesce queued blocks into multi-block commits"
(WP6 §9 item 5). With it, `completes initial sync` is 2.4 s, and the whole ClickHouse e2e is 23 s, inside the 90 s
target. Breakdown of a 23 s run:
- startup and health check about 1 s;
- two restarts 2.3 s + 2.2 s;
- initial sync 2.4 s (of which the UTXO build is 1.0 s);
- the negative-check gap 1 s;
- mempool-commit batching about 1 s in each of the 5 tests that send transactions (double-spend 2.5 s);
- the rest under 1 s each.

## 4. Stability proof

Build: a frozen copy (`git archive` of `5c5413f` + this work's three files, `tsc`), so concurrent uncommitted
edits in the shared worktree did not leak in. Each run used a fresh `cg_e2e_<pid>`, dropped after the run.
Other agents' gates ran on the machine at the same time.

| Backend | Runs | Passed | Wall per run | `completes initial sync` |
|---|---|---|---|---|
| ClickHouse | 9 sequential | **9 of 9** (45 passed + 47 `[postgres]` skipped each) | 23, 24, 24, 24, 23, 24, 29, 24, 23 s | 2.4, 1.5, 1.6, 2.5, 1.3, 2.9, 2.4, 1.5, 1.5 s |
| Postgres (ch1-pg, localhost:15432) | 3 sequential | **3 of 3** (92/92) | 7, 6, 7 s | 2 s |

The "Exiting..." line (failure 11) reached stdout in all 12 runs (no "never reached stdout" warning). No databases
were left behind.

Command (as in `wp5b-checker-and-harness.md` §3):

```sh
CHAINGRAPH_E2E_STORE=clickhouse CHAINGRAPH_E2E_CLICKHOUSE_URL=http://localhost:18123 \
  npx ava --match='*[e2e]*' --timeout=60s build/e2e/e2e.spec.js
```
