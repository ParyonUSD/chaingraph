<!-- cspell:ignore chipnet BCHN nohup getblockcount getchaintxstats pids -->

# Local chipnet lab: Postgres vs ClickHouse from genesis

Owner decision 2026-10-10: local development is chipnet only; mainnet testing happens on GKE. This lab syncs chipnet
from genesis into both stores off the owner's local BCHN chipnet node, then compares per-node parity and sync speed.
No cloud use.

Scripts: `scripts/chipnet-lab/` (bash + one small `.mjs`). Settings: `ch1-chipnet-env.sh`, every value overridable
from the environment.

| Script | Does |
|---|---|
| `ch1-chipnet-setup.sh` | Idempotent. Postgres `chipnet_pg` on `ch1-pg` with every hasura migration applied once, in order (one transaction per `up.sql`, as the e2e harness does; applied names kept in `chipnet_lab_migration`). ClickHouse `chipnet_ch` on `ch1-local` via `bin/chaingraph-clickhouse-ddl.js` (log kept in the results dir), then the lab-only part cleanup settings (below). |
| `ch1-chipnet-run.sh pg\|ch [start\|stop\|status\|summary] [--force]` | Starts one agent detached (`nohup`, pid files in `$CHIPNET_LAB_DIR/pids`, process names `ch1-chipnet-<store>` / `ch1-chipnet-<store>-sampler`). `stop` sends SIGTERM to the pids in the files only. |
| `ch1-chipnet-ch-height.mjs` | The node's visible height in ClickHouse: `max(height)` of the agent's pinned `node_block_at` on one gated snapshot (compiled store of the frozen agent). |
| `ch1-chipnet-parity.sh [--height H] [compare args…]` | `scripts/parity/compare.mjs` between the two databases at `H = min(pg tip, ch tip)`. |
| `ch1-chipnet-cleanup.sh [--yes] [--remove-export]` | Stops both agents; with `--yes` drops both databases. Raw results are kept. |

## Inputs

- **Node:** BitcoinCash Node `-chipnet` on the host, P2P `127.0.0.1:48333` (whitelisted), RPC `127.0.0.1:48332`.
  RPC credentials are read from the node's `bitcoin.conf` into shell variables only (never printed). The node's
  config is never edited and the node is never restarted.
- **Agent:** a frozen export of one commit, never the live worktree (another agent edits it concurrently):

  ```sh
  D=/Users/rb/.claude/jobs/2d61cf2d/tmp/chipnet-lab/agent-67ffa00
  mkdir -p $D && git archive 67ffa00 | tar -x -C $D && git -C .yarn archive HEAD | tar -x -C $D/.yarn
  (cd $D && YARN_ENABLE_NETWORK=0 yarn install --immutable --immutable-cache && yarn build)
  ```

  Parity uses that export's `scripts/parity/compare.mjs` too.
- **Results:** `/Users/rb/dv/ParyonUSD/chaingraph-performance/2026-10-10-chipnet-local/` (`CHIPNET_RESULTS_DIR`),
  never committed. Do not run python with its cwd there.

## Agent settings

Both runs: `CHAINGRAPH_TRUSTED_NODES=chipnet-local:127.0.0.1:48333:chipnet`. The node name is the same for both runs,
so parity compares like with like. Also `NODE_ENV=production` (JSON log lines on stdout → `agent.log`),
`CHAINGRAPH_LOG_PATH=false`, `CHAINGRAPH_LOG_LEVEL_STDOUT=info`, `CHAINGRAPH_EVENT_LOOP_DIAGNOSTIC_MS=10000` and
`NODE_OPTIONS=--max-old-space-size=8192`. The agent's cwd is `$CHIPNET_LAB_DIR/work-<store>`, which holds an empty
`.env`, because `config.ts` at 67ffa00 requires one (since fix pass 3 the file is optional).

| | Postgres run | ClickHouse run |
|---|---|---|
| store | `CHAINGRAPH_STORE=postgres`, `CHAINGRAPH_POSTGRES_CONNECTION_STRING=…/chipnet_pg` | `CHAINGRAPH_STORE=clickhouse`, `CHAINGRAPH_CLICKHOUSE_URL=http://localhost:18123`, `…_DATABASE=chipnet_ch` |
| tuning | defaults (`MAX_CONNECTIONS` = CPU count) | `CHAINGRAPH_CLICKHOUSE_UTXO=off`, `…_MAX_IN_FLIGHT_SAVES=16`, `…_MAX_BLOCKS_PER_COMMIT=64` |
| internal API port | 3201 | 3202 |

Both agents can run at once (different ports, two P2P connections to the same node). That is needed for the
follow-the-tip phase.

## What a run records

`$CHIPNET_RESULTS_DIR/<store>-<UTC>/` (symlink `<store>-latest`):

- `agent.log`: the agent's stdout/stderr.
- `run.env`: the non-secret settings.
- `samples.tsv`: every `CHIPNET_SAMPLE_SECONDS` (30 s): node `getblockcount`, the store's visible height and its
  accepted block count. Then for ClickHouse active parts, max parts per partition, running merges, active and
  inactive bytes; for Postgres the database size. Last, the Docker VM free bytes (ClickHouse `system.disks`).
- `markers.tsv`: `start`, `initial_sync_complete` (time of the agent's `initial sync is complete` log line),
  `tip_reached` (first sample with store height ≥ node height), `disk_guard_stop`, `agent_exit`, `stopped`.
- `summary.json`: wall seconds start → tip, the chain's tx count at that height (`getchaintxstats` at the block),
  tx/s, the max event-loop delay (`eventLoopDelay.maxMs`) and the number of windows ≥ 1 s, error/warn line counts.

**Visible height.**
- Postgres: `max(block.height)` over the node's `node_block` rows.
- ClickHouse: `max(height)` of `node_block_at` on one gated snapshot, via `ch1-chipnet-ch-height.mjs`.

**Guards.**
- Start refuses a sync from genesis when the Docker VM has less than `CHIPNET_MIN_FREE_GIB` (55) GiB free (see
  Footprint). `--force` skips this check.
- While running, the sampler sends SIGTERM to the agent if the VM drops below `CHIPNET_MIN_RUNNING_FREE_GIB` (4) GiB
  free. The VM is shared with other work: `ch1-pg` refuses connections when it is full.

## Parity

`ch1-chipnet-parity.sh`:

- compares at `--at-height H`, `H = min(both tips)` (or `--height H`);
- tables: `block, block_transaction, transaction, output, input, input_spent, node_block, tx_acceptance,
  node_block_history, node_transaction_history`;
- options: `--timestamps exclude`, `--hash-chunks 16`, `--diff`;
- mempool is excluded (the compare.mjs default).

Notes on that choice:
- `utxo` is left out: the ClickHouse run has `UTXO=off`, and Postgres has no F1g function in this lab.
- `node_transaction` is mempool-only, so it is not compared.
- Timestamps are excluded because the two stores ingest at different times (one sync after the other). For a tolerance
  check on runs that overlap in time, add `--timestamps tolerance --ts-tolerance-ms N`.
- Output goes to `parity-<UTC>-h<H>/` (`parity.tsv`, `summary.json`, `diff.txt`, `compare.log`, `tips.tsv`). The exit
  code is compare.mjs's.

`node_transaction_history` is not height-scoped. While both agents follow the tip, history rows of mempool
transactions can differ by timing (one agent archived a tx the other has not seen yet). Read a mismatch there with
the diff before calling it a bug.

## Procedure

```sh
S=scripts/chipnet-lab
$S/ch1-chipnet-setup.sh
$S/ch1-chipnet-run.sh pg            # wait for tip_reached in markers.tsv (status: $S/ch1-chipnet-run.sh pg status)
$S/ch1-chipnet-run.sh pg stop
$S/ch1-chipnet-run.sh ch            # one sync at a time: they share the VM
$S/ch1-chipnet-run.sh ch stop
$S/ch1-chipnet-parity.sh            # at min(tips)
$S/ch1-chipnet-run.sh pg; $S/ch1-chipnet-run.sh ch   # both follow the tip for 30 min
$S/ch1-chipnet-parity.sh            # at the new common height (live tip, re-orgs if any)
$S/ch1-chipnet-cleanup.sh --yes     # when done
```

## Footprint (chipnet 327,296 blocks, 2.84 M txs, 2026-10-10)

The node's `size_on_disk` is 3.24 GB (blocks dir 3.1 GB including undo data).

| Part | Estimate | Basis |
|---|---|---|
| Postgres | ~12 GiB peak | mainnet ratio 690 GB Postgres / ~220 GB chain ≈ 3.1×; plus ~1 GB per-block overhead (327k blocks), WAL ~1–2 GB, post-sync indexes |
| ClickHouse data | ~2 GiB | at 87k blocks: 81 MB active vs Postgres 190 MB at 63k |
| ClickHouse inactive parts | ≤ 0.5 GiB with the lab cleanup settings (7+ GiB without) | smoke runs, below |
| ClickHouse `system` logs | ~3–4 GiB growth | the agent issues ~2–3 queries per block; `query_log` is already 6.3 GiB and `processors_profile_log` 1.8 GiB |
| **Combined** | **~18 GiB** | gate: free ≥ 3 × 18 ≈ 55 GiB (and ≥ 30 GiB) |

On 2026-10-10 the Docker VM had a 93.9 GB disk with ~11 GiB free, so the syncs were not started. Truncating
`system.query_log` and `system.processors_profile_log` on ch1-local frees ~8 GiB, which is still short. Raising
Docker Desktop's disk to ~200 GB is enough.

## Smoke-run findings (2026-10-10, a few minutes, databases reset afterwards)

- **Throughput.** Both stores sync the early, mostly empty chipnet blocks at ~880–1,000 blocks/s.
- **Parity.** Postgres stopped at 63,003 and ClickHouse at 44,628. Parity at 44,628 was `ALL MATCH` on every table
  (44,629 blocks, 46,710 txs, 67,862 outputs, 69,933 inputs, 25,304 spent inputs; histories empty) in 0.8 s.
- **ClickHouse inactive parts filled the VM.** With 64 blocks per commit at ~900 blocks/s, the store commits ~15
  times/s. Each commit and each merge leaves inactive parts (~95 KB per part per table) until cleanup.
  - With the server defaults (`old_parts_lifetime` 480 s) a 2-minute run reached ~8k inactive parts per table. That
    is ~7 GB, and it filled the Docker VM: ClickHouse returned `Cannot reserve 1.00 MiB, not enough space` and
    `ch1-pg` refused connections. Dropping `chipnet_ch` freed the VM at once.
  - With `old_parts_lifetime = 30` it still reached 3.5 GB in 40 s, and the disk guard stopped it.
  - With `old_parts_lifetime = 5, cleanup_delay_period = 1, max_cleanup_delay_period = 5,
    cleanup_delay_period_random_add = 1` it stayed at 0.2–0.5 GB at the same rate. The setup applies these as
    lab-only table settings (`CHIPNET_CH_PART_CLEANUP=` to skip). The production DDL is unchanged.
  - This is a fact about initial sync of small blocks, not of chipnet only. A from-genesis mainnet sync spends its
    first ~500k blocks in the same regime.
- **SIGTERM on the ClickHouse agent logs a fatal line.** Every stop logged `commit_void` (warn) for in-flight
  intents, then `void_refused` (level 60) and `abort_failed` (error) for a node-agnostic commit that had already
  committed (`Refusing to void commit …: it is committed`). Nothing was damaged: both restarts resumed from the
  visible height (44,628 and 34,785), and parity matched. The shutdown path should not try to void a committed seq,
  or should log it below fatal.
