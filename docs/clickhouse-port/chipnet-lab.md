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
  D=/Users/rb/.claude/jobs/2d61cf2d/tmp/chipnet-lab/agent-50e8d2e
  mkdir -p $D && git archive 50e8d2e | tar -x -C $D && git -C .yarn archive a9d1c68 | tar -x -C $D/.yarn
  (cd $D && YARN_ENABLE_NETWORK=0 yarn install --immutable --immutable-cache && yarn build)
  ```

  Parity uses that export's `scripts/parity/compare.mjs` too. The runs below used 50e8d2e, which includes the
  shutdown fix (689c461) and the optional `.env` fix. The smoke runs used 67ffa00.
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
  `tip_reached` (first sample with every block 0..node height accepted, after `initial_sync_complete`; blocks are
  saved out of order, so `max(height)` alone reaches the tip early), `disk_guard_stop`, `agent_exit`, `stopped`.
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
- The Postgres connection sets `max_parallel_workers_per_gather=0`: ch1-pg has Docker's default 64 MB `/dev/shm`,
  and parallel hash plans of the digest queries fail with 53100 `could not resize shared memory segment`.
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

## Results (2026-10-10, agent 50e8d2e)

Setup:
- Docker Desktop VM: 14 CPUs, ~42 GB RAM, 850 GiB free.
- ch1-pg: Postgres 14, stock settings.
- ch1-local: ClickHouse 26.8, with the lab part-cleanup settings.
- Chipnet: 327,302 blocks, 2,844,337 txs. The node's `size_on_disk` is 3.24 GB.
- The two syncs ran one after the other, each from genesis. Raw results:
  `chaingraph-performance/2026-10-10-chipnet-local/`.

### Sync from genesis

| | Postgres | ClickHouse (UTXO off, cap 16, 64 blocks/commit) |
|---|---|---|
| start | 18:33:11Z | 18:45:09Z |
| max height = node tip | +427 s (blocks still missing: 326,490 of 327,302) | – |
| `initial sync is complete` | +484 s, then index build (5 indexes) | +466 s |
| every block accepted (tip) | **+701 s** (sample resolution 30 s) | **+484 s** |
| tx/s to tip | **4,058** (2,844,308 / 701 s); 6,661 to max height | **5,877** (2,844,337 / 484 s) |
| max event-loop delay (10 s windows) | 3,687 ms, 8 windows ≥ 1 s | **871 ms**, 0 windows ≥ 1 s |
| errors / warnings in the log | 0 / 50 (`Statistic duration bug`, benign) | 0 / 0 |
| size after sync | 5.95 GB (`pg_database_size`, with indexes) | 4.73 GiB on disk (3.4 GiB compressed data) |

ClickHouse detail:
- Size: `input` 2.51 GiB on disk (6.95 M rows), `output` 1.40 GiB (7.83 M rows), `transaction` 263 MiB,
  `tx_acceptance` 238 MiB, `block_transaction` 225 MiB, `block` 76 MiB.
- Parts: at most 86 active parts, at most 17 parts in one partition. The sync ended at 75 active parts.
- Inactive bytes peaked at 1.90 GB (at 313k blocks), even with the lab cleanup settings. They fell to ~0 once the
  sync settled.
- `part_log` over the run:
  - 316,908 new parts (5.32 GiB);
  - 84,038 merges, which wrote 49.4 GiB (9.3× the inserted bytes) in 1,004 s of merge time;
  - 0 delayed or rejected inserts.

Wall time and tx/s compare the time until every block was accepted. Postgres builds its indexes after
`initial sync is complete`. It accepts the last ~800 blocks only after that build, at +701 s.

### Parity at H = 327,302 (both stopped after their sync)

| table | rows | result |
|---|---|---|
| block | 327,303 | match |
| block_transaction, transaction | 2,844,337 | match |
| output | 7,833,656 | match |
| input | 6,951,133 | match |
| input_spent | 6,623,830 | match |
| tx_acceptance | 2,844,337 | match |
| node_block_history, node_transaction_history | 0 | match |
| node_block | 327,303 | **1 chunk mismatch**: blocks 327,293 and 327,294 have `accepted_at` set on Postgres and NULL on ClickHouse |

The `node_block` difference is expected for syncs run one after the other. The agent sets `accepted_at` only when
the block time is later than now − 2 h at save time (`agent.ts` ~1759). Postgres saved those two blocks (block times
16:48 / 16:52Z) at ~18:40Z, inside the window. ClickHouse saved them at ~18:52Z, outside it. The difference is in
the wall clock, not in the stores.

### Follow the tip for 30 min (both agents at once, 18:55–19:25Z), parity at H = 327,309

- Both agents followed 7 new blocks with no errors and no re-orgs.
- Max event-loop delay: Postgres 94 ms, ClickHouse 85 ms.
- SIGTERM on the ClickHouse agent no longer logs a fatal line (fixed in 689c461).

| table | result |
|---|---|
| block (327,310), block_transaction / transaction (2,844,555), output (7,834,218), input (6,951,654), tx_acceptance, node_block_history | match |
| node_block | same 2-block `accepted_at` difference as above (expected) |
| node_transaction_history | 179 vs 167 rows. 13 Postgres-only txs were validated 18:42–18:45Z, while the Postgres agent ran after its sync and ClickHouse was not running. The 1 ClickHouse-only tx (`d9444345…`) was heard 18:53:18Z, after the ClickHouse sync. This is mempool timing, not a store bug. |
| **input_spent** | **mismatch, ClickHouse bug**: 3 inputs of tx `d9444345…` (block 327,303) have spent-output columns `0 / '' / no token` on ClickHouse |

### Bug: input spent-output columns never filled after a mempool child-before-parent across a restart

1. At 18:53:18Z, after `initial sync is complete`, the ClickHouse agent stored mempool tx `d9444345…` in commit
   `1099511662525` (`mempool_batch`). Its parents `3263755e…:0` and `1911b769…:1,2` were not known yet. The commit
   wrote `input` with stand-in spent-output columns and 3 `pending_spend` +1 rows.
2. The agent was then stopped and restarted.
3. The new epoch saved block 327,303 in commit `2199023255554`. The block holds the parents (tx index 5 and 11) and
   the child (index 39). It wrote the parents' `output` rows and the 3 `pending_spend` −1 rows, so the spends count
   as resolved.
4. No new `input` row was written for the child. `input` still holds only the epoch-1 stand-in rows. The child was
   already stored ("new txs: 45/46"), so its inputs were not re-written, and the fill step left them unfilled.

Postgres is correct here: it joins `input` to `output`. ClickHouse API readers of the denormalised spent-output
columns would see value 0 and empty bytecode for these inputs. Next step: a regression test for "mempool child
stored with pending spends, restart, block containing parent and child". Possibly also without the restart, if the
fill skips already-stored children in general. The databases are kept as they are for inspection.

## Footprint estimate (before the runs)

The estimate was ~18 GiB combined, so the gate is 3 × 18 ≈ 55 GiB free and ≥ 30 GiB:
- Postgres ~12 GiB: the mainnet ratio of ~3.1× the chain, plus WAL and the post-sync indexes.
- ClickHouse ~2 GiB, plus ≤ 0.5 GiB of inactive parts.
- ClickHouse system logs ~3–4 GiB.

Measured: Postgres 5.95 GB, ClickHouse 4.73 GiB, and an inactive-parts peak of 1.9 GB. The 93.9 GB VM (~11 GiB
free) failed the gate. The VM enlarged to ~980 GB passes it.

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
  or should log it below fatal. Fixed in 689c461: the 50e8d2e runs logged no error on SIGTERM.
