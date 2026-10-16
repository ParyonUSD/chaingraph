<!-- cspell:ignore chipnet paryon qlog txid golden backfill seqidle specoff warmup coldch caffeinate pmset pgrep emption leanoff -->

# CH1 fix-pass-14 verification on golden run log

Kit paryon_kubernetes-consolidation/experiments/chaingraph/ch1-fix14/ (c134859; plan
docs/chaingraph/plans/clickhouse-phase2-fix14-verification.md, 573a6b3), image
agent:ch1-34793cf@sha256:307ef64e0c83ea3835e48c4361b1f1403ff771da8f9af8fd256001e119b09445 (port 34793cf), fresh database
cg_fix14 from golden (`chaingraph-golden-20261007`, read only). What is checked: [fix-pass-14.md](fix-pass-14.md) §5; C1 on
the original fixed targets (<= 80 / <= 65 ms) AND the paired ratio. Cluster paryon-c4d-noarray-test, pool ch1-c4d (spot
only). Owner envelope: spot only (no spot / quota: re-check every 10 min for up to 6 h, then give up), HARD STOP 40 USD
GCP, no hold. Ledger #111. Raw results: chaingraph-performance/2026-10-16-fix14-<run>/ (local only, never committed). One
line per step, UTC.

- 2026-10-16T06:25Z **driver power:** `pmset -g batt` = AC Power (100 %, charged); `AppleClamshellState = No` (lid open). `caffeinate -dims -t 36000` started, PID 25549.
- 2026-10-16T06:26Z **preflight:** gcloud auth ok (richard@quicker.io, chaingraph1); `ch1-image.sh check` ok on 34793cf (fix-9..14 markers); digest sha256:307ef64e… = EXPORTS; **quota PASS: C4D limit 192, in use 0, free 192** (c4d-postgres pool not ours, untouched); `ch1-teardown-verify.sh` PASS; golden snapshot READY. (A first teardown-verify call before `CH1_DATE` was exported wrote to `/` and printed a spurious FAIL; re-run with the session env: PASS.)
- 2026-10-16T06:27Z rates refreshed (Cloud Billing Catalog, SKUs effective 2026-10-15T07:00Z, UNCHANGED: C4D spot core 0.01553/h, RAM 0.001658/GB-h, LSSD 0.0759/GiB-mo -> ~3.08 USD/h; Hyperdisk Balanced 0.091/GiB-mo + IOPS 0.006 + throughput 0.046 -> ~0.596 USD/h). Worst case at the deadline ~35.2 USD (< 40): go. Local secrets (0600); budget.json copied to 2026-10-16-fix14-{backfill,golden,api,teardown}. DOCR read-only token minted 06:27:19Z (expires 18:27:19Z, after the deadline).
- 2026-10-16T06:27:41Z disk ch1-golden-pg created from chaingraph-golden-20261007 (quota re-checked PASS), READY 06:30:23Z.
- 2026-10-16T06:32:25Z pool ch1-c4d created **SPOT on the first attempt** (start 06:30:36Z; quota re-checked PASS; config.spot True); node gke-paryon-c4d-noarray-test-ch1-c4d-0ed1d90d-sbv6 Ready. **deadlineUtc 2026-10-16T16:02:25Z** (pool + 9.5 h). Teardown starts by 15:32Z at the latest. Driver on AC, lid open.
- 2026-10-16T06:32:54Z namespaces applied; budget-stop CronJob ch1-backfill/ch1-budget-stop (schedule '2 16 16 10 *' Etc/UTC); smoke Job Complete: all 12 steps absent/deleted. armed=true in every budget.json. No workload before this point.
- 2026-10-16T06:38:56Z secrets applied (api_ro backstop 6 x 72 + 10 = 442); ClickHouse 26.8.22.13 up (48 cores, 2.73 TiB NVMe), ch1-tools ok; golden Postgres ready (pre-warm 307 s; PG 18.3; nodes 1 bchn-mainnet / 39 bchn-chipnet, tips 971,863 / 322,930). Chains run detached on the laptop (nohup, PID files; waits poll PIDs, no `pgrep -f`); the chain helper captures rc before any expansion.
