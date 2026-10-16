<!-- cspell:ignore chipnet paryon golden backfill caffeinate pmset authchain getaddrinfo emption -->

# CH1 fix-pass-14 verification on golden, in-cluster driver (fix14c / fix14d) run log

Kit paryon_kubernetes-consolidation/experiments/chaingraph/ch1-fix14c/ (no-IAM mode `8d6e06f`, driver image
`ch1-driver:fix14c-8d6e06f@sha256:ea0ae0cc…`; re-run with `2a3c8c8`, image `ch1-driver:fix14c-2a3c8c8@sha256:d8d8718f…`;
plan docs/chaingraph/plans/clickhouse-phase2-fix14c-verification.md), agent image
agent:ch1-34793cf@sha256:307ef64e0c83ea3835e48c4361b1f1403ff771da8f9af8fd256001e119b09445 (port 34793cf), fresh cg_fix14 from
golden (`chaingraph-golden-20261007`, read only). Cluster paryon-c4d-noarray-test, pool ch1-c4d (spot only). Envelope:
spot only, HARD STOP 40 USD GCP all-in including the golden disk's tail until collect, no hold, **no IAM object created or
changed** (driver in no-IAM mode, `CH1_DRIVER_GCLOUD=off`). Ledger #112. Raw results:
chaingraph-performance/2026-10-16-fix14{c,d}-<run>/ (local only, never committed). UTC.

## Kit (phase 1)

- 10:04Z laptop on battery (89 %), lid open; `caffeinate -dims` held until each hand-off.
- No-IAM mode added: Kubernetes RBAC only, gcloud refused in the pod, k8s teardown + autoscaler removal of the empty
  node (pool min 0), phase `done-needs-collect`, `ch1-collect.sh` deletes the pool and golden disk. Rehearsed in kind
  (fake gcloud on the laptop side): budgetstop, happy, deadline, backstop + collect: all PASS. Commit `8d6e06f`.
- Kit bugs found: `imagetools inspect | grep -q` SIGPIPE in preflight; BSD sed lid parse; launch `wait_driver` died under
  set -e + pipefail before the status ConfigMap existed (`7b1f136`); one-shot ClickHouse SELECT checks not retried (`2a3c8c8`).
- `ch1-driver-iam.sh check`: FAIL (no GSA / roles / bindings), so gcloud off, as intended.

## fix14c (attempt 1)

- 10:41:34Z ch1-golden-pg created; 10:45:59Z ch1-results-fix14c; 10:47:57Z pool ch1-c4d SPOT on the first try
  (autoscaling enabled, min 0, max 1); deadline 20:17:57Z. Budget-stop smoke ch1BudgetStopComplete.
- 10:49:23Z driver s1.0 (in-cluster preflight) done; gcloud calls refused 0. `go` exited rc 1 silently in wait_driver
  (kit bug, fixed); re-run of `go` seeded the baselines (11 MB), HAND-OFF 10:50:05Z.
- 10:55:43Z s2.1: DDL applied (56 tables incl. authchain_link). 10:55:45Z **kubectl exec stream reset** (`connection
  reset by peer` to 10.92.0.1:443) emptied the `system.tables` check: false FATAL "cg_fix14.authchain_link missing" ->
  stop policy. **Not a data discrepancy** (the table was in the DDL's list two lines earlier).
- 10:56Z export + k8s teardown (namespaces, PVs) in 30 s; 11:07Z the autoscaler had removed the empty node (~11 min);
  phase done-needs-collect, COLLECT BEFORE 2026-10-19T03:56Z.
- 11:08:20Z `ch1-collect.sh`: pull verified (every file = PD copy; SHA256SUMS ok), pool deleted 11:08:39-11:08:55Z,
  golden disk deleted, results PD + PV + ch1-driver deleted 11:09:38Z, verify PASS with no exception. ~1.4 USD.

## fix14d (attempt 2, the README re-run tag)

- 11:10:43Z ch1-golden-pg; 11:13:48Z ch1-results-fix14d; 11:15:47Z pool SPOT first try (min 0 ok); deadline 20:45:47Z.
  Smoke ok; HAND-OFF 11:16:39Z after 6 min awake.
- s1.4d golden Postgres 330 s; **s2.1 backfill 1-7: 1,399 s** (#111 22.6 min), transform checks ok; s2.4 Hasura, API,
  params, payload, smoke ok; **s2.4g warm-up 46 ok, 0 failed on both pods (WU PASS)**; **s2.3 steps 8-9: 1,251 s**.
- s2.5a authchain wait from 12:06:54Z; the Job progressed to 360/422 batches, 152,314,803 rows by 12:36:09Z.
- **GKE auto-upgrade (cluster on the RAPID channel, no maintenance window):** UPGRADE_MASTER 12:19:42-12:28:50Z,
  **UPGRADE_NODES default-pool 12:34:18-12:39:26Z** (1.36.4-gke.2046002 -> gke.2326000): the default-pool node was
  replaced, the driver pod evicted (resumed on the new node at 12:37:31Z from the ledger, restarts=1, as designed) and
  cluster DNS moved; the authchain Job died on `getaddrinfo EAI_AGAIN clickhouse.ch1-backfill.svc.cluster.local`;
  12:38:35Z s2.5a "ch1-authchain-cg-fix14 failed" -> stop policy. **Not a data discrepancy.**
- 12:39Z export (14 s) + k8s teardown (64 s). UPGRADE_NODES ch1-c4d 12:45:19-12:46:54Z surged in a fresh empty c4d node
  (liqd) while the old one went; collect --force at 12:48Z deleted the pool (12:49:05-12:50:09Z) instead of waiting for
  the autoscaler; golden disk deleted 12:50:14Z; results PD 12:50:40Z; verify PASS with no exception. ~5.9 USD.

## Verdicts

- fix14c: `ch1-fix14-compare.mjs` 0 PASS / 0 FAIL / 41 not run; floor criteria 0 / 0 / 16 not run.
- fix14d: 1 PASS (WU) / 0 FAIL / 40 not run; floor criteria 0 / 0 / 16 not run.
- C1 (absolute <= 80 / <= 65 ms and paired), the per-document rule vs #107, X2, SM, LS, the lean-off A/B, PL, the
  regression rows and G2: **not run** in either attempt (section 4 never started).
- No data discrepancy in anything that ran.

## Spend and teardown

Node: 10:46:05-11:07:47Z (0.36 h) + 11:13:54-12:50:10Z (1.60 h) = 1.96 h x 3.08 = 6.05 USD. Golden disk:
10:41:34-11:09Z (0.46 h) + 11:10:43-12:50:14Z (1.66 h) = 2.12 h x 0.596 = 1.26 USD (tail after the node: ~2 min and 0).
Results PDs ~0.002 USD. **Total ~7.3 USD** of 40. Independent check 12:51Z: node pools default-pool + c4d-postgres
only (not ours); `disks --filter='name~^ch1'` 0 items; no `ch1*` namespace, PV or instance; golden snapshot READY.

## Why no third attempt

A third run on the plan's clock (T + 9.5 h) has a worst case of ~36.0 + 7.3 = 43.3 USD, over the hard stop; staying
inside needs a shorter deadline (a plan change) or a cluster maintenance exclusion (a cluster change): both outside the
envelope. Stopped and reported.
