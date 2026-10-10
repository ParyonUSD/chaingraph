# CH1 Phase 1 lab session (WP7) run log

Kit: paryon_kubernetes-consolidation/experiments/chaingraph/ch1-phase1/ (README order). Cluster paryon-c4d-noarray-test, pool ch1-c4d.
Raw results: chaingraph-performance/2026-10-10-ch-<run>/ (local only). One line per step, UTC.

- 2026-10-10T02:26:20Z preflight: gcloud auth ok (richard@quicker.io, chaingraph1); pools default-pool, c4d-postgres, e19b, e19g (none ch1); no ch1 disks/namespaces; snapshots chaingraph-checkpoint-831862-indexed, bchn-checkpoint-831862, bchn-checkpoint-834000 READY; no chaingraph-ref-834000-*; AR repo chaingraph empty (port image not pushed yet).
- 2026-10-10T02:26Z secrets local + docr (DOCR read-only token expires 14:26Z).
- 2026-10-10T02:37:20Z disks created (start of spend): ch1-chain-bchn-tip (bchn-checkpoint-831862, 350 GiB), ch1-chain-bchn-834 (bchn-checkpoint-834000, 350 GiB), ch1-ref-pg (chaingraph-checkpoint-831862-indexed, 1000 GiB, 40k IOPS / 2400 MiB/s); all READY 02:40:35Z. Deviation: kit ch1-disks.sh fails under macOS /bin/bash 3.2 (empty perf[@] with set -u) - created with the equivalent bare gcloud commands.
- 2026-10-10T02:41:07Z budget.json written for every run (rates refreshed from the Billing Catalog: C4D spot London ~2.90 USD/h all-in, on-demand ~7.30; disks ~0.76 USD/h). deadlineUtc 2026-10-10T11:41:07Z (absolute).
- 2026-10-10T02:41:15Z kit frozen to chaingraph-performance/2026-10-10-ch-backfill/frozen (kit 2deee41, port ca26b8e). Image agent:ch1-ca26b8e@sha256:a9d01819e683...
- 2026-10-10T02:43:29Z pool ch1-c4d created SPOT on the first attempt (start 02:41:20Z); node gke-paryon-c4d-noarray-test-ch1-c4d-fbfc318b-ztv5 Ready.
- 2026-10-10T02:44:10Z namespaces applied; budget stop CronJob armed (schedule 41 11 10 10 *, default-pool), smoke Job Complete (all steps absent/deleted).
- 2026-10-10T02:44Z secrets apply + cloud (11 secrets). df probe: /mnt/stateful_partition/kube-ephemeral-ssd = md0 2.9T.
- 2026-10-10T02:45Z 10-chain applied (BCHN pods + prewarm); ch1-ref Postgres applied (pre-warm running; kit wait raced pod creation, re-run); ClickHouse 26.8.22.13 up (2.73 TiB free), ch1-tools up (pg ok, clickhouse client ok).
- 2026-10-10T02:49:59Z ch1-ref Postgres ready: pre-warm 301 s (755 GB), io_workers 32, shared_buffers 128GB, tip bchn-mainnet 831862, db 690 GB.
- 2026-10-10T02:47Z backfill step 1 (DDL 001-050 -> cg_base, 45 tables/views, image ca26b8e DDL CLI).
- 2026-10-10T02:50:39Z backfill steps 2-4: maxima height 831862, block id 831880, tx id 384120202, node 1, histories 0, node_transaction 0; raw pull 64 streams done 02:53:45Z (~3 min): block 831,863; transaction 384,115,079; block_transaction 384,115,081; output 1,049,443,990; input 942,309,288; node_block 831,863; no rc!=0.
- 2026-10-10T03:03:10Z backfill steps 5-6: transform done 03:02:58Z; all 15 checks ok=1 (staging = cg_base counts; tx_acceptance 384,115,081; no missing ids/hashes; max ids match).
- 2026-10-10T03:05Z backfill step 8 bookkeeping written; step 9 projections DDL 060 + MATERIALIZE started (input/output mutations ~15+ min per part).
- 2026-10-10T03:24Z fingerprint-replay (compare.mjs, --parallel 48, 256 hash chunks) started in parallel with the projection mutations (read-only; data/bookkeeping final).
- 2026-10-10T03:28:41Z backfill replay done: projections materialised (DDL 060 + MATERIALIZE, ~23 min, single-part mutations dominate). commit_log: seq 1099511627777 horizon_switch [], 1099511627778 committed kind backfill [1] (port has an explicit backfill kind; README expected block); visibility 0 and 1 at ...778; writer_lease epoch 1 (ch1-backfill) expired; id_reservation block/tx/node above maxima. cg_base sizes: input 462 GiB, output 194 GiB, transaction 35 GiB, block_transaction 29 GiB, tx_acceptance 30 GiB.
- 2026-10-10T03:29:19Z clones cg_base -> cg_bulk, cg_base -> cg_tip (ATTACH PARTITION FROM, 9 s each): all 12 non-empty tables equal, projection parts equal in all three dbs.
