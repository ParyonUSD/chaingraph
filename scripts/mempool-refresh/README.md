# Positive mempool refresh proof

The agent requests BIP35 mempool inventory after initial sync enables tracking,
after registration on a reconnect when tracking is already enabled, and after
the stale block removal commits during a reorganization. Requests use the main
inbound peer. The existing inventory path restores known node acceptance or
requests missing transaction bodies through GETDATA.

Before requesting, only the requested node's cached acknowledgments of saved
transactions are forgotten. The bodies, pending unsaved transaction sources,
and all other node acknowledgments are preserved. This makes repeated positive
announcements effective after block confirmation removed normalized mempool
support. No database acceptance is inferred from cache invalidation.

There is no negative snapshot reconciliation. BIP35 inventory can be chunked at
50,000 entries and has no completion marker. Missing hashes, silence, empty
inventory, or a filtered response never authorize removing node acceptance.
Normal block confirmation, conflict handling, and expiration remain in use.

BCHN29 permits MEMPOOL only with NODE_BLOOM or the requesting peer's mempool
permission. An operator must permit that request on trusted node connections;
this patch does not change node permissions. The controlled proof below uses
only `-whitelist=mempool@172.17.0.1/32` on its new owner-labeled disposable node.
Earlier actual peerinfo observed that host agent IP. The wrapper rejects a run
if either actual agent connection has another IP or any additional permission.

Build and commit this worktree first; the underlying harness requires a clean,
compiled source tree at the explicitly supplied revision. Run from this folder:

```sh
python3 benchmark.py \
  --base-harness /Users/rb/dv/ParyonUSD/paryon_kubernetes-gke-plan/experiments/chaingraph/gke-test/large-block-fixture/benchmark_agent.py \
  --agent-peer-ip 172.17.0.1 \
  --agent-source /Users/rb/dv/ParyonUSD/chaingraph-mempool-refresh \
  --expected-agent-revision FULL_CLEAN_COMMIT \
  --fixture /private/tmp/chaingraph-large-block-byteheavy-runtime-20261006 \
  --fork /private/tmp/chaingraph-byteheavy-mempool-fork-20261006 \
  --mempool-reentry --timeout 180 \
  --artifacts /private/tmp/chaingraph-positive-mempool-new
```

The wrapper imports the existing harness without editing it, snapshots both
harnesses, and retains the original reorg report and identity/oracle checks.
It then restores the empty-mempool checkpoint into a separate owned database,
catches up the alternate branch over actual P2P, and performs the same real
restart required by the fixture's historical block dates. The startup request
must automatically discover the nonempty source mempool, with exact transaction
identities and the full bounded block-OR-mempool oracle passing. It adds no SQL
mempool support and performs no explicit transaction rebroadcast.

This is a small disposable fixture proof, not a production deployment gate.
Large snapshots, restrictive filters, failed requests, expiration timing,
multi-node races, and deployment runtime performance require separate evidence.
