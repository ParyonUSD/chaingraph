# Local positive mempool proof, 2026-10-06

Measured clean source: `01f33f789183b407721a7191dee2351825ab4581`.
Agent implementation commit: `1e83eedc05451f99b2dbb5caab7fcfcdb574f65c`.
Commit01f33f7 changes only the disposable proof wrapper and its documentation.

Passing artifacts:
`/private/tmp/chaingraph-positive-mempool-01f3-20261006-v2/01-byteheavy/`.
The full result is `report.json`; raw protocol, runtime, PostgreSQL, and fixture
snapshots are retained alongside it. Cleanup reports
`STOPPED_AND_REMOVED_OWNED_RESOURCES`. No cloud deployment or push occurred.

## Actual automatic discovery

The byteheavy fork disconnects2 blocks and connects3. BCHN's resulting mempool
contains63 transactions. The actual agent automatically inserted63 matching
normalized acceptances. Trigger-to-branch acceptance was0.308482s;
trigger-to-eventual mempool drain was1.845587s. Exact ordered identities, the
full bounded block-OR-mempool membership oracle, and include-mempool UTXO samples
passed. No support was injected using SQL and no explicit transaction rebroadcast
was performed.

A second owned database restored from the pre-target checkpoint began with
zero normalized mempool rows. It caught up the alternate chain through actual
P2P, then underwent the same real restart needed by the fixture's historical
dates. The startup request discovered63 transactions, with exact matching
identities and the full bounded membership oracle passing. Clone, catchup,
restart, readiness, and discovery together took22.360881s; this is not a pure
startup request latency measurement.

## Protocol and permission evidence

`permission-peerinfo-incremental-2.json` records the inbound agent connection
as peer6, IP172.17.0.1, `permissions: ["mempool"]`, `relaytxes: true`, and zero
fee filter. `bchn.console.log` records its reorg MEMPOOL request at
15:21:28.443919Z, a2269-byte INV at15:21:29.429942Z, and ensuing GETDATA/TX
responses. That INV is1 count byte plus63 entries of36 bytes. The parsed agent
log `agent-incremental-2.ndjson` contains63 MSG_TX inventory entries and63
mempool insertion logs.

`permission-peerinfo-incremental-4.json` records the startup connection as
peer10 with the same narrow permission. Its MEMPOOL request is recorded at
15:21:52.423109Z and its2269-byte INV at15:21:53.090383Z. The startup agent
log contains63 MSG_TX entries and63 insertion logs. Reorg and startup mempool
identity files are equal. All captured BCHN blockchaininfo snapshots report
`initialblockdownload: false`.

The permission is required for trusted BCHN deployments unless NODE_BLOOM is
enabled: this agent patch does not configure that permission. The proof adds
only `-whitelist=mempool@172.17.0.1/32` to its own disposable node.

## Fixture clock correction and limits

The first patched-agent run, under the original fixed2023 node mocktime, received
the permitted requests but returned no inventory. It is retained at
`/private/tmp/chaingraph-positive-mempool-1e83-20261006-v1/`, including its failed
database dump and `fixture-clock-confound.json`. Exact pinned BCHN source mixes
non-mockable wall time for the inbound send deadline with a mockable time check
(`net_processing.cpp`, lines4357-4358 and4710-4713). Disabling mocktime on the
owned node fixes that test transport condition; production agent timing is
unchanged.

The measured result combines the requested agent BIP35 behavior with a coherent
fixture node clock. No unpatched agent control was rerun under this corrected
clock. Earlier absence is therefore clock-confounded and does not show that
the original deployed agent drops all mainnet mempool traffic. The raw request,
INV, GETDATA, TX, and database evidence directly establishes the new automatic
positive discovery path in this controlled fixture.

Build, lint, formatting, spelling, and41 unit tests passed, with one existing
todo. New tests verify the installed library's actual wire message, retention
of other-node cache acceptance and pending bodies, and positive-only behavior.
This single-node2/3 fork is not the separate100/101 deep-reorg gate. Actual
multi-node reorg acceptance, snapshots above50,000 entries, restrictive filters,
failed request retry, and production-runtime performance remain unproved.
