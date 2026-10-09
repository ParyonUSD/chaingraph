# WP4: commit protocol, visibility gate, ids and writer lease

Plan: `paryon_kubernetes/docs/chaingraph/plans/clickhouse-primary-store.md` §1 (per-node checklist), §2.6, §3.1–§3.4.
Code: `src/store/clickhouse/{client,commit-log,visibility,id-allocator,writer-lease}.ts` (+ `*.spec.ts`, `test-support.ts`).
DDL amendments: `src/store/clickhouse/ddl/040_bookkeeping.sql`, `050_views.sql`, listed in `ddl/README.md` (items 18–22).

This doc answers the five risks from the WP2 review: (a) torn reads across views, (b) incomplete commits
stalling the watermark, (c) dedup tokens and retries, (d) the advisory lease, (e) WP5 hand-offs.

## 1. Commit identity: `commit_seq` and dedup tokens (c)

- **Layout.** `commit_seq = (writer_epoch << 40) | counter`, with `counter >= 1`. The epoch is the lease epoch (§5).
  Each epoch owns the range `[epoch << 40, (epoch + 1) << 40)` (1.1 × 10¹² commits per epoch, 2²⁴ epochs).
  - Seqs are monotonic across restarts with no read: a new epoch's first seq is above every seq of every older epoch.
    `CommitLog.init()` still reads `max(commit_seq)` and refuses to start if it is already in the new epoch or above
    (`CommitStateError`: two writers on one epoch).
  - Two writers can never produce the same seq, so they can never produce the same dedup token.
  - A stale writer's possible seqs are a known closed range, which is what makes fencing possible (§5).
- **Token scheme.** Every commit-critical insert carries `insert_deduplication_token = seq:table:chunk`
  (`dedupToken()`, `OpenCommit.token(table, chunk)`):

  | Insert | Token |
  |---|---|
  | data rows of one table in one commit | `seq:table:N`, N = 0, 1, … in a deterministic chunking of the commit's rows |
  | late rows of an `incomplete` commit (pending-spend fill, §2) | `seq:table:fN` |
  | `commit_log` state rows | `seq:commit_log:intent` / `:incomplete` / `:committed` / `:aborted` |
  | `commit_void` | `seq:commit_void:0` |
  | `epoch_fence` (once per takeover) | `epoch:epoch_fence:0` |
  | `id_reservation` | `epoch:id_reservation:kind:range_start` |

  All of them run with `async_insert = 0`, `insert_deduplicate = 1`, `wait_end_of_query = 1`
  (`ClickHouseClient.insertRowBinary` / `insertSelect`), and every agent-written table has
  `non_replicated_deduplication_window = 10000` (WP2 check a: without it, plain MergeTree ignores tokens).
- **Every retry is idempotent.** A retry re-sends the same rows with the same token.
  - Duplicate delivery (the first attempt landed, the client saw a timeout): the second is dropped by token. If the
    first attempt lands *after* the retry, the first is dropped. Multi-block inserts are deduplicated per block
    (`token_0`, `token_1`, … server-side), so a partially applied insert completes on retry without duplicates.
  - The 10,000-insert window is never the limit: retries happen within seconds, inside one process. After a crash
    nothing is retried under the old seq: the commit is aborted and redone under a new seq (new tokens).
  - Bookkeeping rows are deterministic in their parameters (timestamps are taken once per commit in memory), and
    "same token, different data" is dropped anyway (WP2 check a), so a retried state row cannot change state.
  - `visibility` rows are not tokenised: readers take `max(visible_seq)`, so a duplicate is harmless.
  - The lease's heartbeat rows disable dedup on purpose (`insert_deduplicate = 0`): each is a distinct fact.
- **Duplicate `+1` rows on `utxo` (constant `version`).** Tokens cover retries only. A duplicate transition emitted
  by an agent bug is not a retry and cannot be caught by a token; it leaves `sum(sign) = 2` and the outpoint stays
  unspent after its `−1`. Guards: the single state machine in the agent (WP5), the commit's `row_counts`, and the §2.3
  verifier. Nothing in WP4 can detect it.

## 2. The commit protocol

| Step | Call | Writes | In-memory state |
|---|---|---|---|
| 0 | `WriterLease.acquire()` then `CommitLog.init()` | lease claim; `commit_void` + `aborted` rows for recovered commits; `epoch_fence` | `lastAllocatedSeq` = highest seq in the log |
| 1 | `beginCommit({kind, nodeScope, blockHashHex?, dependsOn?})` | `commit_log` `intent` | seq registered as non-terminal **before** the insert is sent |
| 2 | data inserts in plan §3.1 order (`output`/`input`, `transaction`, `block`/`block_transaction`, acceptance + history, `utxo`/`utxo_by_script`) | rows with `commit_seq = seq`, tokens `seq:table:N` | — |
| 2′ | optional `markIncomplete(seq)`, later the fill rows under the same seq | `commit_log` `incomplete`; fill rows `seq:table:fN` | still non-terminal |
| 3 | `markCommitted(seq, rowCounts)` | `commit_log` `committed` with the counts sent | terminal; listeners fire |
| 4 | `VisibilityPublisher.publishWatermark()` (batched, ≥ 100 ms apart) | `visibility` rows for every node whose watermark advanced | `published` |

Abort at any point: `markAborted(seq, reason)` writes `commit_void` **first**, then `commit_log` `aborted`. From the
void row on, the gate hides the seq; only then is it terminal in memory, so the watermark can pass it.

`markCommitted` also checks the lease (`assertHeld`) and `dependsOn`: a commit whose facts reference rows written by
another commit (a block accepting a tx that an in-flight mempool commit is writing) refuses to commit until that
commit is committed (`CommitDependencyError`). This is what makes the pinned snapshot complete (§4).

### Aborted rows: hidden by the gate, not prevented (b)

Rows are on disk before the abort decision (the crash happens after the data steps), and the watermark must pass
the aborted seq or it would stall forever. So the gate must exclude aborted seqs: every view requires
`commit_seq NOT IN (SELECT commit_seq FROM commit_void)`. "Aborted commits never have visible data rows" is then a
property of the gate, not of the writer.

`commit_void` is a separate tiny table rather than `commit_log FINAL WHERE state = 'aborted'` (the WP2 views): `commit_log`
gets about two rows per commit, ~250 M a year at tip rates, and that scan would run in every query. Rows of aborted
commits are removed later by an `ALTER TABLE … DELETE WHERE commit_seq IN (…)` mutation (plan §3.1); not in WP4.

## 3. Watermark semantics (b)

**Definition.** For node n ≥ 1, `visible(n)` is the highest W such that every commit with `seq ≤ W` whose
`node_scope` contains n is terminal (`committed` or `aborted`). `visible(0)` is the same over every commit, whatever
its scope. Terminal is final: no commit leaves `committed` or `aborted` (except in the takeover race of §5).

**Computation** (`computeWatermarks`, pure). With `open` = the writer's non-terminal commits and `last` = its highest
allocated seq: `W(n) = min{seq ∈ open : n ∈ scope} − 1`, or `last` if there is none; `W(0) = min(open) − 1` or
`last`. Because a seq is registered as open before its first row is sent and seqs only grow, every seq ≤ W(n)
touching n is terminal, and W(n) never decreases. A slow commit on node A never holds back node B (checklist 5).
The randomised unit test checks `W < seq` for every open commit over 200 random states.

**Incomplete commits have a bounded lifetime, by design.**
- `incomplete` is not terminal and holds the node's watermark, as the plan intends.
- Mempool commits are never incomplete. A mempool tx whose parents are unknown is an orphan; it stays in the agent's
  memory (an orphan pool with its own expiry) and is not put into any commit until its parents are known. BCHN does
  not relay orphans, so this is rare. (WP5 implements the pool.)
- Block commits can be incomplete only during concurrent sync, when a child block's input spends an output of an
  earlier block that is still in the download pipeline. The parent is guaranteed to arrive; its outputs complete the
  child under the child's own seq (2′), then the child commits.
- A hard bound: `staleIncomplete(maxAgeMs)` lists incomplete commits older than the bound; the writer aborts them and
  re-queues the work (WP5 calls it from the publish tick; 60 s suggested).
- Deviation from the plan: the fill rows are written **under the child's seq**, not in a separate `fill_pending`
  commit. A separate commit with a higher seq would let the watermark expose the child (seq c) while its inputs sit in
  the fill commit (seq f > c) above the watermark: a torn save. Under the child's seq, the child's facts become
  visible together. The `fill_pending` kind remains available for pure bookkeeping.

**Recovery on startup** (`recoverIncomplete(epoch)`, run by `init()` under the new lease):
1. per older epoch not yet fenced, read its highest seq in `commit_log`;
2. find every commit above `visible(0)` whose final state is `intent` or `incomplete` and abort it (`commit_void`,
   then `aborted` with `abort_reason = "recovered at startup by epoch E (was …)"`); return them for re-queueing;
3. write `epoch_fence` rows for every older epoch not yet fenced (dense from 1), at the seq read in step 1.

After recovery every seq of every older epoch is terminal or fenced, so the first publish sets every watermark to
`lastSeq`. Readers see aborted rows never (void), fenced rows never (fence), and committed rows from then on.

## 4. Pinned reads: no torn reads across views (a)

Every gated view has a pinned twin. The convenience views are unchanged in use:

| Family | Example | Watermark |
|---|---|---|
| live, node-scoped | `utxo_v(node = 1)` | `max(visible_seq)` read inside the view |
| pinned, node-scoped | `utxo_at(node = 1, visible = W)` | `least(W, live visible(n))` |
| live, node-agnostic | `output_v` | `visible(0)` or the committed tail |
| pinned, node-agnostic | `output_at(visible0 = V0, tail = [...])` | `least(V0, live visible(0))`, or a seq in `tail` that is committed |

A reader calls `readSnapshot(client, n)` once per request and passes `nodeViewParams(s)` / `agnosticViewParams(s)`
to every view of that request. The snapshot reads `visible(n)` **first**, then `visible(0)` and the committed tail
in one statement. Any commit ≤ `visible(n)`, and every commit it depends on (`dependsOn`), was committed before
`visible(n)` was read, so it is below the later `visible(0)` or in the later tail: the node-agnostic rows of every
visible node-n fact are in the same snapshot. The clamps (`least(...)`, `tail ∩ committed`) mean a forged or stale
parameter can only hide rows, never expose an unresolved commit. The snapshot is also the natural cache key for the
plan's §2.4 watermark-keyed response cache.

**Index use is unchanged** (`[e2e] pinned views keep primary-key and projection use`, 100k rows):
`utxo_at(node, visible) WHERE token_category = …` reads 1/13 granules on the primary key
`(node_internal_id, token_category)` and passes `max_rows_to_read = 20000`; `output_at(…) WHERE locking_bytecode_prefix = …`
reads projection `p_script`. In `EXPLAIN` the watermark and the fence array fold to constants in `PREWHERE`
(`commit_seq <= [1099511627778][bitShiftRight(commit_seq, 40)]`).

## 5. Lease and epoch fencing (d)

**Claim protocol** (`WriterLease`, `writer_lease` keyed `(lease_name, epoch, agent_id)`; all times are the server's
`now64`, so agent clocks do not order claims):
1. Read all claims. The holder is the claim with the highest epoch, ties broken by earliest `claimed_at`, then
   `agent_id` (`leaseHolder`, pure). If the holder is another agent and unexpired: `LeaseHeldError`.
2. Insert a claim for `epoch = highest + 1`.
3. Wait `settleMs` (default 1 s), re-read. If the holder is not this claim: `LeaseLostError`, before anything is written.
4. Hold: heartbeat every ttl/3 (`renew` re-checks the holder, then writes a heartbeat). `assertHeld()` fails once a
   local monotonic deadline passes: `ttl − safetyMargin`, measured from **before** the server stamped the
   claim/heartbeat, so the holder stops before any contender can see the claim as expired.

**Fencing.** The new holder's `init()` fences every older epoch at the highest seq it read for it. The gate hides
`commit_seq > max_valid_seq` of a fenced epoch. A stale writer that ignores its lease entirely can only write seqs
in its own epoch's range (or reuse its own committed seqs, which tokens drop), so everything it writes after the
fence read is invisible, even if it also writes `committed` rows and a `visibility` row (tested:
`[e2e] WriterLease: a stale holder is taken over after expiry and fenced`). `CommitLog.beginCommit`,
`markCommitted` and `IdAllocator` reservations call `assertHeld`, so a well-behaved stale writer stops by itself.

**Residual races** (all need a pause longer than the safety margin, or claim inserts slower than `settleMs`):
- *Stale commit between fence read and fence write.* A stale writer commits seq k and publishes a watermark ≥ k
  after the new holder read the epoch's max but before it wrote the fence. Readers may see commit k (whole: it is
  a complete commit), then it disappears when the fence lands. Each commit stays all-or-none for every node; what
  is lost is monotonic visibility for that one commit. Recovery's `aborted` row (rank 4) also overrides a racing
  `committed` row (rank 3), with the same effect.
- *Id ranges.* A stale writer reserving an id range after the takeover read can overlap the new writer's ranges.
  Its rows are fenced; the WP5 hash-to-id lookup must ignore fenced and void rows (`gateSql.validCommit`) so it
  never reuses such an id.
- *Two claimants on one epoch.* If a claimant pauses between its read and its claim for longer than `settleMs`,
  two agents can both see themselves as holder for a moment. The later one's `claimed_at` is later, so it loses
  on its own re-read in every interleaving except a claim insert that takes longer than `settleMs`. If both proceed,
  `init()`'s `lastSeq` check refuses the second once the first has written a commit.
- *Clock rate.* The deadline uses the local monotonic clock against the server's expiry; a drift larger than the
  safety margin over one ttl (default 5 s over 30 s) breaks the self-stop, not the fence.

## 6. Crash consistency, step by step

A reader of node n, using live or pinned views, if the agent dies between two steps of one save of n. "After
recovery" is after the next writer's `init()` and first publish. Tested by `[e2e] crash injection` (crash after
each of steps 0–5; node facts = `tx_acceptance`, `utxo`, `utxo_by_script`; node-agnostic = `output`).

| Dies between | n's facts, before recovery | Node-agnostic rows | After recovery | Why |
|---|---|---|---|---|
| (nothing) → 1 intent | none | none | none | nothing written |
| 1 intent → 2 first data insert | none | none | none (aborted) | seq is open, so W(n) < seq; recovery voids it |
| between two data inserts (any pair in step 2) | none | none | none (aborted) | as above; partial rows exist on disk, all voided |
| last data insert → 3 committed | none | none | none (aborted) | as above |
| 2′ incomplete → fill rows | none | none | none (aborted) | `incomplete` is open |
| between fill inserts → committed | none | none | none (aborted) | as above |
| 3 committed → 4 publish | none | **all** (committed tail) | all | W(n) < seq until a publish; recovery publishes `lastSeq` |
| during 4 (one insert for all nodes) | all or none (one part) | all | all | one `INSERT … SELECT` row set is one part |
| after 4 | all | all | all | — |
| `markAborted`: void row → `aborted` row | none | none | none | void hides it; recovery re-aborts (same token) |
| recovery: aborts → fence row | none | none | none | next start redoes recovery (tokens make it idempotent) |
| lease: claim → confirm | — | — | — | nothing else written before confirmation |

Node-agnostic rows of a committed but unpublished commit are visible through the committed tail; they carry no
acceptance fact for any node (checklist 7), so n's view is still "none" for that save.

## 7. Internal ids (§3.3)

`IdAllocator` reserves `[start, end)` in `id_reservation` (token `epoch:id_reservation:kind:start`), awaits the
insert, then hands out ids from it. Per-kind promise chains serialise reservations, so concurrent calls get disjoint,
increasing ids. A request larger than the range size reserves one range of the request's size. On start it resumes
at `max(range_end)`, so a crash skips at most the unused rest of one range per kind. Default range 100,000; `node`
uses 16 (UInt32 ids, few nodes). Ids start at 1.

## 8. Hand-offs to WP5 (e) and later

- **`utxo.created_height` is not exact.** A mempool output keeps `created_height = 0` after confirmation (no UTXO
  rows are written on confirmation). Take the height from `tx_acceptance_v` or drop the column from answers. Not solved here.
- **`input` lacks `fungible_token_amount`.** The plan's denormalised spent-output list omits it, so `inputs.outpoint.fungible_token_amount`
  filters still need a join. Not solved here.
- The orphan pool for mempool txs with unknown parents (§3: mempool commits are never incomplete).
- Calling `staleIncomplete` from the publish tick and re-queueing what it aborts.
- `dependsOn` for block commits that accept txs written by in-flight mempool commits.
- The hash-to-id lookup must apply `gateSql.validCommit` (ignore void and fenced rows).
- Fill rows of an incomplete commit use the child's seq and tokens `seq:table:fN`.
- Not done in WP4: `SQL SECURITY DEFINER` on the views and `SELECT`-only grants for the API user (README item 7).
  The definer user is deployment-specific; Phase 2 sets it with the API role.

## 9. Deviations from plan §3

1. `commit_seq` carries the writer epoch in its high bits (the plan allocates seqs in memory without an epoch).
2. Aborted seqs live in `commit_void`; the gate reads it instead of `commit_log FINAL`.
3. Stale-writer fencing (`epoch_fence`) is new; the plan's lease was advisory only.
4. Fill rows of an incomplete commit use the child's seq, not a separate `fill_pending` commit (§3 above).
5. `markCommitted` takes `dependsOn` (plan: implicit in the in-flight map).
6. Pinned `*_at` views and `readSnapshot` (the plan's views read the watermark per view).
7. The plan's "row counts are checked against what was sent": WP4 stores the sent counts in `row_counts`; counting
   rows per seq server-side is left to the checker (it is a scan per table).

## 10. Test results (local ClickHouse 26.8.22.13, 2026-10-09)

`yarn build` clean; `yarn test:unit` 69 passed, 1 todo (23 of them WP4 unit tests); eslint, prettier and cspell
clean for the WP4 files. With `CHAINGRAPH_E2E_CLICKHOUSE_URL=http://localhost:18123`, the five WP4 spec files run 35
tests (23 unit + 12 `[e2e]`), all passing in four consecutive runs; each `[e2e]` test creates and drops its own
`ch1_wp4_*` database.

| `[e2e]` test | Checks |
|---|---|
| client: ping, bound parameters, 64-bit integers | a hostile string round-trips as data; UInt64 max as a string |
| client: insertRowBinary deduplicated by token | retry dropped; same token + other data dropped; new token inserts |
| commit log: commit, abort, recover, list | recovery aborts intent and incomplete, voids them, fences densely; refuses a used epoch |
| watermark advances only when earlier commits for the node are terminal | per-node independence; node 0 waits for all |
| aborted rows stay invisible after the watermark passes them | through `tx_acceptance_v`, `utxo_v`, `utxo_by_script_v`, `output_v` |
| torn reads: a pinned watermark shows a two-table save all-or-none | nothing between data and committed; nothing before publish; all after; old pin stays empty |
| crash injection | steps 0–5 × before/after recovery: never torn |
| pinned views keep PK and projection use | `utxo_at` 1/13 granules; `output_at` reads `p_script` |
| id reservations: durable ranges, resume after restart | rows present; retry idempotent; restart resumes at 2001 |
| lease: acquire, refuse, renew, release | `LeaseHeldError`; release lets the next writer in at once |
| lease: stale holder taken over and fenced | stale writer's full commit + watermark invisible; its earlier commit visible |
| lease: three simultaneous claimants | exactly one wins |
