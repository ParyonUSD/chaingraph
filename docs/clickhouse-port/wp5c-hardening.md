# WP5c hardening: in-flight cap, config, UTXO compaction design, known differences

Builds on WP4 (`wp4-commit-and-visibility.md`), WP5a-core (`wp5a-core.md`) and WP5a-mempool
(`wp5a-mempool.md`). Lab inputs: `paryon_kubernetes/docs/chaingraph/plans/clickhouse-phase1-lab-runs.md`
precondition 0.3 (in-flight cap), §1.0 (dummy Postgres string), §1.3 (UTXO growth), open items 2, 7, 9.

| Item | Status | Code |
|---|---|---|
| §1 In-flight cap `CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES` | built, default 0 = unbounded (unchanged behaviour) | `node-state.ts` (`InFlightLimiter`, `SaveSlot`, `StoreOperation.whileWaiting`), `clickhouse-store.ts`, `block-commit.ts` |
| §2 Postgres connection string optional in ClickHouse mode | built | `src/config.ts`, `src/store/index.ts`, `src/index.ts` (startup log line) |
| §3 UTXO growth and compaction | design only (not built) | — (summary in `src/store/clickhouse/ddl/README.md`) |
| §4 `forgetNodeValidation` caveat, stand-in spent outputs | assessed: neither bites the lab replay; one cap-specific variant fixed | `block-commit.ts` (timeout re-arm) |

**Behaviour with the cap unset (0):** no limiter object exists, no operation gets a slot, every
`whileWaiting` returns the wait unchanged, the pending-spend timeout fires once as before, and
`poolStats()` returns exactly the WP5a values (live operations, `max` 0). Store behaviour, row output and
timings of a run that does not set the variable are unchanged, so WP6 numbers taken on the earlier build stay
valid. The only differences with the cap unset are config (Postgres string not required in ClickHouse mode)
and one startup log line.

## 1. In-flight cap

**What it bounds.** At most `N = CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES` `saveBlock` /
`acceptBlocksViaHeaders` calls *work* at once: compute ids, read the store, insert rows, commit. Re-orgs,
mempool saves and mode switches take no slot (rare or tiny). `saveBlock` does not resolve while its call is
queued for a slot, so the agent's block buffer sees back-pressure the way it does from a full Postgres pool
(the lab's "16 in flight", `CHAINGRAPH_POSTGRES_MAX_CONNECTIONS=16`).

**Mechanism.**
- `InFlightLimiter(max)`: a counting semaphore. A release hands the slot straight to the head of the queue,
  so the queue is empty whenever fewer than `max` slots are held.
- Ticket = the operation's id, which the operation registry assigns synchronously in store call order. The
  queue is ordered by ticket: new calls are served FIFO (they always carry the largest ticket); a call that
  gave its slot up for a wait and wants it back is served before every newer call (it is older), so older
  work finishes first and nothing starves (a waiter has only finitely many older tickets ahead of it).
- The slot is taken **after** registration in the operation registry, so a queued call is already a
  predecessor of later calls for its nodes (call order is unchanged), and **before** the save registers its
  transaction ids and outputs (so every pinned id/output belongs to a call that is running, see the argument).
- `StoreOperation.whileWaiting(p)` releases the slot, awaits `p`, and takes the slot again by the same ticket.
  It wraps every wait on another operation in the capped paths:

  | Wait | Where |
  |---|---|
  | predecessors' rows (`waitForPredecessorRows`) | block save (tip mode, re-save, mempool nodes), header acceptance |
  | another save of the same block | `BlockCommitter.save` |
  | outputs of a parent not yet registered (`waitForPending`, child-before-parent) | block save |
  | commits this commit depends on (`awaitDependencies`) | block save, header acceptance |
  | earlier operations of a stale node mempool (`ensureFresh` via `freshen`) | block save, header acceptance |

  Waits on an empty set (no predecessors, no dependencies) are not wrapped, so an uncontended save keeps its
  slot from start to commit.
- Shutdown: a queued acquire races the store's `AbandonSignal`; an abandoned queued `saveBlock` resolves as
  handled with nothing written (as every abandoned save), a queued header acceptance rejects.

**Deadlock argument.**
1. *A slot holder never waits on another operation.* Every wait on another operation in the capped paths is
   wrapped (table above). The remaining awaits of a holder are its own ClickHouse I/O (finite: requests have
   a timeout) and the id promises of transactions/outputs pinned by other operations. An id promise is
   resolved by its owner right after the owner's own lookups and id allocation (I/O only), with no wait on
   any operation in between; the owner pins only while running, i.e. a capped owner pins while holding its
   slot and keeps it until the promise is resolved, and an uncapped owner (mempool) needs no slot. So every
   holder releases its slot in finite time, by finishing or by entering a wrapped wait.
2. *The cap adds no edge to the wait-for graph.* An operation queued for a slot waits only on slot holders,
   which by 1 do not wait on anything. The waits between operations are exactly WP5a's (predecessor rows,
   pending spends, dependencies), whose graph has no cycle (wp5a-core §4: outputs are registered at save
   start, a parent never waits for its child's commit, only the child waits for the parent).
3. *Every queued operation is eventually served.* Slots are released in finite time (1) and the queue is
   ordered by ticket; a queued operation has finitely many older tickets ahead of it, and new arrivals queue
   behind it.

Child-before-parent under the cap, the case the lab worried about (cap 2, blocks N+2, N+1, N called in that
order, each spending the previous one's outputs): N+2 and N+1 take the slots, N queues. N+1 waits for N+2's
rows (slot released, N gets it and registers its outputs). N+2 writes its rows and waits for N+1's commit
(released). N+1 takes a slot back, resolves N's outputs from the registry, writes, waits for N's commit
(released). N waits for its predecessors' rows (released), then writes and commits; N+1, then N+2 commit.
Holding the slot through those waits instead parks N+1 in `waitForPending` with both slots held and N queued
forever: the run then ends only at `pendingSpendTimeoutMs`, with N+1's inputs stored as stand-ins (verified:
the e2e test below reports `stuck` with the release removed).

**The pending-spend timeout under the cap.** A parent registers its outputs only once it holds a slot, so a
long queue could delay a parent past the child's `pendingSpendTimeoutMs` (60 s) and turn its spends into
stand-ins (§4). The child's timeout is therefore re-armed while any call is queued for a slot; it fires only
when the queue is empty, as in the unbounded store (where the queue is always empty, so behaviour is
unchanged).

**Heartbeat (`poolStats`).** With the cap: `clients.active` = slots held, `clients.max` = the cap,
`waitingRequests` = calls queued for a slot, `clients.total` = all live store operations (including parked
children and calls in wrapped waits). Without the cap: as WP5a (`active = total` = live operations, `max` 0,
`waitingRequests` 0). The agent's heartbeat logs these under its pool stats.

**What the cap does not bound.** Parked children (incomplete commits waiting for a parent) and calls inside a
wrapped wait hold no slot; their number is bounded by the block buffer, as before. Open commits can therefore
exceed the cap; working calls (the ones that issue ClickHouse inserts) cannot.

**Tests.**
- Unit (`node-state.spec.ts`): cap held and FIFO for new tickets with an older ticket served first; abandon
  removes a waiter and leaks no slot; cap 1, a holder waiting on a queued operation through `whileWaiting`
  completes (it never resolves if the slot is held); no slot = unchanged waits.
- `[e2e] in-flight cap 2, blocks N+2, N+1, N saved concurrently in that order complete; final state exact`:
  4-block chain (N+2 spends N+1, N+1 spends N), two nodes, every commit step slowed by 10 ms, a 1 ms
  `poolStats` sampler: completes within 30 s (1.5 s measured), never more than 2 active, a call was queued,
  per-node blocks/txs/`utxo_at`/`utxo_by_script_at` equal the definitional sets, inputs carry the resolved
  spent values (no stand-in), 4 committed block commits and none aborted, `pending_spend` nets to 0, sums 0/1.
  Negative control: with `idle` made a no-op it reports `stuck`.
- `[e2e] in-flight cap 1, a parked child outlives its pending timeout while its parent is queued for a slot:
  no stand-in`: child parked with a 300 ms timeout, an unrelated slow save (node 3) holds the only slot for
  > 1 s, the parent queued: resolved spends and exact UTXO sets. Negative control: without the re-arm, b's
  spends are stand-ins and the UTXO sets are wrong.

## 2. Config: no Postgres connection string in ClickHouse mode

`CHAINGRAPH_POSTGRES_CONNECTION_STRING` moved from the always-required list to a Postgres-only list:
- `CHAINGRAPH_STORE=postgres` (default): required, same check and message as before
  (`ERROR: missing variable: CHAINGRAPH_POSTGRES_CONNECTION_STRING` / `Missing expected environment variable.`).
- `CHAINGRAPH_STORE=clickhouse`: not required and ignored; `postgresConnectionString` is `undefined`. The
  Postgres pool object is still constructed at import (`postgres-store.ts`, unchanged) but never used, so
  nothing connects.

The image's `defaults.env` still carries a localhost string, so in practice the variable was never missing;
the point is that a ClickHouse manifest no longer needs (or should carry) a dummy. Checked by loading
`build/config.js` from a copy with the line removed from `defaults.env`: ClickHouse mode loads
(`postgresConnectionString` undefined, cap parsed), Postgres mode fails with the message above, Postgres
with the variable set loads, `CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES=-1` fails with a clear message.

`CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES`: optional, integer ≥ 0, default 0 (unbounded); validated in
`config.ts` like the other numeric options, passed through `storeConfigFromEnvironment` →
`createClickHouseStore({ maxInFlightSaves })`. The agent logs `Store: clickhouse (in-flight cap: 16).` (or
`unbounded`) at startup. `CHAINGRAPH_POSTGRES_MAX_CONNECTIONS` is still parsed in both modes; in ClickHouse mode
it only feeds the automatic block-buffer size (`CHAINGRAPH_BLOCK_BUFFER_TARGET_SIZE_MB` unset), so set the
buffer explicitly (the lab does: 512).

**Pod environment, for `image.md` "Pod environment (agent)"** (replace the Postgres row, add the cap row):

| Var | Value |
| --- | --- |
| `CHAINGRAPH_STORE` | `clickhouse` |
| `CHAINGRAPH_CLICKHOUSE_URL` | HTTP(S) endpoint, e.g. `http://clickhouse.<ns>.svc.cluster.local:8123` or Cloud `https://<host>:8443` (from a Secret if it embeds credentials) |
| `CHAINGRAPH_CLICKHOUSE_DATABASE` | default `cg` |
| `CHAINGRAPH_CLICKHOUSE_USER` / `_PASSWORD` | from a Secret; default `default` / empty |
| `CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES` | in-flight cap; default `0` = unbounded. Lab: `16` (the Postgres pool size it is compared with) |
| `CHAINGRAPH_CLICKHOUSE_PENDING_SPEND_TIMEOUT_MS` | optional; default `60000`. Lab: `600000` (§4) |
| `CHAINGRAPH_BLOCK_BUFFER_TARGET_SIZE_MB` | set explicitly (the automatic size is derived from the Postgres connection count). Lab: `512` |
| `CHAINGRAPH_POSTGRES_CONNECTION_STRING` | **not needed** with `CHAINGRAPH_STORE=clickhouse` (ignored if set); remove the dummy |
| `CHAINGRAPH_CLICKHOUSE_DDL_DIR` | optional (DDL CLI only), override the DDL directory |
| `CHAINGRAPH_TRUSTED_NODES` etc. | as for the Postgres agent |

## 3. UTXO growth and compaction (design, not built)

### 3.1 Why `utxo` grows

`utxo` / `utxo_by_script` are `VersionedCollapsingMergeTree(sign, version)` with `version` = the row's own
`commit_seq` (WP5a-core §2, DDL item 24). A merge deletes a +1/−1 pair only if key **and** version are equal,
so a pair written by two commits (output created in commit A, spent in commit R) never collapses: both rows
stay forever and every read of that key range aggregates them (`GROUP BY … HAVING sum(sign) > 0`). Only pairs
inside one commit collapse (an output created and spent in one block, the horizon build). The rule is
required for correctness (a merge must never cancel a committed +1 against an uncommitted or aborted −1).

Cross-commit pairs: every spend of an output from an earlier block in tip mode; with mempool tracking almost
every spend (created by one mempool commit, spent by a later one; confirmation writes no UTXO rows); every
mempool tx that leaves unconfirmed (+1 at validation, −1 at expiry/replacement); re-orgs.

### 3.2 Size at mainnet scale

Inputs: 6,000 spends/block × 144 blocks/day = 864,000 spends/day; live mainnet UTXO set 59.1 M per node
(sizing 2026-09-15); 126.8 M UTXO rows = 6.5 GiB in #83, ≈ 55 bytes/row compressed.

| | per node, per table | per node (both tables) |
|---|---|---|
| dead rows/day (worst case: every spend cross-commit, mempool on) | 2 × 864 k = 1.73 M | 3.46 M |
| bytes/day at 55 B/row | ≈ 95 MB | ≈ 190 MB (≈ 69 GB/year) |
| dead rows = live rows (59.1 M) after | ≈ 34 days | |
| after 1 year | ≈ 630 M dead vs 59 M live (≈ 11×) | |

Read cost grows with the dead rows in the scanned key range: a full-set read (F1g-style count per node) scans
≈ 2× the rows after a month; a category read (`utxo_at … WHERE token_category = X`) scans live + 2 × (spends
of X since the last compaction); an active category with 1,000 spends/day adds 730 k rows/year to a range
whose live set may be a few hundred rows (#83's 4 ms warm PUSD read would become tens to hundreds of ms).
WP7 §1.3 measures the real growth per spend on the 2,138-block replay.

### 3.3 Options considered

**(a) `ALTER TABLE … DELETE` of net-zero keys** (keys whose valid rows are all ≤ S and sum to 0). A mutation
is not atomic across parts: if the −1 is removed before the +1 (they sit in different parts), readers see
sum +1 and a spent output reappears. Safe only in two phases with completion between them:
`DELETE WHERE sign = 1 AND key IN zero_set` (sums go to −1, never > 0, so `HAVING sum(sign) > 0` answers do
not change), then `DELETE WHERE sign = -1 AND key IN zero_set`. Cost: each phase rewrites (or, with
lightweight `DELETE FROM`, masks) every part that holds a matching row, which for an unpartitioned table keyed
by category/script is nearly every part, twice per run, on a table of 10⁸–10⁹ rows. It also leaves the 0/1
sum invariant broken between phases (checker must skip), and does nothing for keys whose net is ±1. Fallback
only.

**(b) WP5a-core §2's anti-pair insert** (insert +1 version R and −1 version A with `commit_seq = 0`, merges
then delete four rows). Exact at every merge state, no DDL change, but reclaim depends on merges bringing all
four rows into one part (not guaranteed for large parts) and until then the rows double. Not recommended.

**(c) Recommended: generational rewrite with partition drop**, below.

### 3.4 Recommended design: compacted generations behind the gate

**DDL (new tables or a rebuild; not an `ALTER`):**
- `utxo` and `utxo_by_script` get `generation UInt32 DEFAULT 0` (0 = written by the agent) and
  `PARTITION BY (generation, intDiv(commit_seq, 65536))`. 2¹⁶ commits per agent bucket (hours at tip rates;
  epoch boundaries, 2⁴⁰, are bucket boundaries). Collapse stays within a commit (one seq, one partition), and
  rows of different generations never merge together.
- `utxo_compaction` (ReplacingMergeTree by `generation`): one row per flip, `(generation, upto_seq,
  live_generations Array(UInt32), compacted_seq, flipped_at)`. The current state is the row with the highest
  generation; generation 0 = `(0, 0, [], 0)` (nothing compacted).

**Gate (every UTXO view and the checker):** add
`(generation = 0 AND commit_seq > {upto}) OR has({live}, generation)` next to the watermark/void/fence filters.
`*_v` read the current row inside the view; `readSnapshot` reads it once per request (with `visible(n)`) and the
`*_at` views take `{gen:UInt32}`, clamped to `least(gen, current)` like the watermark clamp, so a forged value can
only select an older complete generation. `upto` and `live` fold to constants in `PREWHERE`, like the fence
array, and prune partitions.

**A compaction run** (a separate job holding no writer lease; it never writes agent tables other than
`utxo`, `utxo_by_script`, `utxo_compaction`):
1. **Choose S.** Read every node's published `visible(n)`; wait `T_margin` (10 min ≥ the longest request and
   any watermark-keyed cache TTL); `S` = the last agent bucket boundary ≤ min over nodes of those
   watermarks (watermarks only grow, so S stays ≤ every live watermark). Skip if S ≤ current `upto`.
   Every commit ≤ S is terminal (committed, aborted or fenced), so its rows are final.
2. **Choose the representative seq** `C` = the highest committed, non-void, non-fenced seq ≤ S. Compacted
   rows carry `commit_seq = C` (≤ every reader's watermark after the flip, as the range's max seq) and
   `version = C`.
3. **Build generation g+1** with one `INSERT … SELECT` per table (dedup token `compaction:g+1:table:N`,
   retry-safe): per key, `sum(sign)` over the *valid* rows (void and fenced excluded) of
   - minor run: the current delta generations + agent rows in `(upto, S]`; `live' = [base, g+1]`;
   - major run: base + deltas + agent rows in `(upto, S]`; `live' = [g+1]`;

   emitting one row with `sign = net` where `net ≠ 0` (net is −1, 0 or +1; −1 is legitimate: a spend whose
   creator lies in the base generation, or a child whose parent commit has a higher seq; it must be kept so
   the total stays exact), with the other columns from `any(...)` (identical per key by the WP5a-core
   rule). Net-zero keys emit nothing. The new rows are invisible: the current row does not list g+1.
4. **Flip:** insert `utxo_compaction (g+1, S, live', C, now)`. One single-row insert: every snapshot read
   after it sees agent rows > S plus `live'`; every snapshot read before it keeps generation g (its rows are
   untouched). For any reader, the sum per key is the same as without compaction, because each compacted
   row is the exact net of the rows it replaces and every replaced row is ≤ S ≤ the reader's watermark.
5. **Reclaim** after another `T_margin`: `DROP PARTITION` for every agent bucket ≤ S and every compacted
   generation not in `live'`. Those rows are invisible to every snapshot of generation ≥ g+1, so the order
   and atomicity of the drops do not matter. Aborted (void) and fenced rows in those buckets are reclaimed
   too (this replaces the WP4 §2 `ALTER … DELETE` of aborted rows for the UTXO tables).

**Why the swap must go through the gate.** A plain `INSERT` of net rows followed by `DROP` of the old range
is not exact in between: while both exist, a key with an old +1 ≤ S and a later −1 > S (spent after S) sums
to +1 + 1 − 1 = +1 and a spent output reappears; dropping first instead hides unspent outputs until the
insert lands. The generation flip makes the replacement a single-row atomic change for new snapshots and no
change at all for old ones.

**Invariants and interactions.**
- *Watermarks.* S ≤ min `visible(n)`: no open, incomplete or horizon-build commit is ever compacted (a running
  `utxo_build` holds its nodes' watermarks, so S stays below it). A lagging node holds S back for all nodes;
  a per-node variant (per-node `upto` map) is possible if that matters.
- *Late writes.* Fill rows of an incomplete commit use the child's seq, which is open and > S. A stale
  (fenced) writer's rows are excluded from the net and stay invisible (fence filter and `commit_seq > upto`);
  a late insert into a dropped bucket recreates a gen-0 partition ≤ S that the gate ignores and the next run
  drops.
- *Re-orgs deeper than S* write −1 rows for outputs whose +1 is compacted: sums stay exact (nets preserved).
- *Collapsing.* Within a generation keys are unique; across generations and versus agent rows the partition
  key keeps parts apart, so a merge can never cancel a compacted row.
- *Writer.* The agent reads no `utxo` rows (its sign rules read `tx_acceptance`/`node_block`), so it is
  unaffected; it only writes `generation = 0`.
- *Snapshots.* A pinned snapshot older than `T_margin` may miss rows after a reclaim (WP4's pins are exact
  forever only because nothing is deleted). `readSnapshot` carries its read time; the API and the cache must
  not reuse a snapshot older than `T_margin`.
- *Bulk mode / backfill:* no agent UTXO rows; nothing to compact. The backfill and horizon build land in one
  bucket each.

### 3.5 Frequency and cost at mainnet scale

| Run | When | Input rows (per node, per table) | Output rows | Estimated cost |
|---|---|---|---|---|
| minor | daily (or every few hours) | ≈ 1.9 M (≈ 1 M +1, 0.86 M −1 of the day) + previous deltas | live new outputs of the period + unmatched −1 rows (spends of base outputs) | one `GROUP BY` over ~2–5 M rows: seconds of CPU, ~0.1–0.3 GB read/written; reclaims the pairs created and spent inside the window |
| major | weekly | base 59.1 M + deltas (≤ ~10 M) | ≈ 59 M | ≈ #83's UTXO model build (126.8 M rows in 3 min + 6 min merges on 96 cores); on a 16 vCPU node estimate 5–15 min and ≈ 3.2 GiB written per table per node, i.e. ≈ 13 GiB/week for 2 nodes × 2 tables |

Steady state: dead rows bounded by one major period's unmatched pairs (spends of outputs older than the base)
plus at most one minor window: roughly ≤ 7 days × 0.86 M ≈ 6 M rows per node per table (≈ 10 % of live)
instead of growing 630 M/year. Disk for the extra generation during a run: one base copy (≈ 3.2 GiB per table
per node) until the reclaim.

### 3.6 Test plan (when built)

1. **Pure model** (`utxo.spec.ts` style): random accept / spend / re-org / mempool sequences over two nodes with
   void and fenced commits mixed in; compaction at random S and random minor/major mixes; for every watermark
   W ≥ S and every key, the sum over the gated rows is identical before and after, and equals the
   definitional `unspent(n, o)`; for W < S the old generation is unchanged until the reclaim.
2. **`[e2e]` swap exactness:** a pinned-view poller (`nodeView`) for both nodes across steps 3–5, with keys that
   straddle S (created ≤ S, spent > S), net −1 keys (base +1 / delta −1; child seq < parent seq), void and
   fenced rows ≤ S: every poll equals the definitional set; old snapshots stay exact until the reclaim.
3. **Crash injection** between every step (after the insert, after the flip, between drops): readers exact
   before and after; a rerun is idempotent (tokens, flip row) and finishes the reclaim.
4. **Concurrent agent:** tip-mode saves, a re-org deeper than S and mempool churn while a run executes; final
   state equals an uncompacted twin database; `badUtxoSums` with the generation gate stays 0/1.
5. **Merges:** `OPTIMIZE … FINAL` on both tables mid-run and after: no compacted row cancels.
6. **Index use:** `utxo_at … WHERE token_category = X` and `utxo_by_script_at … WHERE prefix` keep primary-key use
   (`EXPLAIN indexes = 1`), plus partition pruning on `generation`/`commit_seq`; rows read before/after a
   minor run on the replay's top categories.
7. **Scale:** on the WP7 replay database after the run: minor and major wall time, bytes read/written,
   rows before/after, read latency of the §1.3 scopes; extrapolate with §3.5.

## 4. Known differences from WP5a-mempool, assessed for the lab replay

Replay: 831,863 → 834,000 from the backfilled checkpoint, trusted BCHN node(s) serving stored blocks.

**`forgetNodeValidation` stale memory.** Only the checker's fault injection calls it (`e2e.spec.ts`
double-validation check, `checker.spec.ts`). The lab runs no fault injection against the agent's database
(parity is a read-only compare). **Cannot bite; accepted.**

**Stand-in spent outputs** (value 0, empty bytecode, no token on the `input` row; no UTXO −1 for the node).
Two sources:
- *Mempool orphan released after `orphanGraceMs` (1 s).* Needs mempool transactions whose parent Chaingraph has
  not seen. The agent requests no mempool snapshot: it saves only transactions announced after "enabled
  mempool tracking" (after initial sync and the horizon build). A replay BCHN with `-connect=0` (or peered
  only with the other replay node) announces nothing, so there are no mempool transactions at all.
  **Cannot bite** while the trusted nodes have no outside peers. If a run ever connects them to live peers,
  every relayed tx spends outputs from beyond 834,000 and would be parked/stand-in; parity must then exclude
  mempool-only rows (they are not in any replayed block).
- *Block child whose parent's save is not called within `pendingSpendTimeoutMs` (60 s) after the child
  parks.* Saves are called in download-completion order and a parent registers its outputs at the start of
  its save, so this needs a parent block delayed by more than 60 s relative to a later block (e.g. a stalled
  transfer of 831,864, 31 MB, from a local BCHN). Unlikely, but silent if it happens: the `input` row keeps
  the stand-in and the parent's output stays unspent for the node, which parity would catch. The variant the
  cap would have introduced (the parent queued for a slot past the timeout) is fixed (§1, re-arm). No cheap
  general fix exists (resolving a block spender after its commit would need a later rewrite of `input`
  rows), so: **accepted, with two lab mitigations**: set `CHAINGRAPH_CLICKHOUSE_PENDING_SPEND_TIMEOUT_MS=600000`
  (a child holds its nodes' watermark only while its parent is genuinely late, so a long bound costs nothing
  in a normal run), and assert zero stand-ins after the run:

  ```sql
  -- inputs whose denormalised spent output differs from the output row (stand-ins and any other mismatch)
  SELECT count() FROM input AS i
  INNER JOIN output AS o
    ON i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
  WHERE i.commit_seq NOT IN (SELECT commit_seq FROM commit_void)
    AND (i.value_satoshis != o.value_satoshis OR i.locking_bytecode != o.locking_bytecode
         OR i.token_category != o.token_category);
  ```

  A stand-in whose parent never arrived does not join (no `output` row); in a replay every parent exists, so
  the join covers them.

## 5. Test results (local ClickHouse 26.8.22.13, Postgres 14 ch1-pg, 2026-10-10)

| Run | Result |
|---|---|
| `yarn build` | clean; eslint, prettier, cspell clean for every changed file |
| `yarn test:unit` | 89 passed, 1 todo (4 new limiter/slot tests) |
| ClickHouse spec suite (`CHAINGRAPH_E2E_CLICKHOUSE_URL=http://localhost:18123 npx ava 'build/store/clickhouse/*.spec.js'`) | 91 passed (incl. the 2 new `[e2e]` cap tests), 1 min 40 s; scratch databases dropped |
| e2e, ClickHouse (`CHAINGRAPH_E2E_STORE=clickhouse …`, `build/e2e/e2e.spec.js`) | 45 passed, 47 `[postgres]` skipped (58 s). A first run had 1 failure in `saves block transactions if previously announced tx is seen but not yet saved` (count read 0 right after the save log line): a pre-existing visibility-timing flake, seen identically in WP6's run on the build before this work; the cap is unset in the e2e, so no WP5c code is on that path |
| e2e, Postgres (`CHAINGRAPH_E2E_POSTGRES_HOST=localhost CHAINGRAPH_E2E_POSTGRES_PORT=15432`) | 92 passed (8 s) |

Negative controls (build output patched, not committed): slot held across waits → the cap-2 test reports
`stuck`; timeout re-arm removed → the cap-1 test finds stand-in spends and wrong UTXO sets.
