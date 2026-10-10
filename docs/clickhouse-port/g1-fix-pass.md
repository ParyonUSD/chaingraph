# G1 fix pass: void-after-visible and unresolved pending spends

Date 2026-10-10. Input: the G1 verdict (`paryon_kubernetes-consolidation/docs/chaingraph/decisions/2026-10-10_clickhouse-phase1-g1.md`,
must-fix 1 and 2), [ch1-run-log.md](ch1-run-log.md), and the c1 evidence in
`chaingraph-performance/2026-10-10-ch-replay-nvme/` (local only: `logs/agent-c1.final.log`, `results/c1-stall/`,
`results/commits-c1.tsv`, `results/probe-c1.jsonl`, `results/parity/windows-c1/`).

Short version: both bugs are one chain of events. A transient ClickHouse error failed a block batch; nothing
stopped later batches of the same node from committing on top of the missing blocks; their children waited the
full pending-spend timeout for parents that would never be stored, then were committed with stand-in inputs. No
commit was voided after it became visible: the watermark passed an *aborted* commit, which WP4 allows, and that
exposed later commits above a hole. Root causes confirmed from the log and reproduced locally; fixed; 9 new
lab-condition specs red before, green after.

## 1. What the c1 evidence shows

Timeline (agent log, `commits-c1.tsv`, probe):

| Time (UTC) | Event | Evidence |
|---|---|---|
| 08:23:41 | window opens (831,863 → 834,000), every block downloaded within minutes | 2,138 `Downloaded block` lines; buffer 1,539 blocks at 08:34 |
| 08:24–08:33 | commits ...554–...567 (831,863–832,461) in 0.2–6.7 s | `commits-c1.tsv` |
| 08:34:23.659 | the next batch (832,462–832,517, 56 blocks) fails: `socket hang up` (ECONNRESET) on a read, **before `beginCommit`** (no seq; no void row) | 56 level-60 `socket hang up` lines = one per block of the batch; these 56 heights have neither `Saved` nor `Parked` lines; no seq between ...567 and ...568 |
| 08:34:23.675 | the agent logs fatal and starts its shutdown; the shutdown **drains the block buffer**, i.e. keeps saving every buffered block above the failed ones | `Attempting shutdown...`, `Already shutting down.` ×120 (one per further fatal) |
| 08:34:37 → 12:28 | every later batch (...568–...592) spends outputs of 832,462–832,517, which are never stored: each parks, waits the full 600 s pending-spend timeout, is filled with stand-in inputs (value 0, no UTXO −1) and **commits and becomes visible**; the next batch waits for its predecessor's fill rows (tip mode), so the cadence is 600 or 1,200 s per commit | `commits-c1.tsv` from ...568: 603 s, 1,186 s, ...; 1,419 `Parked block` lines from 832,518 on |
| 11:58:09 | batch ...588 (833,618–833,681) fails with server error 209 `Timeout exceeded while reading from socket (30000 ms)` and is aborted (void row). It was **open**: never committed, never visible | 64 level-60 209 lines = 64 blocks; no `Saved`/`Parked` for 833,618–833,681; probe at 11:58:16: `prev ...586 → v ...588`, committed commits listed in that step: only ...587 |
| 11:58–12:38 | ...589–...592 commit on top of the second hole; spends of ...588's (void) outputs are not resolvable (the gate excludes void rows) and time out | `pending-spend-vs-output.tsv`: 170 (...592) and 238 (...593) outpoints "found" in `output` with `commit_seq` ...588 |
| 12:28–12:42 | ...593 (833,938–834,000) waits ~600 s for ...592's fill, parks at 12:38:28, is stopped at 12:42 before its own 600 s ran out | `Parked block – height: 833938` at 12:38:28 |

Consequences:

- **Bug 1 (missing rows in visible range).** The four failing parity windows are exactly the two holes: 832,363–832,462
  and 832,463–832,562 miss 1 + 55 blocks = 832,462–832,517 (the 08:34 batch); 833,563–833,662 and 833,663–833,762
  miss 45 + 19 = 64 blocks = 833,618–833,681 (...588). The `block` row deficit (80 = 56 + 24 blocks of ...588 whose
  block row was not yet written) matches. "Voided after visible" is not what happened: ...588 was voided while
  open, and WP4's watermark then legitimately passed it (aborted = terminal, rows hidden by `commit_void`), which
  made the later commits ...589–...592 visible above the hole. Same for the 08:34 batch, which never had a seq.
- **Bug 2 (pending spends not resolved although "the parent output is stored").** The 238 + 170 "stored" parents are
  rows of the **void** commit ...588, invisible to `validCommitSql` by design. The 67 + 21 outpoints absent from
  `output` are outputs of 832,462–832,517: the only heights ≤ 834,000 with no rows at all (the batch failed before
  writing anything; ...588's `output` insert, one per batch, did land). Not above 834,000, not never fetched. The
  "not released by the 600 s timeout" for ...593 is the stop at 12:42, 3.5 min after it parked.
- **Silent corruption** besides the holes: every child of a hole was committed with stand-in inputs (value 0) and
  no UTXO −1 rows. Re-saving the missing parents later would not repair those input rows.

Hypotheses from the task, checked:

| Hypothesis | Verdict |
|---|---|
| Lease recovery voids committed-but-visible commits | No. Recovery aborts only `intent`/`incomplete` (FINAL, `state_rank`) above visible(0); no lease event in c1 (all commits epoch 2). Now also asserted against per-node watermarks. |
| The `Already shutting down` burst was a lease-loss path | No. It is the agent's fatal handler: one line per failed block save after the first (56 at 08:34, 64 at 11:58). |
| A socket timeout aborts a batch after partial publication | Partly: 209 aborted ...588, but it was never published. No code path voids a committed seq in c1. |
| Recent-output cache evicts a parent before the child looks | Real code race (below), but not the c1 trigger. |
| Timeout fallback does not look up committed outputs | True: the wait listened only to registrations. Fixed. |

## 2. Root causes in the code

1. **No retry for reads; inserts retried only transport errors.** `ClickHouseClient.query` had no retry, so one
   ECONNRESET failed the whole 56-block batch; 209 (`SOCKET_TIMEOUT`, server-side receive timeout) is a
   `ClickHouseError` and was never retried even for deduplicated inserts.
2. **A failed batch did not stop later batches of its node.** Block batches wait only for predecessors' *rows*
   (WP6b), never for their *commit*; the registry poisons later operations on failure but block saves never checked
   it; a failure before `beginCommit` holds no watermark at all. So later batches committed and the watermark
   passed the aborted (or seq-less) batch: a hole.
3. **The store kept working after the failure.** Abandoning in-flight work was wired only to SIGINT/SIGTERM; the
   agent's fatal-path shutdown drained the buffer through the store, and a rejected save stayed in the buffer
   (`BlockBuffer.drain` waits for an empty buffer), so the drain never ended (four hours "shutting down").
4. **Pending waits listened only to registrations.** `waitForPending` never re-read the store. A parent that
   registers, commits and is evicted from the recent cache (500k outputs default; a few big blocks) between the
   child's lookup and its subscription is never seen: full timeout, then a stand-in. Possible across node lanes in
   tip mode and within a lane in bulk mode. Not the c1 trigger.
5. **Found by the new lab spec: batches out of chain order deadlock.** Blocks joined the open batch regardless of
   order. After a lost lease the cut-off calls re-run in the order they failed, so batches mixed blocks that spend
   each other in both directions: a cycle of commit dependencies (state dump: every operation waiting for its
   neighbours on both sides). Pre-existing since WP6b.

`markAborted` also had no guard: called for a committed seq (any error after `markCommitted` reaching
`BlockCommitter.save`'s catch) it would have written a void row for a visible commit. No such call happened in
c1, but nothing prevented it.

## 3. The fix

| # | Change | Where |
|---|---|---|
| A | Reads retry transport errors and transient server codes (3, 32, 202, 209, 210, 252), 3 times with backoff; deduplicated inserts retry the same set | `client.ts` (`isRetryable`, `query`) |
| B | **Commit order per node:** a block commit is marked committed only after every earlier operation of its nodes has settled, and throws `DependencyFailedError` (abort while open, above every watermark) if one failed. An earlier operation that waits transitively for this one (child-before-parent) is not waited for; data dependencies are recorded as found (`noteWaitsOn`) and the check re-runs on every wait-for-graph change, so the graph stays acyclic | `node-state.ts` (`awaitCommitOrder`), `block-commit.ts` step 6 |
| C | **A failed block batch abandons the epoch's in-flight work** (as shutdown): every uncommitted operation aborts, new saves are refused (resolve as handled), parked children stop waiting. A parked batch abandoned this way rejects its `committed` (it used to resolve, logging "Saved") | `clickhouse-store.ts` (`onBatchFailure`), `block-commit.ts` |
| D | **A batch is one contiguous chain segment** (a block joins only if it extends the batch's last block) | `clickhouse-store.ts` (`canAppend`) |
| E | **Pending wait re-checks the store**: after subscribing, one lookup of the still-missing outpoints if any commit was committed since the batch's last lookup | `block-commit.ts` (`waitForPending`) |
| F | **Void guard:** `markAborted` throws `VisibilityInvariantError` for a committed seq, a seq whose `committed` row was sent (may have landed; it stays open until recovery reads its state), or a seq at or below a published watermark of node 0 or its scope; recovery checks against the per-node watermarks in `visibility`. The publisher asserts before every publish that no watermark reaches an open commit of its scope (`WatermarkInvariantError`) | `commit-log.ts`, `visibility.ts` |
| G | **Diagnostics:** `onDiagnostic` events, logged by `createStore` as one structured line each under `clickhouseDiagnostic`: `commit_void` (seq, reason, prior state, scope, watermarks checked), `void_refused` (fatal), `abort_failed`, `block_batch_failed` (seq or `none`, heights, whether it abandoned), `pending_spend_timeout` (seq, unresolved count, sample, waited ms) | `clickhouse-store.ts`, `src/store/index.ts` |
| H | Agent: a failed save is removed from the buffer so the shutdown can finish; a parked block whose commit fails now stops the agent (it only logged before); `removeBlock` ignores unknown blocks | `agent.ts`, `components/block-buffer.ts` |

With C and B together a failure leaves each node at a chain prefix: B stops anything later that is still open, C
stops everything not yet registered. The agent exits; the next start restores from committed `node_block` and
downloads the rest. All of this is shared by UTXO on and off.

Interaction with the 23k tx/s target: B adds a wait only at the commit step, without a slot and after the lane is
yielded; the gate numbers below show no throughput change.

## 4. Tests

New `src/store/clickhouse/g1-fix.spec.ts` (`[e2e]`, scratch `ch1_fix_*`). Each asserts: no void at or below a
watermark published earlier (a poller watches `visibility` and `commit_void`, as a reader would); per-node parity
with an independent recomputation (the node sees a prefix of its chain, exactly its txs and UTXOs, every input
carries its spent output's value); no wait for the pending-spend timeout.

| Test | Before (old code + test hooks) | After |
|---|---|---|
| failed batch at `output` (mid-commit), utxo on | **red**: node sees 36 of 40 blocks with a hole; children stored with stand-ins | green; restart re-saves exactly |
| same, utxo off | **red** (same) | green |
| failed batch at `begin` (before the seq: the 08:34 case), utxo on / off | **red** ×2 (hole) | green ×2 |
| commit order: the second batch fails after `rows-written`, later independent batches | **red**: ...778 aborted, ...779–...783 committed (hole) | green: all later ones aborted |
| socket reset on a read and 209 on an insert mid-batch | **red**: 13 saves rejected | green, nothing aborted |
| parent committed by another node and evicted (cache capacity 1) while the child is parked | **red**: child waited the full 10 s, input value 0 | green: committed 25 ms after release, value 4,321 |
| lab, utxo on: 160 blocks, 8 per batch, cap 16, chunks of 24 in reverse order, cache 4, 3 resets + 3 × 209, lease lost mid-batch | **red**: deadlock (300 s test timeout) | green, 4.4 s |
| lab, utxo off | **red** (same) | green, 4.4 s |

Unit: void guard (committed, may-be-committed, watermark of scope and node 0, other node's watermark allowed, no
void row written when refused), publisher assertion, `isRetryable`, `awaitCommitOrder` (order, failure, cycle
exclusion incl. a late edge and a transitive one), client read retry against ClickHouse.

Existing spec adjusted: none. `unknown spent outputs are looked up once per save` stays at 10 lookups because the
re-check (E) runs only if a commit landed since the lookup.

## 5. Results (local, this branch)

| Suite | Result |
|---|---|
| ClickHouse store specs `build/store/clickhouse/*.spec.js` | 134/134 (incl. the 9 G1 specs and the new unit tests) |
| e2e ClickHouse, UTXO on | 45/45 (47 `[postgres]` skipped) |
| e2e ClickHouse, UTXO off | 45/45 |
| e2e ClickHouse, `MAX_IN_FLIGHT_SAVES=16` | 45/45 |
| e2e ClickHouse, UTXO off + cap 16 | 45/45 |
| e2e Postgres (ch1-pg) | 92/92 |
| `yarn build`, `yarn test:unit` | ok, 107 passed + 1 todo |
| eslint / prettier / cspell on changed files | clean |

Ingestion gate, `--store clickhouse`, same host as [wp6b-write-path.md](wp6b-write-path.md) §8 (medians of 3, Postgres
14 on ch1-pg for the harness):

| Scenario | WP6b | G1 fix | Limit |
|---|---|---|---|
| max-block wall (100,001 tx) | 4.43 s | **4.44 s** (4.65 / 4.43 / 4.44), 22.5k tx/s | ≤ Postgres 7.58 s: pass |
| re-org converge | 3.54 s | **3.82 s** (3.82 / 4.57 / 3.71) | ≤ 6 s: pass |
| catch-up `--quick` (1,000 blocks) | 645.6 blocks/s | **644.7 blocks/s** (644.7 / 644.7 / 641.4), ~700 parts | ≥ 300: pass |

Re-org is within run-to-run spread (one 4.57 s outlier); no regression on the other two.

## 6. For the re-run

- The c1 failures were triggered by transport/server hiccups that now retry. The 209 means the server waited 30 s
  for a request body: the agent's event loop was blocked about 30 s (also visible as the commit of ...587 at
  11:57:36 reported to the agent at 11:58:09, and the 33.8 s HOL p95). Find that stall (GC with a 12 GB heap, or a
  large synchronous fill/encode) before the re-run; the retry hides it but HOL will not.
- Grep the agent log for `clickhouseDiagnostic`: any `void_refused` is a bug; `block_batch_failed` means the run
  stopped by design (restart from the same database is correct); `pending_spend_timeout` with UTXO off and mainnet
  blocks in order should not appear at all.
- Parity windows should now be exact over the whole visible range or the run should have stopped; no stand-in
  inputs (value 0 for a non-coinbase input) should exist: add that check to the parity kit.
- Not changed here: must-fix 3 (HOL), 5 (spent-output lookup granularity), 6 (concurrency ratio); lab-kit fixes.
- Behaviour change to note: after a failed block save the ClickHouse store refuses further writes until restart
  (the agent shuts down on that failure anyway, now also for a failed parked commit).
