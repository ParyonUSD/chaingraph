# WP3a: `ChaingraphStore` refactor, proof of no behaviour change

WP3a introduces `src/store/types.ts` (`ChaingraphStore`), moves `src/db.ts` to
`src/store/postgres/postgres-store.ts` (behind `createPostgresStore()`), adds
`createStore()` + `CHAINGRAPH_STORE` / `CHAINGRAPH_CLICKHOUSE_*` config, and
wires the agent to `this.store.*`. This document records the proof that the
Postgres backend behaves the same as before.

- **Before:** f785bf7 (last WP1a commit). The two WP1b commits between it and
  WP3a (1a7e925, c5cf5b5) only touch `src/e2e/mempool-cleanup.spec.ts`,
  `src/store/mempool-graph*.ts` and `src/store/mempool-cleanup.vectors.ts`. The
  agent imports none of these, so the agent runtime code is the same.
- **After:** 2b02b35 (WP3a head).
- Both trees were exported with `git archive` to
  `/Users/rb/.claude/jobs/2d61cf2d/tmp/wp3a-{before,head}`, with `node_modules`
  symlinked from the worktree, and built with `tsc`. The shared worktree has
  other agents' uncommitted files, so nothing below ran in it.
- Machine: MacBook Pro (Apple M-series), Node 24, ch1-pg (postgres:14, stock
  settings) on localhost:15432.

## Commits

| Commit | Change |
| --- | --- |
| 42099bd | `feat(store)`: `ChaingraphStore` interface (`src/store/types.ts`) |
| 0bc646c | `refactor(store)`: `git mv src/db.ts src/store/postgres/postgres-store.ts` + `createPostgresStore()`, `src/db.ts` turns into a deprecated re-export shim |
| 566e591 | `feat(store)`: `createStore()` + `CHAINGRAPH_STORE` / `CHAINGRAPH_CLICKHOUSE_*` config |
| 2b02b35 | `refactor(agent)`: agent and `index.ts` use the store |

## (a) `yarn test` on the committed state

These are the `yarn test` steps (`tsc`, `eslint src`, `prettier --list-different`, `cspell`,
`nyc ava --timeout=30s`), run from the exported 2b02b35 tree. Yarn itself
couldn't run there because the export has no `.yarn/releases`.

| Step | Result |
| --- | --- |
| build, lint, prettier, cspell | all OK |
| ava (all) | **146 passed, 1 todo, 0 failed** (12 s) |
| of which `e2e.spec.ts` | **92 / 92** |
| of which `mempool-cleanup.spec.ts` | 8 / 8 |
| unit (components) / store | 21 / 25 |

## (b) Ingestion gate, before and after, on ch1-pg

```sh
node scripts/ingestion-gate/run.mjs --scenarios <max-block|reorg> \
  --pg-url postgres://chaingraph:very_insecure_postgres_password@localhost:15432
```

The two trees ran alternately. The "checkpointed" rounds issued `CHECKPOINT`
on ch1-pg before every run. All runs: max-block **PASS**, reorg **PASS\***
(the known deep-reorg check is allow-listed, as before).

| Scenario | Tree | Runs | WAL bytes (min … max) | Wall / converge s (sorted) | Median s |
| --- | --- | --- | --- | --- | --- |
| max-block, plain | before | 4 | 224,914,088 … 224,914,160 | 5.080, 5.203, 5.274, 5.313 | 5.24 |
| max-block, plain | after | 3 | 224,914,128 … 224,914,184 | 5.134, 5.149, 5.158 | 5.15 |
| max-block, checkpointed | before | 3 | 224,959,352 … 224,959,376 | 5.205, 5.625, 5.738 | 5.63 |
| max-block, checkpointed | after | 3 | 224,959,352 … 224,959,408 | 5.390, 5.450, 6.045 | 5.45 |
| reorg, plain | before | 4 | 223,319,176 … 223,534,896 | 1.352, 1.392, 1.450, 1.454 | 1.42 |
| reorg, plain | after | 3 | 223,241,920 … 223,980,976 | 1.453, 1.453, 1.498 | 1.45 |
| reorg, checkpointed | before | 8 | 223,288,056 … 223,643,016 (median ≈ 223.57 MB) | 1.391 … 1.560 | 1.449 |
| reorg, checkpointed | after | 8 | 223,061,872 … 224,167,760 (median ≈ 223.66 MB) | 1.394 … 1.735 | 1.443 |

**WAL bytes are not byte-deterministic on this setup, before or after.** The
design (§F step 4) and the WP3a brief expected an exact WAL match. Repeated
runs of the *same* tree do not reproduce exact WAL byte counts:

- **max-block** varies by about 50–100 B per run. It is stable to 0.00004% and
  the two trees overlap completely. With a checkpoint before each run, both
  trees produced exactly 224,959,352 B at least once. Starting right after a
  `CHECKPOINT` adds about 45 KB of full-page images, which is the offset
  between the plain and checkpointed rows.
- **reorg** varies by a few hundred KB per run in both trees (0.1–0.5%). The
  two agents' block-download, header-acceptance and reorg interleaving is
  timing-dependent, and so is autovacuum on the freshly loaded gate database.
  The distributions overlap. The after set had one high outlier (224.17 MB),
  but the medians differ by 0.04% and the SQL text is identical (below).
- Wall and converge times are within run-to-run noise. Medians are within ±3%
  either way, with no consistent direction. The checkpointed rounds ran while
  other agents were active on the machine.

WP1a's single runs on ch1-pg (224.9 MB max-block, 223.5 MB reorg) fall inside
the plain ranges above.

## (c) SQL text capture

This was a one-off patch, not committed, applied the same way to both exported
trees. It prepends `import '../sql-capture.mjs';` to `bin/chaingraph.js`, which
is what the e2e suite spawns. `sql-capture.mjs` wraps
`pg.Client.prototype.query` (pool queries go through it too) and appends every
statement's text to `<cwd>/sqlcap/<pid>.ndjson`. Each tree then ran
`ava build/e2e/e2e.spec.js --match='*[e2e]*'` twice (92/92 each time). All
agent processes the suite spawns were captured: 3 per run, the third being the
restart with the second trusted-node set.

Normalisation (`sql-normalise.mjs`):
- bytea literals become `'\x?'`, ISO timestamp literals become `'TS'`, and
  integer literals become `N`;
- whitespace is collapsed;
- runs of identical `VALUES` tuples are collapsed to `(…),…`;
- the result is counted as a multiset of statements.

| Comparison | Distinct normalised statements | Set difference | Statements whose count differs | Total statements |
| --- | --- | --- | --- | --- |
| before1 vs before2 | 3,363 / 3,363 | **0** | 13 | 17,207 / 17,196 |
| after1 vs after2 | 3,363 / 3,363 | **0** | 11 | 17,201 / 17,197 |
| before1 vs after1 | 3,363 / 3,363 | **0** | 14 | 17,207 / 17,201 |
| before2 vs after2 | 3,363 / 3,363 | **0** | 10 | 17,196 / 17,197 |

The set of distinct statements is **identical** before and after. The
multisets are not exactly equal, and they are not equal between two runs of the
same tree either. The count differences are confined to timing-driven
statements, and they vary the same way within each tree:

| Statement (prefix) | before1 | before2 | after1 | after2 |
| --- | --- | --- | --- | --- |
| `BEGIN;` / `COMMIT;` | 3424 | 3419 | 3419 | 3422 |
| saveBlock linked-count check `SELECT COUNT(*)::bigint AS count FROM block_transaction …` | 3415 | 3410 | 3410 | 3413 |
| expiry scan `SELECT encode(transaction.hash, 'hex') AS "hash", node.name …` (100 ms timer) | 16 | 23 | 26 | 16 |
| repair sweep `WITH directly_accepted AS …` (same timer) | 17 | 24 | 26 | 17 |
| saveBlock `WITH transactions_in_block … VALUES (N, NULL::timestamp),…` | 1264 | 1262 | 1262 | 1262 |

The test process's own fixture and checker queries are not captured (only
spawned agents are patched). The in-process `db.js` calls in `e2e.spec.ts`
(`getAllKnownBlockHashes`, `saveTransactionForNodes`) run the same functions
the agent runs, and the e2e assertions on them pass.

## (d) Rename

`src/db.ts` still exists as a 6-line re-export shim, so plain `-M` reports a
rewrite of `db.ts` plus a new file. With break detection (`-B`), git pairs the
rename:

```text
$ git show -M -B --stat 0bc646c
 src/agent.ts                                    |    3 +-
 src/db.ts                                       | 1152 +----------------------
 src/{db.ts => store/postgres/postgres-store.ts} |  154 ++-
 3 files changed, 127 insertions(+), 1182 deletions(-)

$ git diff -M -B --stat c5cf5b5..2b02b35
 src/agent.ts                                    |  364 ++++---
 src/config.ts                                   |   97 ++
 src/db.ts                                       | 1152 +----------------------
 src/index.ts                                    |    5 +
 src/store/index.ts                              |   53 ++
 src/{db.ts => store/postgres/postgres-store.ts} |  154 ++-
 src/store/types.ts                              |  261 +++++
 7 files changed, 702 insertions(+), 1384 deletions(-)
```

The rename shows 91% similarity. The moved file's diff contains only:
- import path edits;
- the removed `import type { Agent }`;
- the four result interfaces moving to `src/store/types.ts` (re-exported);
- the `saveBlock` parameter change (`transactionCache` → `isSavedTransaction`, 3 lines);
- appended code: `managedIndexes`, `buildManagedIndexes`, `createPostgresStore`.

No SQL template text changed. Most of the `agent.ts` line count is prettier
re-indentation from `fn(...)` → `this.store\n  .fn(...)`; `git diff -w` gives
59+/102−.

## What is not a literal `fn(...)` → `this.store.fn(...)` change

None of these changes the SQL or the order of database calls:

1. **`saveBlock` takes `isSavedTransaction: (hash) => boolean`** (the design's
   one planned signature change). The agent passes
   `(hash) => this.transactionCache.get(hash)?.db === true`, the same test
   `db.ts` applied before. This removes the `db.ts` → `agent.ts` type import.
2. **The initial-sync finish moves into `store.finishInitialSync(hooks)`.** The
   synchronous_commit restore, index build and progress poller move from the
   agent into `postgres-store.ts` (`buildManagedIndexes`, `managedIndexes`).
   The order is unchanged: restore synchronous_commit, logging any error and
   continuing, then build indexes, with a 5 s progress poll whose errors are
   logged. A failed index build is still fatal and shuts down. The log lines
   are the same, emitted through the hooks `onSyncSettingsRestored`,
   `onNonFatalError` and `onIndexProgress`; the agent's new
   `logIndexCreationProgress` keeps the same formatting. The agent's
   `managedIndexes` property and `buildIndexes()` method are gone.
3. **`enableMempoolTracking()` resolves `{ schemaIsCurrent }`** instead of a
   bare boolean (`reenableMempoolCleaning` is unchanged underneath).
4. **`removeStaleBlocksForNode` takes an optional `removedAt`**, which the
   agent doesn't pass and Postgres ignores.
5. **`index.ts` runs `createStore()` and `await store.init()` (a no-op on
   Postgres) before `new Agent(...)`.** The top-level await defers agent
   construction by one microtask. The heartbeat's `pgPool` object now comes
   from `store.poolStats()` with the same shape and values, and shutdown calls
   `store.close()` (`pool.end()`).
6. **New config.** `CHAINGRAPH_STORE` defaults to `postgres`; an invalid value
   throws at startup. `clickhouse` requires a valid `CHAINGRAPH_CLICKHOUSE_URL`
   (the URL value is never echoed). `_DATABASE` defaults to `cg` and must be a
   plain identifier, `_USER` defaults to `default`, and `_PASSWORD` defaults to
   empty and is never logged. `createStore()` throws "not implemented yet
   (WP5)" for `clickhouse`. None of these variables are set in
   `defaults.env`, so existing deployments are unaffected.
