# `CHAINGRAPH_CLICKHOUSE_UTXO=off`: no stored UTXO tables

`CHAINGRAPH_CLICKHOUSE_UTXO=on|off` (default `on`, validated in `src/config.ts`) picks whether the ClickHouse store
keeps the per-node UTXO set in `utxo` / `utxo_by_script`. With `off` the store offers what upstream Chaingraph v1
offers: no stored unspent root. Unspent outputs are worked out at query time from `output`, `input` and acceptance.
The store option is `ClickHouseStoreOptions.utxo` (`'on' | 'off'`). The checker option is
`createClickHouseChecker(client, db, { utxo })` / `createChecker({ backend: 'clickhouse', …, utxo })`.

Set it **once per database**. Flipping it on a database that already has data is not supported: `off → on` leaves
`utxo` missing everything written while off, and `on → off` leaves stale rows nobody updates.

## What is skipped (off)

No row is written to `utxo` or `utxo_by_script` on any path:

| path                                                                       | where                                                | how it is skipped                          |
| -------------------------------------------------------------------------- | ---------------------------------------------------- | ------------------------------------------ |
| block save (inline +1/−1, tip mode)                                        | `block-commit.ts` `blockUtxoDelta`                   | `utxo: false` returns no rows              |
| block save, mempool cleanup rows                                           | `block-commit.ts` → `changeRows`                     | `{ utxo: false }` leaves `rows.utxo` empty |
| block save, pending-spend fill (−1 once the parent arrives)                | `block-commit.ts` fill step                          | `fillUtxo` is empty                        |
| header acceptance (`INSERT … SELECT` deltas)                               | `clickhouse-store.ts` `acceptBlocksViaHeaders`       | statement not issued                       |
| re-org release (`INSERT … SELECT` deltas)                                  | `clickhouse-store.ts` `removeStaleBlocksForNode`     | statement not issued                       |
| mempool add / replace / conflict / cascade / expire / confirm / resolution | `mempool-commit.ts` `changeRows`                     | `{ utxo: false }`                          |
| lease recovery, re-run calls                                               | same committers (they share the `utxo` context flag) | same as above                              |
| horizon UTXO build                                                         | `finishInitialSync`                                  | skipped entirely, see below                |

There is also a second guard. Each RowBinary insert helper (`block-commit.ts`, both helpers in `mempool-commit.ts`)
refuses the tables in `utxoTables` when the context's `utxo` is `false`. So a row a pure function let through still
never reaches ClickHouse.

**Initial sync.** `prepareForInitialSync` writes its `horizon_switch` (bulk enter) as before. `finishInitialSync`
writes no `utxo_build` commit and runs no build statements. It records the switch back to tip mode as a second
`horizon_switch` commit with row counts `bulk_exit = 1, bulk_start = <seq>`. On start, `startEpoch` reads the
latest committed `horizon_switch` / `utxo_build`, and it resumes bulk mode only for a `horizon_switch` without
`bulk_exit`. So a restart after the sync stays in tip mode. Tip mode still matters when off: block saves decide
acceptance transitions from stored state, and the running batches per node set go back to 1.

## What still runs (off)

- Everything that is not the UTXO set: `output`, `input`, `transaction`, `block_transaction`, `node_block`,
  `tx_acceptance`, `node_transaction`, history, `pending_spend` and `commit_log` are written exactly as with `on`.
- **Pending-spend resolution.** An `input` row carries the attributes of the output it spends (value, locking
  bytecode, token fields). That is a base fact, not part of the UTXO set. So child-before-parent waits, the
  `pending_spend` rows and the fill of `input` all behave as with `on`, and `pendingUtxo` is still computed, so the
  `pending_spend` rows match too. Only the utxo −1 rows of the fill are dropped.
- Commit dependencies, the visibility gate, fencing, recovery: all unchanged.

## How `unspent` is answered (off)

`ClickHouseChecker.unspent(node, scope)` takes one node snapshot, as every checker read does, and asks through the
pinned views:

```sql
SELECT DISTINCT lower(hex(transaction_hash)), output_index
FROM output_at(visible0, tail, fence, void)
WHERE transaction_hash IN (SELECT transaction_hash FROM tx_acceptance_at(node, visible, fence, void))
  AND (transaction_hash, output_index) NOT IN (
        SELECT outpoint_transaction_hash, outpoint_index FROM input_at(visible0, tail, fence, void)
        WHERE transaction_hash IN (SELECT transaction_hash FROM tx_acceptance_at(node, visible, fence, void)))
  [AND locking_bytecode = unhex({lockingBytecode})] [AND token_category = {category}]
ORDER BY 1, 2
```

In words: outputs of transactions accepted by node n (in a block n accepts, or in n's mempool), minus outpoints spent
by an input of a transaction n accepts. This is Chaingraph v1's definition. It names the node, so the per-node rule
holds. With `on` the checker keeps reading `utxo_at` / `utxo_by_script_at`. The e2e harness passes the agent's
`CHAINGRAPH_CLICKHOUSE_UTXO` to the checker (`e2eClickHouseUtxo()`), and the variable is on the helper's
pass-through list.

## Trade-off

|                  | `on` (stored UTXO tables)                                                                                                                                                                                                                                               | `off` (v1 style)                                                                                                                                                                                                                                         |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| write cost       | +2 collapsing tables. Every acceptance transition writes 2 rows per output and per spent output; header acceptance and re-org run an `INSERT … SELECT` join over `output`/`input` per table; the end of initial sync runs the horizon build over every bulk-mode height | none of that: block, header, re-org and mempool commits write only base facts and acceptance                                                                                                                                                             |
| storage          | two more copies of every output's facts per node (plus −1 rows until merges collapse them), and the `utxo_by_script` projection                                                                                                                                         | none beyond the base tables                                                                                                                                                                                                                              |
| unspent query    | a key lookup on the collapsed set (by category, or by the 25-byte script prefix)                                                                                                                                                                                        | a join at query time: `output` ⋈ `tx_acceptance` per node, anti-joined with `input` ⋈ `tx_acceptance`. Cheap when scoped to a script or category with a selective filter; a whole-set or broad-category scan grows with all outputs/inputs ever accepted |
| initial sync end | horizon build (minutes to hours on mainnet)                                                                                                                                                                                                                             | instant (one `horizon_switch`)                                                                                                                                                                                                                           |

Pick `off` when ingestion throughput and storage matter more than fast unspent lookups, or to compare against
upstream v1 behavior. Keep `on` for an API that serves address/token balance queries at tip.

## Tests

- Unit (`block-commit.spec.ts`): with `utxo: false`, `blockUtxoDelta` and `changeRows` produce no utxo rows across
  every mempool transition, and their pending/other rows equal the `on` output.
- `[e2e]` `utxo-off.spec.ts`: the same history on two scratch databases (`ch1_utxooff_*`, one on and one off): a bulk
  horizon with a restart, a 3-block 2-node chain with spends, a mempool replacement on one node, a re-org onto a
  competing block and a header re-acceptance. With off, `utxo` / `utxo_by_script` are empty and there is no
  `utxo_build`. `checker.unspent` per node (unscoped, by category, by script) equals the on-mode answer.
- The full ClickHouse e2e suite passes with `CHAINGRAPH_CLICKHOUSE_UTXO=off` and with the default.
