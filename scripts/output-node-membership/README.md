# Output node membership rollout

The Hasura migration adds maintained `accepted_node_ids` and
`unspent_node_ids` arrays to `public.output`. It does not expose partially
built state: `accepted_output` and `unspent_output` raise an error until the
state has passed exact validation.

For a database populated after the migration, leave the agent running through
initial sync. The statement triggers maintain both arrays during ingestion.
After sync and managed-index creation complete, run the backfill call (it has
no historical heap pages to scan) and the finalizer.

For an existing database, stop the Chaingraph agent before applying the
migration. The migration uses the most widely accepted node as a compact
historical default, without rewriting the output table. Run the resumable
backfill, then the finalizer:

```sql
CALL output_membership.backfill(20000);
```

For a large database, use the offline fast backfill instead of the call above.
It scans normalized acceptance, inputs, and outputs once to materialize exact
desired arrays, then updates only rows whose current arrays differ. It includes
rows missed by a partly completed original backfill.

Keep the agent paused, suspend any job which can call the original backfill,
and stop manual writes for the whole run. Install and run it from a session
which will remain connected:

```sh
psql "$CHAINGRAPH_POSTGRES_CONNECTION_STRING" \
  --file scripts/output-node-membership/install-fast-backfill.sql

psql "$CHAINGRAPH_POSTGRES_CONNECTION_STRING" \
  --set=batch_heap_blocks=20000 \
  --set=operator_confirmation=WRITERS_PAUSED \
  --set=scratch_budget_bytes=442381631488 \
  --set=desired_row_ceiling=160000000 \
  --file scripts/output-node-membership/run-fast-backfill.sql
```

The run rejects a ready rollout and takes advisory lock `(20261001, 1)`, the
same lock used by ingestion and the original backfill. A lock failure means a
writer or another backfill is still active. The confirmation value is an
operator assertion because PostgreSQL cannot verify Kubernetes replica or job
suspension state.

Set `scratch_budget_bytes` to current free filesystem bytes, not total volume
capacity. The helper counts `input`, `output`, `transaction`, and configured
nodes without trusting planner distinct estimates. After acceptance it measures
the non-default acceptance relation and gates the larger of two disjoint peaks:
one 112-byte-per-input external outpoint sort plus ceiling-bounded sparse source
stages, or the sparse source and CTID target plus an 8 GiB hash-spill allowance.
The gate adds 15 percent page overhead and reserves 64 GiB for WAL and
checkpoints. `desired_row_ceiling` bounds every per-node source, the combined
sparse source, and the final target. The shown 160 million ceiling pads the
proven 133 million-row full-mainnet target by about 20 percent. Recheck free
bytes immediately before every run or resume; the replacement target records
that current-free-space value independently of the earlier acceptance budget.

The acceptance source commits first, so a disconnect during target construction
does not repeat acceptance. The dense default node retains the global merge
stream and one external outpoint sort with `GroupAggregate`. Non-default nodes
scan their small acceptance-exception indexes in transaction-key order and use
parameterized primary-key probes into `input` and `output`. The helper stores
the sparse node's spent keys in the existing capped node stage, then
merge-anti-joins ordered creator probes against those keys. This avoids another
full pass over the mainnet input and output indexes. The creator plan uses an
incremental sort within each transaction hash. The spender statement has one
outpoint sort and `GroupAggregate`. These statements run in sequence and do not
add a second ceiling-sized stage to the scratch bound.

The helper commits a durable checkpoint after each node. A sparse-node failure
rolls back its spent stage and desired rows while retaining every completed-node
checkpoint. A final sequential output scan applies leading-`OP_RETURN`
exclusion and materializes only rows whose current arrays differ. The acceptance
and temporary input-index objects remain until cleanup so recovery never
discards their proven sources. Each CTID batch then updates `public.output`,
records counts, advances the durable cursor, and updates the canonical row count
in one transaction. A disconnected session can resume with the same command.
Do not run `VACUUM FULL`, `CLUSTER`, table rewrites, or normalized-state writes
between source creation and completion because the work table contains captured
CTIDs.

Check progress without exposing connection details:

```sh
psql "$CHAINGRAPH_POSTGRES_CONNECTION_STRING" \
  --file scripts/output-node-membership/status-fast-backfill.sql
```

After its phase is `backfilled`, run `finalize.sql` as below. The finalizer
independently reconstructs membership before setting readiness to true:

```sh
psql "$CHAINGRAPH_POSTGRES_CONNECTION_STRING" \
  --file scripts/output-node-membership/finalize.sql
```

Once that succeeds, remove the potentially large desired table. The small state
and batch tables remain as durable backfill history:

```sh
psql "$CHAINGRAPH_POSTGRES_CONNECTION_STRING" \
  --file scripts/output-node-membership/cleanup-fast-backfill.sql
```

The helper leaves all output indexes in place. Dropping them would reduce
per-row index maintenance, but it also removes uniqueness and query support
until full rebuilds finish. The current production estimate does not prove that
index maintenance dominates this sparse update. Any later index-drop variant
must first save exact definitions, reject inbound foreign keys, measure a batch
with indexes present, and prove restoration on a copy.

The original backfill commits after each heap-page range. Its cursor and batch
history are in `output_membership.state` and
`output_membership.backfill_batch`; rerunning the original `CALL` resumes it.
Both procedures hold the same advisory lock used by ingestion triggers, so
other writes wait until the active procedure finishes. Keeping the agent
stopped also prevents an unbounded queue of waiting ingestion work.

The finalizer builds any missing indexes, analyzes `input` and `output`, and
independently reconstructs all accepted and unspent memberships from the
normalized tables. It marks the GraphQL roots ready only after finding no
missing, extra, malformed, or OP_RETURN memberships.

`unspent_node_ids` deliberately excludes locking bytecode beginning with
`OP_RETURN` (`0x6a`). Such outputs remain in `accepted_node_ids`, preserving
the distinction between transaction acceptance and spendable output state.
