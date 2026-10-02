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
nodes without trusting planner distinct estimates. It rejects the source build
unless the budget covers the acceptance heap, two acceptance lookup indexes, a
covering input outpoint index, the desired heap and CTID index, 15 percent page
overhead, and 64 GiB for WAL and checkpoints. `desired_row_ceiling` is enforced
across all committed target batches. The shown 160 million ceiling pads the
proven 133 million-row full-mainnet target by about 20 percent. Recheck the row
counts and free bytes immediately before a run.

The acceptance source commits first, so a disconnect during target construction
does not repeat acceptance. The helper adds bounded B-tree lookup indexes,
scans the output heap in batches, and commits the target cursor after every
batch. It drops the temporary input index and acceptance table after target
construction. This avoids database-sized hash and sort spills. Each CTID batch
then updates `public.output`, records its counts,
advances the durable cursor, and updates the canonical row count in one
transaction. A disconnected session can resume with the same command. Do not
run `VACUUM FULL`, `CLUSTER`, table rewrites, or normalized-state writes between
source creation and completion because the work table contains captured CTIDs.

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
