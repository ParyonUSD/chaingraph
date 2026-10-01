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

```sh
psql "$CHAINGRAPH_POSTGRES_CONNECTION_STRING" \
  --file scripts/output-node-membership/finalize.sql
```

The backfill commits after each heap-page range. Its cursor and batch history
are in `output_membership.state` and
`output_membership.backfill_batch`; rerunning the same `CALL` resumes it. The
procedure holds the same advisory lock used by ingestion triggers, so other
writes wait until it finishes. Keeping the agent stopped also prevents an
unbounded queue of waiting ingestion work.

The finalizer builds any missing indexes, analyzes `input` and `output`, and
independently reconstructs all accepted and unspent memberships from the
normalized tables. It marks the GraphQL roots ready only after finding no
missing, extra, malformed, or OP_RETURN memberships.

`unspent_node_ids` deliberately excludes locking bytecode beginning with
`OP_RETURN` (`0x6a`). Such outputs remain in `accepted_node_ids`, preserving
the distinction between transaction acceptance and spendable output state.
