# Fresh no-array baseline images

This explicit profile packages the latest agent in `baseline` mode with a
matching normalized Hasura schema. The canonical image profile is unchanged.
Use it only with a fresh database: it excludes membership migrations before
application, rather than dropping columns or undoing a populated backfill.

The selected migrations are every migration before `1790852400000` plus
`1790950000000_bound_block_confirmation_mempool_cleanup` and
`1791100004000_add_node_block_membership_index`. This keeps normalized mempool
cleanup, parser and aggregate fixes, and the normalized node-block index.
No `output_membership` schema, output membership array columns, maintenance
triggers or four array indexes are installed. The agent retains startup and
periodic mempool refresh and the pool, heap and block buffer configuration.

Only this profile's packaged metadata excludes the two tracked functions
`accepted_output` and `unspent_output`,
because the excluded migrations define those functions. Their GraphQL roots
are absent. The original normalized `search_output` and `search_output_prefix`
functions, tables, relationships and computed fields remain. Search functions
retain their pre-array semantics, including the original 25-byte prefix limits.

After committing, prepare a clean source archive and pinned offline dependencies:

```sh
python3 images/no-array-baseline/prepare_context.py \
  --source . --revision HEAD --yarn-source ../chaingraph-production \
  --destination /private/tmp/chaingraph-no-array-context
```

`--destination` must be new. Dependencies must match `yarn.lock`; the build
checks immutable installation and records the archive, lock and all dependency
hashes. No local `.env`, database, `node_modules` or compiled files are copied.
Build both images from that same revision, with the archived profile files:

```sh
docker build --platform linux/amd64 --build-arg SOURCE_REVISION=REVISION \
  -f /private/tmp/chaingraph-no-array-context/images/no-array-baseline/agent.Dockerfile \
  -t chaingraph/no-array-agent:REVISION /private/tmp/chaingraph-no-array-context
docker build --platform linux/amd64 --build-arg SOURCE_REVISION=REVISION \
  -f /private/tmp/chaingraph-no-array-context/images/no-array-baseline/hasura.Dockerfile \
  -t chaingraph/no-array-hasura:REVISION /private/tmp/chaingraph-no-array-context/images/hasura
```

The manifests are pinned amd64 Node 24 and Hasura 2.49.5 bases. Both images
record the source revision and `no-array-baseline` profile labels. Keep agent
`CHAINGRAPH_OUTPUT_MEMBERSHIP_MODE=baseline`; its startup guard rejects the
canonical array schema. No membership backfill or finalizer is used.
