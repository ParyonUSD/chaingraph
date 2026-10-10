# Agent image with the ClickHouse store

`images/agent/Dockerfile` builds the agent (`node bin/chaingraph.js`, the default `CMD`) and,
since WP5c, ships the ClickHouse DDL and a standalone DDL step:

- `build/store/clickhouse/ddl/` in the image: `NNN_*.sql`, `README.md`, `apply.sh` (copied from
  `src/store/clickhouse/ddl`). `apply.sh` needs bash/curl/perl, which the alpine image does not
  have; in the image use the node CLI below.
- `bin/chaingraph-clickhouse-ddl.js` (`yarn clickhouse:ddl` in a checkout, after `yarn build`):
  applies every `NNN_*.sql` in order to `CHAINGRAPH_CLICKHOUSE_DATABASE` (created if missing),
  then lists the database's tables. Idempotent (every statement is `IF NOT EXISTS` /
  `CREATE OR REPLACE`), exit 0 on success, 1 on any failure. Reads only the
  `CHAINGRAPH_CLICKHOUSE_*` env vars (not `config.ts`, so no Postgres connection string). The
  password is never logged; the URL is logged without credentials, and error messages are redacted.
- `ddl-apply.ts` finds the DDL via `CHAINGRAPH_CLICKHOUSE_DDL_DIR` if set, else `./ddl` next to
  the compiled file (the image), else `src/store/clickhouse/ddl` of a source checkout.

## Build (linux/amd64)

`.yarn` is a git submodule (Yarn 3.3.1 release, plugins and offline cache) from
[ParyonUSD/chaingraph-dependencies](https://github.com/ParyonUSD/chaingraph-dependencies) branch
`clickhouse-store` (upstream `bitauth/chaingraph-dependencies` plus the `@clickhouse/client`
1.24.1 cache zip). Check out with `git clone --recursive`, or `git submodule update --init --depth 1`
in an existing clone. `git archive` does not include submodules, so export `.yarn` separately.
Build from an export of a commit so uncommitted edits never reach the image:

```sh
bash -c 'set -eu; SHA=$(git rev-parse --short HEAD); B=$(mktemp -d)
git archive HEAD | tar -x -C $B
git -C .yarn archive HEAD | tar -x -C $B/.yarn
cd $B && docker buildx build --platform linux/amd64 -f images/agent/Dockerfile \
  -t <registry>/chaingraph-agent:clickhouse-$SHA .'
# add --push to publish, or --load to keep it in the local daemon
```

The Dockerfile installs offline with `yarn install --immutable --immutable-cache`; every
package, `@clickhouse/client` included, comes from the submodule's `.yarn/cache`. No network
`yarn install` is needed. The base image is `node:24-alpine` pinned by digest (`NODE_IMAGE` arg).

## DDL step (init container or Job)

Same image, different command:

```sh
docker run --rm \
  -e CHAINGRAPH_CLICKHOUSE_URL=http://host.docker.internal:18123 \
  -e CHAINGRAPH_CLICKHOUSE_DATABASE=cg \
  <image> node bin/chaingraph-clickhouse-ddl.js
```

```yaml
initContainers:
  - name: clickhouse-ddl
    image: <registry>/chaingraph-agent:clickhouse-<sha>@sha256:<digest>
    command: ["node", "bin/chaingraph-clickhouse-ddl.js"]
    env: # same CHAINGRAPH_CLICKHOUSE_* as the agent (below)
```

Output: one line naming endpoint, database, user, DDL directory and files; then
`ok, 57 statement(s) in N ms; <db> has 45 table(s)/view(s): …`.

## Pod environment (agent)

| Var | Value |
| --- | --- |
| `CHAINGRAPH_STORE` | `clickhouse` |
| `CHAINGRAPH_CLICKHOUSE_URL` | HTTP(S) endpoint, e.g. `http://clickhouse.<ns>.svc.cluster.local:8123` or Cloud `https://<host>:8443` (from a Secret if it embeds credentials) |
| `CHAINGRAPH_CLICKHOUSE_DATABASE` | default `cg` |
| `CHAINGRAPH_CLICKHOUSE_USER` / `_PASSWORD` | from a Secret; default `default` / empty. Credentials in the URL are used if these are empty |
| `CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES` | in-flight cap; default `0` = unbounded. Lab: `16` (the Postgres pool size it is compared with) |
| `CHAINGRAPH_CLICKHOUSE_PENDING_SPEND_TIMEOUT_MS` | optional; default `60000`. Lab: `600000` (`wp5c-hardening.md` §4) |
| `CHAINGRAPH_BLOCK_BUFFER_TARGET_SIZE_MB` | set explicitly (the automatic size is derived from the Postgres connection count). Lab: `512` |
| `CHAINGRAPH_CLICKHOUSE_MAX_BLOCKS_PER_COMMIT` | optional; blocks per multi-block commit, default `64` (`wp6b-write-path.md`). Lab: `64` |
| `CHAINGRAPH_CLICKHOUSE_MAX_BYTES_PER_COMMIT` | optional; block bytes per multi-block commit, default 32 MiB (a larger block commits alone). Lab: default (32 MiB) |
| `CHAINGRAPH_CLICKHOUSE_LEASE_TTL_MS` | optional; writer lease TTL, integer ≥ 1000, default `120000` (`wp6b-gate-cost.md`). Lab: `120000` |
| `CHAINGRAPH_CLICKHOUSE_DDL_DIR` | optional (DDL CLI only), override the DDL directory |
| `CHAINGRAPH_POSTGRES_CONNECTION_STRING` | **not needed** with `CHAINGRAPH_STORE=clickhouse` (ignored if set; WP5c, `wp5c-hardening.md` §2); remove the dummy |
| `CHAINGRAPH_TRUSTED_NODES` etc. | as for the Postgres agent |

## Verification (WP5c, 2026-10-09, image built from `f7b4481`, before the WP5a-mempool commits)

MacBook (Apple M-series), Docker Desktop, buildx v0.37.0, cross-building linux/amd64 under emulation;
build from `git archive HEAD` + working `.yarn`, only the base image cached (before the
submodule moved to ParyonUSD/chaingraph-dependencies; see the submodule build row).

| Item | Result |
| --- | --- |
| `docker buildx build --platform linux/amd64 --load -t ch1-agent:local` | ok, 24 s (yarn install 8.7 s, tsc 8.6 s) |
| Image | linux/amd64, 70.9 MB, node v24.21.0 (`node:24-alpine`) |
| DDL CLI in the container → `http://host.docker.internal:18123`, db `ch1_wp5c_image` | exit 0, 57 statements, 45 tables/views (10 MergeTree, 4 Replacing, 6 VersionedCollapsing, 25 views); re-run exit 0 (idempotent); database dropped afterwards |
| `CHAINGRAPH_CLICKHOUSE_DDL_DIR=/nope` | exit 1, clear message |
| `build/config.js` in the image with `CHAINGRAPH_STORE=clickhouse` and no Postgres var | loads (defaults.env value) |
| Clean `.yarn` submodule (bitauth upstream, no ClickHouse zip), `--target build-stage` | fails at `yarn install` (YN0056); fixed by the ParyonUSD `clickhouse-store` submodule |
