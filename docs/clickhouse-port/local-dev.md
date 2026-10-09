# Local development (ClickHouse port)

Everything runs on the laptop; no cloud. Container/volume names are prefixed
`ch1-` so they can be stopped/removed without generic patterns.

## Checkout

```sh
git clone --recursive -b experiment/clickhouse-store https://github.com/ParyonUSD/chaingraph.git
# existing clone / worktree:
git submodule update --init --depth 1 -- .yarn
YARN_ENABLE_NETWORK=0 yarn install --immutable --immutable-cache   # offline, links node_modules
```

`.yarn` is a submodule: ParyonUSD/chaingraph-dependencies branch `clickhouse-store` (Yarn 3.3.1,
plugins, offline cache incl. `@clickhouse/client`). The offline install above is the only
`yarn install` needed.

## Containers

Postgres for the e2e suite and the ingestion gate (same image/env as CI,
`.github/workflows/ci.yaml`):

```sh
docker volume create ch1-pg-data
docker run -d --name ch1-pg -p 15432:5432 \
  -e POSTGRES_DB=chaingraph -e POSTGRES_USER=chaingraph \
  -e POSTGRES_PASSWORD=very_insecure_postgres_password \
  -v ch1-pg-data:/var/lib/postgresql/data \
  --health-cmd pg_isready --health-interval 2s \
  postgres:14
docker exec ch1-pg pg_isready -U chaingraph
```

ClickHouse (HTTP on http://localhost:18123, native on 19000, user `default`,
no password):

```sh
docker volume create ch1-local-data
docker run -d --name ch1-local -p 18123:8123 -p 19000:9000 \
  --ulimit nofile=262144:262144 \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  -v ch1-local-data:/var/lib/clickhouse \
  clickhouse/clickhouse-server:26.8
curl -s http://localhost:18123/ping   # Ok.
```

Stop/start: `docker stop ch1-pg ch1-local` / `docker start ch1-pg ch1-local`.

## Env vars

```sh
export CHAINGRAPH_E2E_POSTGRES_HOST=localhost
export CHAINGRAPH_E2E_POSTGRES_PORT=15432
```

## Commands

```sh
yarn build
yarn test:e2e     # needs ch1-pg + env vars above
yarn test:unit
yarn test         # build + lint + prettier + cspell + all ava tests
```

Postgres-only e2e tests carry `[postgres]` in their title; filter e.g. with
`npx ava --match='*[e2e]*' --match='!*[postgres]*'` (after `yarn build`).

## Baseline (branch experiment/clickhouse-store @ e561d0c, untouched)

MacBook Pro (Apple M-series, 10 cores), Node 24, Docker Desktop VM 8 GB.

| Run | Result | Wall |
| --- | --- | --- |
| `yarn build` | ok | 1.5 s |
| `yarn test:e2e` | 97 passed (92 `e2e.spec.ts` + 5 `mempool-cleanup.spec.ts`), 0 failed | 13.6 s |
| `yarn test:unit` | 21 passed, 1 todo | 1.3 s |

## Ingestion gate against ch1-pg

The gate (`docs/ingestion-gate.md`) defaults to its own throwaway server; on
this machine the Docker VM has only 8 GB so `--pg auto` would pick a host
cluster from Homebrew postgresql@18. Instead it was pointed at ch1-pg with
`--pg-url` (the gate creates/drops `chaingraph_ingestion_gate`; the
`chaingraph` user is a superuser in the official image, so no extra setup):

```sh
yarn build
yarn ingestion-gate --scenarios reorg \
  --pg-url postgres://chaingraph:very_insecure_postgres_password@localhost:15432
yarn ingestion-gate --scenarios max-block \
  --pg-url postgres://chaingraph:very_insecure_postgres_password@localhost:15432
```

Note ch1-pg runs Postgres 14 with stock settings (shared_buffers 128 MB,
default max_wal_size), not the gate's tuned Postgres 18; numbers are not
directly comparable with the reference table in `docs/ingestion-gate.md`.

| Scenario (agent @ 5fa58c7) | Status | Headline | tx/s | WAL | peak heap | gate wall |
| --- | --- | --- | --- | --- | --- | --- |
| reorg | PASS* (known deep-reorg check allow-listed) | converge 1.50 s | 66,532 | 223.5 MB | – | 6.9 s |
| max-block (31.80 MB, 100,001 txs) | PASS | wall 6.07 s | 16,480 | 224.9 MB | 1,092 MB | 9.4 s |

max-block fits in the 8 GB VM when run alone (burst, 3 blocks, would not).

## After WP1a (StoreChecker port)

`e2e.spec.ts` still has 92 tests, all green on ch1-pg; the file runs in ~8 s
(was ~13 s) because fixed 1 s sleeps after agent events became
`eventually()` polls. 47 of the 92 carry `[postgres]` (37 SQL-function macro
and encoder tests, `transaction_data_carrier_outputs`, 3 `search_output*`,
indexes/triggers, concurrent-conflict, cascade, expiry, confirmed-archive,
backfill).

Run only this file: `npx ava build/e2e/e2e.spec.js --match='*[e2e]*'`.

Note: the `search functions use the 25-byte … prefix index` EXPLAIN test was
timing-dependent: once auto-vacuum has analyzed `output` the planner picks
`output_search_index` instead of the test copy. It failed reproducibly on the
long-lived ch1-pg and passed on a fresh container; fixed in 4ef9e3f by
accepting either index.

## Applying the DDL (CLI)

`yarn build`, then (env only; password never logged; idempotent; exit 0/1):

```sh
CHAINGRAPH_CLICKHOUSE_URL=http://localhost:18123 CHAINGRAPH_CLICKHOUSE_DATABASE=cg yarn clickhouse:ddl
```

Same step from the agent image (`node bin/chaingraph-clickhouse-ddl.js`, URL
`http://host.docker.internal:18123`), building the image and the pod env: [image.md](image.md).
`src/store/clickhouse/ddl/apply.sh` (curl) still works from a checkout.
