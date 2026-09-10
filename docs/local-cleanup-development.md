# Local chipnet development for mempool cleanup

Prepared on 10 September 2026 on branch `fix/mempool-cleanup-performance`.
This setup reuses the existing local chipnet database and the user's BCHN GUI.
It does not use either DigitalOcean cluster.

## Services

| Component | Local configuration |
| --- | --- |
| BCHN | Bitcoin Cash Node 29.0.0 GUI, launched by the user with `-chipnet`; P2P `127.0.0.1:48333` |
| PostgreSQL | Native PostgreSQL 18.3; `data/postgres-local`; database `chaingraph` on port 5432 |
| Hasura | Repository image built from v2.49.5, container `chaingraph-cleanup-hasura`; `http://127.0.0.1:8080` |
| Agent | Local source, built with `yarn build`, then `node bin/chaingraph.js`; internal API port 3200 |

The existing `.env` supplies database credentials and exactly one trusted node:

```dotenv
CHAINGRAPH_TRUSTED_NODES=bchn-chipnet:127.0.0.1:48333:chipnet
```

Before starting, confirm the connection string targets the local `chaingraph`
database. Do not copy a remote connection string into this environment.
Hasura connects to this same database through `host.docker.internal`.
Its Docker port is bound to `127.0.0.1`; the local console needs no admin secret.

## Start and inspect the existing setup

Run these commands from the repository root. Start each service only if stopped.

```sh
pg_ctl -D data/postgres-local status
# If stopped:
pg_ctl -D data/postgres-local \
  -l .scratch/cleanup-development/postgres.log \
  -o '-c listen_addresses=localhost -p 5432' -w start

docker start chaingraph-cleanup-hasura

yarn build
NODE_ENV=development \
CHAINGRAPH_BLOCK_BUFFER_TARGET_SIZE_MB=0.25 \
CHAINGRAPH_POSTGRES_MAX_CONNECTIONS=2 \
CHAINGRAPH_LOG_PATH=.scratch/cleanup-development/agent.ndjson \
node bin/chaingraph.js
```

The last command runs in the foreground. Stop it before starting another agent.
Changes to TypeScript require rebuilding and restarting the agent.

The initial setup used a detached agent process, with its PID in
`.scratch/cleanup-development/agent.pid` and stdout/stderr in
`.scratch/cleanup-development/agent.stdout.log`. These files are not maintained
automatically when starting the foreground command above.

```sh
# Inspect the initially launched background agent:
ps -p "$(cat .scratch/cleanup-development/agent.pid)" -o pid,etime,stat,command
tail -f .scratch/cleanup-development/agent.stdout.log

# Inspect other local services:
docker logs -f chaingraph-cleanup-hasura
tail -f .scratch/cleanup-development/postgres.log

# Health and GraphQL:
curl http://127.0.0.1:3200/health-check
curl http://127.0.0.1:8080/healthz
curl -H 'Content-Type: application/json' \
  -d '{"query":"{ node { name } block(order_by: {height: desc}, limit: 1) { height } }"}' \
  http://127.0.0.1:8080/v1/graphql
```

The small buffer and two database connections reproduce the successful chipnet
catch-up settings recorded in `.scratch/pg-duckdb-local-runbook.md`. With the
automatic buffer, this startup queued many block reservations, reached the
200-download limit, and waited for five-minute retries. Only local runtime
settings were adjusted. Incomplete-block repair and mempool cleanup remain
enabled; their implementations are unchanged.

## Existing work and validation

Evidence of earlier development includes:

- `.scratch/chaingraph-agent-repair-handoff.md`, covering confirmation cleanup,
  expiry, cascading invalidation and historical block repair.
- `.scratch/pg-duckdb-local-runbook.md`, including the previous chipnet setup and
  small-buffer workaround. This setup uses ordinary PostgreSQL, without DuckDB.
- Existing regression cases in `src/e2e/e2e.spec.ts`.

The reused database initially contained only `bchn-chipnet`, with maximum saved
block height 304,040. BCHN RPC reported chain `chip`, height 322,981, and
`initialblockdownload=false` before the agent began catching up.

The current source built successfully, and **all 89 existing end-to-end tests
passed**. The run used Node 26.0.0 and PostgreSQL 18.3. It includes expiry and
descendant removal, stale confirmed mempool cleanup, and reorg regressions.
Hasura applied all 11 repository migrations, reported consistent metadata and
returned chipnet data over GraphQL.

To rerun the existing suite:

```sh
yarn build
CHAINGRAPH_E2E_POSTGRES_HOST=127.0.0.1 \
CHAINGRAPH_E2E_POSTGRES_PORT=5432 \
yarn test:e2e
```

The suite drops and recreates **only `chaingraph_e2e_test`**, uses simulated local
peers on ports 19333–19335, and runs its own agent on port 3201. It has sequential
setup dependencies, so run the full suite rather than selecting only a late
cleanup test. It does not connect to BCH mainnet.

Backups of both pre-existing local databases, plus the previous Hasura catalog,
are in `.scratch/cleanup-development/`. The test output is `e2e-tests.log` there.
These backups and logs remain ignored by Git. The cleanup performance fix has
not yet been implemented or deployed.
