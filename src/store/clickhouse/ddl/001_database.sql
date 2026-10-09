-- Chaingraph ClickHouse primary store: database.
-- Plan: docs/chaingraph/plans/clickhouse-primary-store.md (paryon_kubernetes), §2.
-- The database name is the literal `cg` throughout; apply.sh can rewrite it (e.g. for a scratch db).
CREATE DATABASE IF NOT EXISTS cg;
