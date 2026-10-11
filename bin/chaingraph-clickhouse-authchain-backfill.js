#!/usr/bin/env node
/**
 * Backfill the ClickHouse authchain index of CHAINGRAPH_CLICKHOUSE_URL /
 * _DATABASE (see src/store/clickhouse/authchain-backfill-cli.ts). Requires a
 * build (`yarn build`) and the DDL (bin/chaingraph-clickhouse-ddl.js).
 */
import '../build/store/clickhouse/authchain-backfill-cli.js';
