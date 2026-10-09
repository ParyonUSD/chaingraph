#!/usr/bin/env node
/**
 * Apply the ClickHouse DDL to CHAINGRAPH_CLICKHOUSE_URL / _DATABASE (see
 * src/store/clickhouse/ddl-cli.ts). Requires a build (`yarn build`); the agent
 * image ships one. Usable as a Kubernetes init container or Job:
 *   node bin/chaingraph-clickhouse-ddl.js
 */
import '../build/store/clickhouse/ddl-cli.js';
