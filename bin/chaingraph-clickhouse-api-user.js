#!/usr/bin/env node
/**
 * The API's ClickHouse user with its max_concurrent_queries_for_user
 * backstop (see src/store/clickhouse/api-user-cli.ts). Requires a build:
 *   node bin/chaingraph-clickhouse-api-user.js <xml|sql|apply>
 */
import '../build/store/clickhouse/api-user-cli.js';
