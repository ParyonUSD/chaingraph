/* eslint-disable max-params */
// cspell:ignore clickhouse
/**
 * The API's ClickHouse user and its settings profile (fix-pass-4.md §8,
 * owner decision 2026-10-12): a cluster-wide backstop on the statements the
 * API user may run at once, `max_concurrent_queries_for_user = pods ×
 * statementsPerPod + margin` (default `pods × 72 + 10`). Each API process
 * sends at most 64 statements at once (its ClickHouse client's sockets;
 * since fix pass 8 its admission budgets requests by CPU, not statements);
 * with several worker processes per pod, count processes as pods. The
 * backstop bounds all API pods together, so a mis-sized pod count or a
 * misbehaving process cannot flood the server. Over it ClickHouse refuses a
 * statement at once (code 202 TOO_MANY_SIMULTANEOUS_QUERIES). The margin
 * covers what the gate does not count: watermark polls, live-query
 * refreshes and `KILL QUERY` of abandoned statements. The setting is
 * `const` in the profile, so a session cannot raise it.
 *
 * Two renderings of the same profile: users.d XML for config-file users
 * (the Kubernetes kits mount it from a Secret) and SQL for servers with SQL
 * access management (the local Docker server). See
 * docs/clickhouse-port/fix-pass-5.md and local-dev.md.
 */
import { createHash } from 'node:crypto';

/** The settings profile of the API user. */
export const apiProfileName = 'chaingraph_api';

/** Statements per API pod (process): its ClickHouse client's 64 sockets plus headroom (fix-pass-8.md). */
export const defaultStatementsPerPod = 72;

/** Statements outside the gate (watermark polls, live refreshes, kills). */
export const defaultStatementsMargin = 10;

export interface ApiUserBackstop {
  /** API pods (processes) reading through this user. */
  pods: number;
  statementsPerPod: number;
  margin: number;
}

/** `max_concurrent_queries_for_user` of the API user's profile. */
export const apiMaxConcurrentQueries = ({
  margin,
  pods,
  statementsPerPod,
}: ApiUserBackstop) => pods * statementsPerPod + margin;

const positiveInteger = (
  env: NodeJS.ProcessEnv,
  name: string,
  fallback: number,
  minimum: number
) => {
  const raw = env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(`${name} must be an integer >= ${minimum}.`);
  }
  return value;
};

/**
 * CHAINGRAPH_CLICKHOUSE_API_PODS (default 1),
 * CHAINGRAPH_CLICKHOUSE_API_STATEMENTS_PER_POD (default 72),
 * CHAINGRAPH_CLICKHOUSE_API_STATEMENTS_MARGIN (default 10).
 */
export const apiUserBackstopFromEnv = (
  env: NodeJS.ProcessEnv = process.env
): ApiUserBackstop => ({
  margin: positiveInteger(
    env,
    'CHAINGRAPH_CLICKHOUSE_API_STATEMENTS_MARGIN',
    defaultStatementsMargin,
    0
  ),
  pods: positiveInteger(env, 'CHAINGRAPH_CLICKHOUSE_API_PODS', 1, 1),
  statementsPerPod: positiveInteger(
    env,
    'CHAINGRAPH_CLICKHOUSE_API_STATEMENTS_PER_POD',
    defaultStatementsPerPod,
    1
  ),
});

const identifierPattern = /^[A-Za-z_][A-Za-z0-9_]*$/u;

const checkIdentifier = (kind: string, name: string) => {
  if (!identifierPattern.test(name)) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(`${kind} must match ${identifierPattern.source}.`);
  }
  return name;
};

export interface ApiUserDefinition {
  /** The API's ClickHouse user. */
  user: string;
  /** The user's password (only its sha256 is written). */
  password: string;
  /** The database the user reads (SQL rendering: `GRANT SELECT`). */
  database: string;
  backstop: ApiUserBackstop;
  /** The profile the API profile inherits from (default `default`). */
  parentProfile?: string;
  /** The API profile's name (default `chaingraph_api`). */
  profile?: string;
}

const sha256Hex = (text: string) =>
  createHash('sha256').update(text).digest('hex');

/**
 * users.d XML: profile `chaingraph_api` (inherits `default`, readonly = 2,
 * the backstop, `const`) and the user with that profile, password as
 * sha256 only.
 */
export const apiUserXml = (definition: ApiUserDefinition) => {
  const user = checkIdentifier('user', definition.user);
  const parent = checkIdentifier(
    'parent profile',
    definition.parentProfile ?? 'default'
  );
  const profile = checkIdentifier(
    'profile',
    definition.profile ?? apiProfileName
  );
  const limit = apiMaxConcurrentQueries(definition.backstop);
  return `<clickhouse>
  <!-- Chaingraph API user; max_concurrent_queries_for_user = ${
    definition.backstop.pods
  } pods x ${definition.backstop.statementsPerPod} + ${
    definition.backstop.margin
  } -->
  <profiles>
    <${profile}>
      <profile>${parent}</profile>
      <readonly>2</readonly>
      <max_concurrent_queries_for_user>${limit}</max_concurrent_queries_for_user>
      <constraints>
        <max_concurrent_queries_for_user><const/></max_concurrent_queries_for_user>
      </constraints>
    </${profile}>
  </profiles>
  <users>
    <${user}>
      <password_sha256_hex>${sha256Hex(
        definition.password
      )}</password_sha256_hex>
      <profile>${profile}</profile>
      <quota>default</quota>
      <networks><ip>::/0</ip></networks>
      <access_management>0</access_management>
    </${user}>
  </users>
</clickhouse>
`;
};

/**
 * SQL (servers with SQL access management): the profile, the user (created
 * if missing, password as sha256), the profile assigned and SELECT on the
 * database plus the system tables the API reads (`system.processes`:
 * KILL QUERY of its own abandoned statements; `system.projections`: store
 * features; `system.query_log`: the measured CPU of its own statements,
 * admission.ts). An XML user has every grant unless restricted, so the XML
 * rendering needs none. Idempotent; re-running with another pod count
 * updates the limit.
 */
export const apiUserSql = (definition: ApiUserDefinition) => {
  const user = checkIdentifier('user', definition.user);
  const database = checkIdentifier('database', definition.database);
  const parent = checkIdentifier(
    'parent profile',
    definition.parentProfile ?? 'default'
  );
  const profile = checkIdentifier(
    'profile',
    definition.profile ?? apiProfileName
  );
  const limit = apiMaxConcurrentQueries(definition.backstop);
  const hash = sha256Hex(definition.password);
  return [
    `CREATE SETTINGS PROFILE OR REPLACE ${profile} SETTINGS INHERIT '${parent}', readonly = 2, max_concurrent_queries_for_user = ${limit} CONST`,
    `CREATE USER IF NOT EXISTS ${user} IDENTIFIED WITH sha256_hash BY '${hash}'`,
    `ALTER USER ${user} IDENTIFIED WITH sha256_hash BY '${hash}' SETTINGS PROFILE '${profile}'`,
    `GRANT SELECT ON ${database}.* TO ${user}`,
    `GRANT SELECT ON system.processes TO ${user}`,
    `GRANT SELECT ON system.projections TO ${user}`,
    `GRANT SELECT ON system.query_log TO ${user}`,
  ];
};
