/* eslint-disable @typescript-eslint/no-magic-numbers, @typescript-eslint/naming-convention, functional/no-loop-statement, no-await-in-loop, complexity, functional/no-let */
// cspell:ignore clickhouse txid
/**
 * Inputs read by internal id alone must be served by input.p_txid
 * (fix-pass-9.md §1). The base table is ordered by transaction hash: an
 * id-only read it has to serve scans every row (golden X6 after 8b: 1.04 B
 * rows). `input` gets filler rows in several parts (ids no document reads),
 * so a scan is visible in read_rows; each document that reads inputs then
 * runs and every statement must
 *   - read far fewer rows than the filler, and
 *   - when it reads inputs by id alone, use `input.p_txid`
 *     (system.query_log `projections`).
 */
import test from 'ava';

import { e2eClickHouseUrl } from '../store/clickhouse/test-support.js';

import { documentText } from './catalogue.js';
import { apiQueryIdPrefix } from './db.js';
import type { Env } from './spec-env.js';
import { one, recordQueries, setup } from './spec-env.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

/** Filler input rows (4 inserts: 4 parts). */
const fillerParts = 4;
const fillerRowsPerPart = 100_000;
const fillerRows = fillerParts * fillerRowsPerPart;

/**
 * An inputs read by id alone: `input_at`'s view, then the id set with no
 * hash set before it (model.ts, inputs `source`); as compiled, or as sent
 * (lean-sql.ts: the view merged into the read, the link set read straight
 * from the level array).
 */
export const idOnlyInputsRead = (sql: string) =>
  /FROM input WHERE (?:(?!FROM )[^])*?\) WHERE transaction_internal_id IN \(SELECT internal_id FROM/u.test(
    sql
  ) ||
  /FROM input WHERE \((?:(?!FROM |transaction_hash)[^])*?\) AND \(+transaction_internal_id IN \(SELECT tupleElement\(arrayJoin\(/u.test(
    sql
  );

/**
 * An inputs root matched through the spend relation, read by the spends'
 * internal ids with the hash sets out of key analysis (roots.ts `runInputs`,
 * fix-pass-13.md §2); as compiled or as sent.
 */
export const rootInputsBySpendIds = (sql: string) =>
  /(?:WHERE|AND) \(+transaction_internal_id IN \(SELECT transaction_internal_id FROM /u.test(
    sql
  ) && sql.includes('identity((transaction_hash, input_index)) IN');

const documents: [string, string, boolean, boolean?][] = [
  /* [label, document, reads inputs by id alone, inputs root by spend ids] */
  [
    'inputs, key columns only',
    `query { transactions(node: "${one}", limit: 100) { nodes { hash inputs { input_index outpoint_transaction_hash outpoint_index sequence_number } } } }`,
    true,
  ],
  [
    'inputs with outpoint (L33 / L35 shape)',
    `query { transactions(node: "${one}", limit: 100) { nodes { hash inputs { outpoint_transaction_hash outpoint_index outpoint { value_satoshis token_category fungible_token_amount } } } } }`,
    true,
  ],
  [
    'filtered by input_index (L14 / L17 shape)',
    `query { transactions(node: "${one}", limit: 100) { nodes { hash inputs(where: { input_index: { _in: [0, 3, 4, 5] } }, order_by: { input_index: ASC }) { input_index outpoint { value_satoshis } } } } }`,
    true,
  ],
  [
    'filtered by outpoint (L22 / L30 shape)',
    `query { transactions(node: "${one}", limit: 100) { nodes { hash inputs(where: { outpoint: { value_satoshis: { _gt: "0" } } }, order_by: { input_index: ASC }) { input_index outpoint { value_satoshis } } } } }`,
    true,
  ],
  [
    'inputs root, nested transaction inputs (L17 shape)',
    `query { inputs(node: "${one}", limit: 100) { nodes { input_index transaction { hash inputs(where: { input_index: { _eq: 0 } }) { input_index outpoint { value_satoshis } } } } } }`,
    true,
  ],
  [
    'inputs root filtered by outpoint (L17 page)',
    `query { inputs(node: "${one}", limit: 100, where: { outpoint: { value_satoshis: { _gt: "0" } } }) { nodes { transaction_internal_id input_index transaction { hash } } } }`,
    false,
    true,
  ],
  [
    'inputs root filtered by outpoint, unlocking_bytecode: by hash',
    `query { inputs(node: "${one}", limit: 100, where: { outpoint: { value_satoshis: { _gt: "0" } } }) { nodes { input_index unlocking_bytecode } } }`,
    false,
    false,
  ],
  [
    'unlocking_bytecode (X6 shape): by hash and id',
    `query { transactions(node: "${one}", limit: 100) { nodes { hash inputs { input_index unlocking_bytecode sequence_number } } } }`,
    false,
  ],
  [
    'X6 GetTransactionDetails',
    documentText('X06-GetTransactionDetails.graphql'),
    false,
  ],
];

const statementsSince = async (env: Env, since: string) => {
  await env.client.command('SYSTEM FLUSH LOGS');
  return env.client.query<{
    projections: string[];
    query: string;
    read_rows: string;
  }>(
    `SELECT projections, query, toString(read_rows) AS read_rows FROM system.query_log
     WHERE type = 'QueryFinish' AND current_database = currentDatabase()
       AND startsWith(query_id, {prefix:String}) AND event_time_microseconds >= toDateTime64({since:String}, 6)
       AND query_kind = 'Select'
     ORDER BY event_time_microseconds`,
    { prefix: apiQueryIdPrefix, since }
  );
};

e2e(
  '[e2e] inputs read by internal id alone use input.p_txid; no statement scans input',
  async (t) => {
    t.timeout(300_000);
    const env = await setup(t, 'inputs_txid');
    const { seq } = (
      await env.client.query<{ seq: string }>(
        'SELECT toString(min(commit_seq)) AS seq FROM input'
      )
    )[0]!;
    for (let part = 0; part < fillerParts; part += 1) {
      /*
       * Ids no transaction has, each part's range spanning the dataset's
       * ids (0 and above 10^9: the parts' column statistics prune no part,
       * as on golden, whose input parts are chunked by outpoint hash), at a
       * committed commit_seq (no partition pruning hides a scan).
       */
      await env.client.command(
        `INSERT INTO input (transaction_hash, input_index, transaction_internal_id, outpoint_transaction_hash, outpoint_index, sequence_number, unlocking_bytecode, commit_seq)
         SELECT toFixedString(SHA256(concat('filler', toString(n))), 32), 0, if(n % ${fillerRowsPerPart} = 0, 0, 1000000000 + n),
                toFixedString(SHA256(concat('filler-out', toString(n))), 32), 0, 0, randomString(40), ${seq}
         FROM (SELECT number + ${
           part * fillerRowsPerPart
         } AS n FROM numbers(${fillerRowsPerPart}))`
      );
    }
    env.api.db.featureTtlMs = 0;
    t.true(
      (await env.api.db.storeFeatures()).inputsByIdProjection,
      'every part of input has p_txid'
    );
    const { hash } = await env
      .gql<{
        transactions: { nodes: { hash: string }[] };
      }>(
        `query { transactions(node: "${one}", limit: 100) { nodes { hash input_count } } }`
      )
      .then((data) => data.transactions.nodes.at(-1)!);
    const database = (
      await env.client.query<{ d: string }>('SELECT currentDatabase() AS d')
    )[0]!.d;
    for (const [label, document, byIdAlone, rootBySpendIds] of documents) {
      const { now } = (
        await env.client.query<{ now: string }>(
          "SELECT toString(now64(6, 'UTC')) AS now"
        )
      )[0]!;
      const recorder = recordQueries(env.api);
      const response = await env.gqlRaw(document, { hash, node: one });
      recorder.stop();
      t.is(response.errors, undefined, `${label}: no error`);
      const recorded = recorder.events.filter(
        (event) => event.kind === 'data' && event.sql.includes('FROM input')
      );
      t.true(recorded.length > 0, `${label}: reads input`);
      t.is(
        recorded.some((event) => idOnlyInputsRead(event.sql)),
        byIdAlone,
        `${label}: ${byIdAlone ? 'by id alone' : 'by hash and id'}`
      );
      if (rootBySpendIds !== undefined) {
        t.is(
          recorded.some((event) => rootInputsBySpendIds(event.sql)),
          rootBySpendIds,
          `${label}: ${rootBySpendIds ? 'root by spend ids' : 'root by hash'}`
        );
      }
      const logged = (await statementsSince(env, now)).filter((row) =>
        row.query.includes('FROM input')
      );
      t.true(logged.length > 0, `${label}: logged`);
      for (const row of logged) {
        t.true(
          Number(row.read_rows) < fillerRows / 4,
          `${label}: ${row.read_rows} rows read (input holds ${fillerRows} filler rows)`
        );
        if (idOnlyInputsRead(row.query) || rootInputsBySpendIds(row.query)) {
          t.true(
            row.projections.includes(`${database}.input.p_txid`),
            `${label}: input.p_txid used (${row.projections.join(', ')})`
          );
        }
      }
    }
  }
);
