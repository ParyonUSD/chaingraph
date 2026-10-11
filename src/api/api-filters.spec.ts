/* eslint-disable @typescript-eslint/no-magic-numbers, camelcase, @typescript-eslint/naming-convention, functional/no-loop-statement, functional/no-let, no-await-in-loop, @typescript-eslint/require-array-sort-compare, complexity, max-params, functional/no-throw-statement, sort-keys */
// cspell:ignore clickhouse
/**
 * Phase 2 G2 (docs/clickhouse-port/phase2-api.md): every scalar operator
 * (R9), the boolean combinators incl. NOT EXISTS and OR of EXISTS (R10),
 * relationship filters to depth 5 bound to one row (R11), bytes in and out
 * (R12), byte-prefix filters (R13) and locking-bytecode sets (R14) on the
 * two-node scratch dataset. Each filter's result equals the independent
 * evaluator of spec-oracle.ts (raw rows by direct SQL, SQL three-valued
 * logic in JavaScript), on both nodes.
 */
import test from 'ava';

import { p2pkh } from '../store/clickhouse/spec-fixtures.js';
import { e2eClickHouseUrl } from '../store/clickhouse/test-support.js';

import {
  holder1,
  holder3,
  loanScript,
  paryon,
  priceScript,
  redeemer,
  user1,
  user2,
} from './spec-dataset.js';
import type { Env } from './spec-env.js';
import { one, recordQueries, setup, two } from './spec-env.js';
import type { Oracle } from './spec-oracle.js';
import {
  accepted,
  evalInput,
  evalOutput,
  evalTransaction,
  loadOracle,
} from './spec-oracle.js';
import type { InputFilter, OutputFilter, TransactionFilter } from './sql.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const sorted = (values: readonly string[]) => [...values].sort();

/** Every page of a root (keys of its rows). */
const walk = async (
  env: Env,
  root: 'inputs' | 'outputs' | 'transactions' | 'unspent_outputs',
  node: string,
  where: unknown
) => {
  const select =
    root === 'transactions'
      ? 'hash'
      : root === 'inputs'
      ? 'transaction_hash input_index'
      : 'transaction_hash output_index';
  const keys: string[] = [];
  let after: string | null = null;
  for (let page = 0; page < 50; page += 1) {
    const result: {
      [key: string]: {
        has_more: boolean;
        end_cursor: string | null;
        nodes: { [key: string]: unknown }[];
      };
    } = await env.gql(
      `query($node: String!, $where: ${
        root === 'transactions'
          ? 'TransactionFilter'
          : root === 'inputs'
          ? 'InputFilter'
          : 'OutputFilter'
      }, $after: ${root === 'transactions' ? 'BigInt' : 'String'}) {
        r: ${root}(node: $node, limit: 7, after: $after, where: $where) { has_more end_cursor nodes { ${select} } }
      }`,
      { after, node, where }
    );
    result.r!.nodes.forEach((row) => {
      keys.push(
        root === 'transactions'
          ? String(row.hash)
          : `${String(row.transaction_hash)}:${String(
              row.output_index ?? row.input_index
            )}`
      );
    });
    if (!result.r!.has_more) return keys;
    after = result.r!.end_cursor;
  }
  throw new Error('too many pages');
};

const outputFilters = (c0: string): OutputFilter[] => [
  /* no condition: the unfiltered unspent form (anti-join over every spend) */
  {},
  { output_index: { _eq: 1 } },
  { output_index: { _in: [0, 2, 4] }, value_satoshis: { _gte: 800n } },
  { output_index: { _nin: [0, 1, 2, 3] }, token_category: { _eq: paryon } },
  { value_satoshis: { _lt: 1_000n } },
  { value_satoshis: { _gt: 1_000n } },
  { value_satoshis: { _neq: 1_000n }, output_index: { _lte: 20 } },
  { locking_bytecode: { _eq: loanScript } },
  { locking_bytecode: { _in: [user1, user2] } },
  { locking_bytecode: { _nin: [user1, user2, loanScript] } },
  { locking_bytecode: { _prefix: 'a914' } },
  { locking_bytecode: { _prefix_in: ['76a9', '6a'] } },
  { locking_bytecode: { _gt: user1, _lte: user2 } },
  { locking_bytecode: { _neq: '6a' } },
  { token_category: { _is_null: true } },
  {
    token_category: { _is_null: false },
    nonfungible_token_capability: { _is_null: true },
  },
  { token_category: { _in: [paryon, redeemer] } },
  { token_category: { _neq: paryon } },
  { nonfungible_token_capability: { _eq: 'mutable' } },
  { nonfungible_token_capability: { _neq: 'mutable' } },
  { _not: { nonfungible_token_capability: { _eq: 'mutable' } } },
  { nonfungible_token_capability: { _in: ['none', 'minting'] } },
  { nonfungible_token_capability: { _nin: ['none'] } },
  { _not: { nonfungible_token_capability: { _in: ['none'] } } },
  { nonfungible_token_commitment: { _prefix: '01' } },
  { nonfungible_token_commitment: { _prefix_in: ['01', 'c1'] } },
  { _not: { nonfungible_token_commitment: { _prefix: '01' } } },
  {
    nonfungible_token_commitment: { _eq: '03' },
    token_category: { _eq: paryon },
  },
  { nonfungible_token_commitment: { _gte: '01', _lt: '02' } },
  { nonfungible_token_commitment: { _is_null: false, _neq: '03' } },
  { fungible_token_amount: { _gt: 50n } },
  { fungible_token_amount: { _is_null: true } },
  { fungible_token_amount: { _is_null: false } },
  { _not: { fungible_token_amount: { _in: [100n, 200n] } } },
  { transaction_hash: { _eq: c0 }, output_index: { _lte: 3 } },
  { transaction_hash: { _neq: c0 } },
  { transaction_internal_id: { _gt: -1n } },
  { transaction: { is_coinbase: { _eq: true } } },
  { transaction: { in_block: { height: { _eq: 1 } } } },
  { transaction: { in_mempool: true } },
  {
    spent_by: {
      transaction: { has_output: { token_category: { _eq: paryon } } },
    },
  },
  { _not: { spent_by: {} } },
  /* opt-in acceptance of the spend: the level's node accepts the spending transaction */
  { _not: { spent_by: { transaction: { block_window: {} } } } },
  { spent_by: { transaction: { block_window: { include_mempool: false } } } },
  {
    _and: [
      { token_category: { _eq: paryon } },
      {
        _or: [
          { nonfungible_token_capability: { _eq: 'mutable' } },
          { fungible_token_amount: { _gte: 100n } },
        ],
      },
    ],
  },
  {
    _or: [
      { locking_bytecode: { _eq: holder1 } },
      { locking_bytecode: { _eq: holder3 } },
    ],
  },
];

const transactionFilters = (hashes: readonly string[]): TransactionFilter[] => [
  {
    has_output: {
      nonfungible_token_capability: { _eq: 'mutable' },
      token_category: { _eq: paryon },
    },
  },
  { has_input: { outpoint: { locking_bytecode: { _eq: loanScript } } } },
  { _not: { has_output: { token_category: { _is_null: false } } } },
  {
    _or: [
      { has_output: { locking_bytecode: { _eq: user1 } } },
      { has_input: { outpoint: { locking_bytecode: { _eq: user2 } } } },
    ],
  },
  { input_count: { _gte: 4 }, is_coinbase: { _eq: false } },
  { is_coinbase: { _neq: true } },
  { block_window: { from_height: 1, to_height: 2 } },
  { block_window: { from_height: 1, include_mempool: false } },
  { block_window: { to_height: 1 } },
  { in_block: { height: { _in: [0, 2] } } },
  { in_block: { timestamp: { _gt: 1_600_000_000n } } },
  { in_mempool: true },
  { in_mempool: false, is_coinbase: { _eq: false } },
  { hash: { _in: hashes } },
  { hash: { _nin: hashes } },
  { output_count: { _gt: 1 }, output_value_satoshis: { _lte: 2_000n } },
  { internal_id: { _gt: -1n } },
  { internal_id: { _lt: 0n } },
  { locktime: { _eq: 0n }, size_bytes: { _gt: 105 }, version: { _eq: 2 } },
  // depth 5: input → spent output → its transaction → an output → its spender's transaction
  {
    has_input: {
      outpoint: {
        transaction: {
          has_output: {
            spent_by: {
              transaction: {
                has_output: { token_category: { _eq: redeemer } },
              },
            },
          },
        },
      },
    },
  },
  // same row (input 4 is a user1 coin) vs two independent EXISTS
  {
    has_input: {
      input_index: { _eq: 4 },
      outpoint: { locking_bytecode: { _eq: user1 } },
    },
  },
  {
    _and: [
      { has_input: { input_index: { _eq: 4 } } },
      { has_input: { outpoint: { locking_bytecode: { _eq: user2 } } } },
    ],
  },
  {
    has_input: {
      input_index: { _eq: 4 },
      outpoint: { locking_bytecode: { _eq: user2 } },
    },
  },
  {
    has_input: {
      _or: [
        { outpoint: { token_category: { _eq: redeemer } } },
        { input_index: { _gt: 3 } },
      ],
    },
  },
  { has_input: { _not: { outpoint: { token_category: { _is_null: true } } } } },
  {
    has_input: { outpoint: { token_category: { _eq: paryon } } },
    _not: { has_output: { locking_bytecode: { _eq: loanScript } } },
  },
];

const inputFilters = (c0: string): InputFilter[] => [
  { input_index: { _in: [0, 4] } },
  { outpoint: { token_category: { _eq: paryon } } },
  { outpoint_transaction_hash: { _eq: c0 }, outpoint_index: { _lte: 10n } },
  { sequence_number: { _eq: 4_294_967_294n } },
  { transaction: { has_output: { locking_bytecode: { _eq: holder3 } } } },
  { _not: { outpoint: { token_category: { _is_null: false } } } },
  { outpoint: { locking_bytecode: { _in: [user1, priceScript] } } },
  {
    _or: [
      { outpoint: { nonfungible_token_commitment: { _prefix: '01' } } },
      { input_index: { _eq: 1 } },
    ],
  },
];

/** GraphQL variables: enum values upper-case, bigints as strings. */
const variables = (value: unknown): unknown =>
  JSON.parse(
    JSON.stringify(value, (key, item: unknown) =>
      typeof item === 'bigint'
        ? item.toString()
        : key.endsWith('capability') &&
          item !== null &&
          typeof item === 'object'
        ? Object.fromEntries(
            Object.entries(item).map(([operator, operand]) => [
              operator,
              Array.isArray(operand)
                ? operand.map((entry) => String(entry).toUpperCase())
                : typeof operand === 'string'
                ? operand.toUpperCase()
                : operand,
            ])
          )
        : item
    )
  );

const expectedOutputs = (oracle: Oracle, filter: OutputFilter) =>
  oracle.outputs
    .filter(
      (output) =>
        accepted(oracle, output.transaction_hash) &&
        evalOutput(oracle, output, filter) === true
    )
    .map((output) => `${output.transaction_hash}:${output.output_index}`);

e2e(
  '[e2e] G2 R9–R14: output, transaction and input filters (every operator, three-valued NOT, NOT EXISTS, OR of EXISTS, same-row, depth 5, prefixes, sets) equal the independent evaluator on both nodes',
  async (t) => {
    const env = await setup(t, 'g2_filters');
    const c0 = env.data.c0.hash;
    const hashes = [
      env.data.closeA.hash,
      env.data.fin2.hash,
      env.data.redeemB.hash,
    ];
    let checked = 0;
    for (const node of [one, two]) {
      const oracle = await loadOracle(
        env.client,
        node === one ? env.nodes.node1 : env.nodes.node2
      );
      for (const filter of outputFilters(c0)) {
        const actual = await walk(env, 'outputs', node, variables(filter));
        t.deepEqual(
          sorted(actual),
          sorted(expectedOutputs(oracle, filter)),
          `${node} outputs ${JSON.stringify(variables(filter))}`
        );
        checked += 1;
        /* unspent at query time: every path (no filter, selective NOT IN, the anti-join forms) */
        const spent = new Set(
          oracle.inputs
            .filter((input) => accepted(oracle, input.transaction_hash))
            .map(
              (input) =>
                `${input.outpoint_transaction_hash}:${input.outpoint_index}`
            )
        );
        t.deepEqual(
          sorted(await walk(env, 'unspent_outputs', node, variables(filter))),
          sorted(
            expectedOutputs(oracle, filter).filter((key) => !spent.has(key))
          ),
          `${node} unspent_outputs ${JSON.stringify(variables(filter))}`
        );
        checked += 1;
      }
      for (const filter of transactionFilters(hashes)) {
        const actual = await walk(env, 'transactions', node, variables(filter));
        const expected = [...oracle.transactions.values()]
          .filter(
            (transaction) =>
              accepted(oracle, transaction.hash) &&
              evalTransaction(oracle, transaction, filter) === true
          )
          .map((transaction) => transaction.hash);
        t.deepEqual(
          sorted(actual),
          sorted(expected),
          `${node} transactions ${JSON.stringify(variables(filter))}`
        );
        checked += 1;
      }
      for (const filter of inputFilters(c0)) {
        const actual = await walk(env, 'inputs', node, variables(filter));
        const expected = oracle.inputs
          .filter(
            (input) =>
              accepted(oracle, input.transaction_hash) &&
              evalInput(oracle, input, filter) === true
          )
          .map((input) => `${input.transaction_hash}:${input.input_index}`);
        t.deepEqual(
          sorted(actual),
          sorted(expected),
          `${node} inputs ${JSON.stringify(variables(filter))}`
        );
        checked += 1;
      }
    }
    t.log(`${checked} filters checked against the evaluator`);
    // some filters must actually select rows (not an all-empty pass)
    const nonEmpty = await walk(env, 'outputs', one, {
      locking_bytecode: { _prefix: 'a914' },
    });
    t.true(nonEmpty.length > 10);
  }
);

e2e(
  '[e2e] G2 R12 + R14: bytes in and out (validation, case, literals and variables) and locking-bytecode sets chunked at 1,000 per parameter',
  async (t) => {
    const env = await setup(t, 'g2_bytes');
    // invalid hex is refused by the scalar; a hash of the wrong length by the compiler
    for (const bad of ['abc', 'zz', '\\x6a', '0x6a']) {
      const result = await env.gqlRaw(
        `query($v: Hex!) { outputs(node: "node-one", limit: 1, where: { locking_bytecode: { _eq: $v } }) { has_more } }`,
        { v: bad }
      );
      t.truthy(result.errors?.[0]?.message, bad);
    }
    const short = await env.gqlRaw(
      `{ outputs(node: "node-one", limit: 1, where: { token_category: { _eq: "aa" } }) { has_more } }`
    );
    t.regex(short.errors?.[0]?.message ?? '', /32 bytes/u);
    // upper-case input equals lower-case; output is lower-case without prefix; literal = variable
    const literal = await env.gql<{
      outputs: {
        nodes: { locking_bytecode: string; token_category: string }[];
      };
    }>(
      `{ outputs(node: "node-one", limit: 100, where: { locking_bytecode: { _eq: "${loanScript.toUpperCase()}" } }) { nodes { locking_bytecode token_category } } }`
    );
    const variable = await env.gql<typeof literal>(
      `query($v: [Hex!]!) { outputs(node: "node-one", limit: 100, where: { locking_bytecode: { _in: $v } }) { nodes { locking_bytecode token_category } } }`,
      { v: [loanScript] }
    );
    t.deepEqual(literal, variable);
    t.true(literal.outputs.nodes.length > 0);
    literal.outputs.nodes.forEach((row) => {
      t.regex(row.locking_bytecode, /^[0-9a-f]+$/u);
      t.is(row.locking_bytecode, loanScript);
    });
    // 2,500 locking bytecodes (2,497 unknown): bound in chunks of 1,000; the result equals the 3 real ones
    const unknown = Array.from({ length: 2_497 }, (_, index) =>
      p2pkh(`unknown-${index}`)
    );
    const recorder = recordQueries(env.api);
    const set = await walk(env, 'outputs', one, {
      locking_bytecode: { _in: [...unknown, user1, user2, holder1] },
    });
    recorder.stop();
    const oracle = await loadOracle(env.client, env.nodes.node1);
    t.deepEqual(
      sorted(set),
      sorted(
        expectedOutputs(oracle, {
          locking_bytecode: { _in: [user1, user2, holder1] },
        })
      )
    );
    const first = recorder.events.find((event) => event.kind === 'data')!;
    const lists = Object.values(first.params).filter(
      (value) => Array.isArray(value) && value.length > 100
    ) as unknown[][];
    t.true(lists.length >= 3, 'chunked');
    t.true(lists.every((list) => list.length <= 1_000));
  }
);
