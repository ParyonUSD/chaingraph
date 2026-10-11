/* eslint-disable @typescript-eslint/no-magic-numbers, camelcase, @typescript-eslint/naming-convention, functional/no-loop-statement, functional/no-let, no-await-in-loop, @typescript-eslint/require-array-sort-compare, complexity, max-params, functional/no-throw-statement */
// cspell:ignore clickhouse
/**
 * Phase 2 G3 (docs/clickhouse-port/phase2-api.md): keyset pagination in
 * every form (R15), offset (R16), ordering at the root, through object
 * relationships and on nested lists with variable direction (R17), limits
 * incl. nested and top-1 (R18), nested arguments and aliases (R19), the
 * relationship edges not covered elsewhere (R20: block → transactions →
 * transaction/block), multiple roots, root aliases, named operations,
 * variables, fragments and @include/@skip (R21, R22). Orders are checked
 * against the raw rows sorted in JavaScript (spec-oracle.ts), not against the
 * API.
 */
import test from 'ava';

import { e2eClickHouseUrl } from '../store/clickhouse/test-support.js';

import { paryon } from './spec-dataset.js';
import type { Env } from './spec-env.js';
import { one, setup, two } from './spec-env.js';
import type { Oracle, OracleOutput } from './spec-oracle.js';
import { accepted, loadOracle } from './spec-oracle.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

interface Row {
  [key: string]: unknown;
}

/** Walk every page of `field` (`after` = end_cursor, or `cursor` for an order_by transactions page). */
const walk = async (
  env: Env,
  document: string,
  variables: { [name: string]: unknown },
  cursorVariable = 'after'
) => {
  const rows: Row[] = [];
  let cursor: string | null = null;
  let pages = 0;
  for (;;) {
    const result: {
      r: { has_more: boolean; end_cursor: string | null; nodes: Row[] };
    } = await env.gql(document, { ...variables, [cursorVariable]: cursor });
    rows.push(...result.r.nodes);
    pages += 1;
    if (!result.r.has_more) return { pages, rows };
    cursor = result.r.end_cursor;
    if (pages > 200) throw new Error('too many pages');
  }
};

/** Compare by a list of [value, direction] keys (bigints, numbers or strings). */
const byKeys =
  <T>(keys: (row: T) => [bigint | number | string, 'ASC' | 'DESC'][]) =>
  (a: T, b: T) => {
    const ka = keys(a);
    const kb = keys(b);
    for (let index = 0; index < ka.length; index += 1) {
      const [x, direction] = ka[index]!;
      const [y] = kb[index]!;
      if (x !== y) {
        const less = x < y ? -1 : 1;
        return direction === 'ASC' ? less : -less;
      }
    }
    return 0;
  };

const outputKey = (output: OracleOutput) =>
  `${output.transaction_hash}:${output.output_index}`;

const acceptedOutputs = (oracle: Oracle) =>
  oracle.outputs.filter((output) => accepted(oracle, output.transaction_hash));

e2e(
  '[e2e] G3 R15 + R16 + R17: keyset walks (every root, default and mixed-direction orders, through relationships), offset pages and variable direction equal the raw rows sorted in JS',
  async (t) => {
    const env = await setup(t, 'g3_order');
    // more rows: two blocks of node-one-only transactions
    await env.extendNodeOne([]);
    await env.extendNodeOne([]);
    for (const node of [one, two]) {
      const oracle = await loadOracle(
        env.client,
        node === one ? env.nodes.node1 : env.nodes.node2
      );
      const transactions = [...oracle.transactions.values()].filter((tx) =>
        accepted(oracle, tx.hash)
      );
      // transactions, default order both directions, page size 2
      for (const order of ['ASC', 'DESC'] as const) {
        const { rows, pages } = await walk(
          env,
          `query($node: String!, $order: Order!, $after: BigInt) {
            r: transactions(node: $node, limit: 2, order: $order, ${
              order === 'ASC' ? 'after: $after' : 'before: $after'
            }) { has_more end_cursor nodes { hash internal_id } }
          }`,
          { node, order }
        );
        t.true(pages > 2);
        t.deepEqual(
          rows.map((row) => row.hash),
          [...transactions]
            .sort(byKeys((tx) => [[tx.internal_id, order]]))
            .map((tx) => tx.hash),
          `${node} transactions ${order}`
        );
      }
      // transactions, mixed-direction order_by with an opaque cursor
      const mixed = await walk(
        env,
        `query($node: String!, $cursor: String) {
          r: transactions(node: $node, limit: 3, cursor: $cursor,
            order_by: [{ output_count: DESC }, { size_bytes: ASC }, { hash: DESC }]) { has_more end_cursor nodes { hash } }
        }`,
        { node },
        'cursor'
      );
      t.deepEqual(
        mixed.rows.map((row) => row.hash),
        [...transactions]
          .sort(
            byKeys((tx) => [
              [tx.output_count, 'DESC'],
              [tx.size_bytes, 'ASC'],
              [tx.hash, 'DESC'],
              [tx.internal_id, 'ASC'],
            ])
          )
          .map((tx) => tx.hash),
        `${node} transactions mixed order_by`
      );
      // offset pages (R16) of the same order equal slices
      const ordered = mixed.rows.map((row) => row.hash);
      for (const offset of [0, 3, 7]) {
        const page = await env.gql<{ transactions: { nodes: Row[] } }>(
          `query($node: String!, $offset: Int) {
            transactions(node: $node, limit: 4, offset: $offset,
              order_by: [{ output_count: DESC }, { size_bytes: ASC }, { hash: DESC }]) { nodes { hash } }
          }`,
          { node, offset }
        );
        t.deepEqual(
          page.transactions.nodes.map((row) => row.hash),
          ordered.slice(offset, offset + 4),
          `${node} offset ${offset}`
        );
      }
      // outputs: order through the transaction relationship + nested keys (L12's order), and a value order
      const outputs = acceptedOutputs(oracle);
      const through = await walk(
        env,
        `query($node: String!, $after: String) {
          r: outputs(node: $node, limit: 5, after: $after,
            order_by: [{ transaction: { internal_id: DESC } }, { output_index: ASC }]) {
            has_more end_cursor nodes { transaction_hash output_index }
          }
        }`,
        { node }
      );
      t.deepEqual(
        through.rows.map(
          (row) => `${String(row.transaction_hash)}:${String(row.output_index)}`
        ),
        [...outputs]
          .sort(
            byKeys((output) => [
              [output.transaction_internal_id, 'DESC'],
              [output.output_index, 'ASC'],
            ])
          )
          .map(outputKey),
        `${node} outputs through transaction`
      );
      const byValue = await walk(
        env,
        `query($node: String!, $after: String, $direction: Order!) {
          r: outputs(node: $node, limit: 4, after: $after, where: { token_category: { _eq: "${paryon}" } },
            order_by: [{ value_satoshis: $direction }, { locking_bytecode: ASC }]) {
            has_more end_cursor nodes { transaction_hash output_index }
          }
        }`,
        { direction: 'DESC', node }
      );
      t.deepEqual(
        byValue.rows.map(
          (row) => `${String(row.transaction_hash)}:${String(row.output_index)}`
        ),
        outputs
          .filter((output) => output.token_category === paryon)
          .sort(
            byKeys((output) => [
              [output.value_satoshis, 'DESC'],
              [output.locking_bytecode, 'ASC'],
              [output.transaction_internal_id, 'ASC'],
              [output.output_index, 'ASC'],
            ])
          )
          .map(outputKey),
        `${node} outputs by value (variable direction)`
      );
      // inputs: composite default keyset and a custom order
      const inputs = oracle.inputs.filter((input) =>
        accepted(oracle, input.transaction_hash)
      );
      const inputWalk = await walk(
        env,
        `query($node: String!, $after: String) {
          r: inputs(node: $node, limit: 3, after: $after, order_by: [{ outpoint_index: DESC }]) {
            has_more end_cursor nodes { transaction_hash input_index }
          }
        }`,
        { node }
      );
      t.deepEqual(
        inputWalk.rows.map(
          (row) => `${String(row.transaction_hash)}:${String(row.input_index)}`
        ),
        [...inputs]
          .sort(
            byKeys((input) => [
              [input.outpoint_index, 'DESC'],
              [input.transaction_internal_id, 'ASC'],
              [input.input_index, 'ASC'],
            ])
          )
          .map((input) => `${input.transaction_hash}:${input.input_index}`),
        `${node} inputs by outpoint_index desc`
      );
      // unspent outputs (default keyset) and blocks (height desc)
      const unspent = await walk(
        env,
        `query($node: String!, $after: String) {
          r: unspent_outputs(node: $node, limit: 4, after: $after) { has_more end_cursor nodes { transaction_hash output_index } }
        }`,
        { node }
      );
      const spent = new Set(
        inputs.map(
          (input) =>
            `${input.outpoint_transaction_hash}:${input.outpoint_index}`
        )
      );
      t.deepEqual(
        unspent.rows.map(
          (row) => `${String(row.transaction_hash)}:${String(row.output_index)}`
        ),
        outputs
          .filter((output) => !spent.has(outputKey(output)))
          .sort(
            byKeys((output) => [
              [output.transaction_internal_id, 'ASC'],
              [output.output_index, 'ASC'],
            ])
          )
          .map(outputKey),
        `${node} unspent outputs`
      );
      const blocks = await walk(
        env,
        `query($node: String!, $after: String) {
          r: blocks(node: $node, limit: 1, after: $after, order_by: { height: DESC }) { has_more end_cursor nodes { height } }
        }`,
        { node }
      );
      const heights = [
        ...new Set(
          [...oracle.acceptance.values()]
            .flat()
            .filter((block) => block !== 0n)
            .map((block) => Number(oracle.blocks.get(block)!.height))
        ),
      ].sort((a, b) => b - a);
      t.deepEqual(
        blocks.rows.map((row) => row.height),
        heights,
        `${node} blocks by height desc`
      );
    }
  }
);

e2e(
  '[e2e] G3 R18 + R19 + R20 + R21 + R22: nested limits, offsets, orders and filters; aliases of one relationship; block → transactions edges; multiple and aliased roots; named operations, fragments, @include/@skip',
  async (t) => {
    const env = await setup(t, 'g3_nested');
    const oracle = await loadOracle(env.client, env.nodes.node1);
    const c0 = env.data.c0.hash;
    // nested relationship arguments and aliases (R18, R19)
    const nested = await env.gql<{
      transactions_by_hash: {
        top: { output_index: number }[];
        second: { output_index: number }[];
        paryon: { output_index: number }[];
        last_input: { input_index: number }[];
        none: { output_index: number }[];
      }[];
    }>(
      `query($hashes: [Hex!]!) {
        transactions_by_hash(hashes: $hashes) {
          top: outputs(limit: 2, order_by: { value_satoshis: DESC }) { output_index }
          second: outputs(limit: 2, offset: 2, order: DESC) { output_index }
          paryon: outputs(where: { token_category: { _eq: "${paryon}" }, nonfungible_token_capability: { _eq: MUTABLE } }) { output_index }
          last_input: inputs(order: DESC, limit: 1) { input_index }
          none: outputs(limit: 0) { output_index }
        }
      }`,
      { hashes: [c0] }
    );
    const c0Outputs = oracle.outputsByTransaction.get(c0)!;
    const [row] = nested.transactions_by_hash;
    t.deepEqual(
      row!.top.map((output) => output.output_index),
      [...c0Outputs]
        .sort(
          byKeys((output) => [
            [output.value_satoshis, 'DESC'],
            [output.output_index, 'ASC'],
          ])
        )
        .slice(0, 2)
        .map((output) => output.output_index)
    );
    const indexes = c0Outputs
      .map((output) => output.output_index)
      .sort((a, b) => b - a);
    t.deepEqual(
      row!.second.map((output) => output.output_index),
      indexes.slice(2, 4)
    );
    t.deepEqual(
      row!.paryon.map((output) => output.output_index),
      c0Outputs
        .filter(
          (output) =>
            output.token_category === paryon &&
            output.nonfungible_token_capability === 'mutable'
        )
        .map((output) => output.output_index)
        .sort((a, b) => a - b)
    );
    t.deepEqual(row!.last_input, [{ input_index: 0 }]);
    t.deepEqual(row!.none, []);

    // block → transactions → transaction / block (R20 edge 11), nested where/order/limit
    const block = await env.gql<{
      blocks: {
        nodes: {
          height: number;
          transactions: {
            transaction_index: number;
            transaction: { hash: string };
            block: { height: number };
          }[];
          last: { transaction_index: number }[];
          with_tokens: { transaction_hash: string }[];
        }[];
      };
    }>(`{
      blocks(node: "node-one", limit: 1, where: { height: { _eq: 1 } }) {
        nodes {
          height
          transactions { transaction_index transaction { hash } block { height } }
          last: transactions(order_by: { transaction_index: DESC }, limit: 1) { transaction_index }
          with_tokens: transactions(where: { transaction: { has_output: { token_category: { _is_null: false } } } }) { transaction_hash }
        }
      }
    }`);
    const [one1] = block.blocks.nodes;
    t.deepEqual(
      one1!.transactions.map((entry) => entry.transaction.hash),
      env.data.block1.transactions.map((tx) => tx.hash)
    );
    t.deepEqual(
      one1!.transactions.map((entry) => entry.transaction_index),
      env.data.block1.transactions.map((_, index) => index)
    );
    t.true(one1!.transactions.every((entry) => entry.block.height === 1));
    t.deepEqual(one1!.last, [
      { transaction_index: env.data.block1.transactions.length - 1 },
    ]);
    t.deepEqual(
      one1!.with_tokens.map((entry) => entry.transaction_hash).sort(),
      env.data.block1.transactions
        .filter((tx) =>
          tx.outputs.some((output) => output.tokenCategory !== undefined)
        )
        .map((tx) => tx.hash)
        .sort()
    );

    // R21 + R22: several operations in one document, a named one selected; root aliases; fragments; @include/@skip
    const document = `
      fragment Out on Output { transaction_hash output_index }
      query First($node: String!) { transactions(node: $node, limit: 1) { has_more } }
      query Second($node: String!, $withTip: Boolean!, $skipNodes: Boolean!, $id: Int!) {
        a: unspent_outputs(node: $node, limit: 2) { nodes { ...Out } }
        b: unspent_outputs(node: $node, limit: 2, after: null, where: { output_index: { _eq: $id } }) { nodes { ...Out transaction @include(if: $withTip) { hash } } }
        tip: block_tip(node: $node) @include(if: $withTip) { height }
        n: nodes @skip(if: $skipNodes) { name }
      }`;
    const response = await fetch(env.server.url, {
      body: JSON.stringify({
        operationName: 'Second',
        query: document,
        variables: { id: 0, node: one, skipNodes: true, withTip: false },
      }),
      headers: { 'content-type': 'application/json' },
      method: 'POST',
    });
    const body = (await response.json()) as {
      data: {
        a: { nodes: Row[] };
        b: { nodes: Row[] };
        tip?: unknown;
        n?: unknown;
      };
    };
    t.deepEqual(Object.keys(body.data).sort(), ['a', 'b']);
    t.is(body.data.a.nodes.length, 2);
    t.true(body.data.b.nodes.every((output) => output.output_index === 0));
    t.true(body.data.b.nodes.every((output) => !('transaction' in output)));
    const included = await env.gql<{
      b: { nodes: { transaction: { hash: string } }[] };
      tip: { height: number };
    }>(
      `query($node: String!) {
        b: unspent_outputs(node: $node, limit: 1) { nodes { transaction @include(if: true) { hash } } }
        tip: block_tip(node: $node) @include(if: true) { height }
      }`,
      { node: one }
    );
    t.is(included.tip.height, 2);
    t.is(included.b.nodes[0]!.transaction.hash.length, 64);

    // limits: 0 is an empty page with has_more; above the cap and negative offsets are refused (R33)
    const empty = await env.gql<{
      transactions: { nodes: Row[]; has_more: boolean };
    }>(
      `{ transactions(node: "node-one", limit: 0) { has_more nodes { hash } } }`
    );
    t.deepEqual(empty.transactions, { has_more: true, nodes: [] });
    for (const bad of [
      `{ transactions(node: "node-one", limit: 10001) { has_more } }`,
      `{ outputs(node: "node-one", limit: 5001) { has_more } }`,
      `{ inputs(node: "node-one", limit: 1001) { has_more } }`,
      `{ transactions(node: "node-one", limit: 1, offset: -1) { has_more } }`,
      `{ transactions(node: "node-one", limit: 1) { nodes { outputs(limit: 100001) { output_index } } } }`,
      `{ outputs(node: "node-one", limit: 1, after: "not-a-cursor") { has_more } }`,
    ]) {
      const result = await env.gqlRaw(bad);
      t.truthy(result.errors?.[0]?.message, bad);
    }
  }
);
