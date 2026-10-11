/* eslint-disable @typescript-eslint/no-magic-numbers, camelcase, @typescript-eslint/naming-convention, functional/no-loop-statement, functional/no-let, no-await-in-loop, @typescript-eslint/require-array-sort-compare */
// cspell:ignore clickhouse
/**
 * Phase 2 G1 (docs/clickhouse-port/phase2-api.md): per-node acceptance facts
 * as data (R5), the per-node mempool root (R6), block roots (R7), the
 * node-agnostic lookups and the id watermark (R8, R14 chunking) and node
 * metadata (R35), on the two-node scratch dataset. Every result is compared
 * with direct SQL on the store's convenience views (`*_v`, their own
 * snapshot subqueries: an independent path from the API's pinned views and
 * compiler).
 */
import { createHash } from 'node:crypto';

import test from 'ava';

import { makeTx, p2pkh } from '../store/clickhouse/spec-fixtures.js';
import { e2eClickHouseUrl } from '../store/clickhouse/test-support.js';

import type { Env } from './spec-env.js';
import { one, recordQueries, setup, two } from './spec-env.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const sorted = (values: readonly string[]) => [...values].sort();

const sql = async <T>(env: Env, statement: string) =>
  env.client.query<T>(statement, {});

const nodeId = (env: Env, name: string) =>
  name === one ? env.nodes.node1 : env.nodes.node2;

/** A second mempool transaction on node-one (validated after redeemB). */
const extraMempool = async (env: Env) => {
  const tx = makeTx({
    label: 'api-roots-mempool-2',
    outputs: [{ lockingBytecode: p2pkh('roots'), valueSatoshis: 500n }],
    spends: [[env.data.c0.hash, 22]],
  });
  await env.store.saveMempoolTransaction(tx, [
    {
      nodeInternalId: env.nodes.node1,
      validatedAt: new Date(Date.now() + 1_000),
    },
  ]);
  await env.store.publishWatermarks();
  return tx;
};

e2e(
  '[e2e] G1 R6: per-node mempool root ordered by validated_at, keyset pages, filters; equal to node_transaction_v',
  async (t) => {
    const env = await setup(t, 'g1_mempool');
    await extraMempool(env);
    const document = `query($node: String!, $after: String) {
      mempool(node: $node, limit: 1, after: $after,
              order_by: [{ validated_at: DESC }, { transaction_internal_id: DESC }]) {
        has_more end_cursor
        nodes { transaction_hash transaction_internal_id validated_at transaction { hash output_count } }
      }
    }`;
    for (const node of [one, two]) {
      const expected = await sql<{ hash: string; ms: string }>(
        env,
        `SELECT lower(hex(transaction_hash)) AS hash, toString(toUnixTimestamp64Milli(validated_at)) AS ms
         FROM node_transaction_v(node = ${nodeId(env, node)})
         ORDER BY validated_at DESC, transaction_internal_id DESC`
      );
      const rows: { transaction_hash: string; validated_at: string }[] = [];
      let after: string | null = null;
      let pages = 0;
      do {
        const page: {
          mempool: {
            has_more: boolean;
            end_cursor: string | null;
            nodes: {
              transaction_hash: string;
              validated_at: string;
              transaction: { hash: string };
            }[];
          };
        } = await env.gql(document, { after, node });
        page.mempool.nodes.forEach((row) => {
          t.is(row.transaction.hash, row.transaction_hash);
          rows.push(row);
        });
        after = page.mempool.has_more ? page.mempool.end_cursor : null;
        pages += 1;
      } while (after !== null && pages < 10);
      t.deepEqual(
        rows.map((row) => row.transaction_hash),
        expected.map((row) => row.hash),
        `${node}: mempool in validated_at order`
      );
      t.deepEqual(
        rows.map((row) => Date.parse(row.validated_at)),
        expected.map((row) => Number(row.ms))
      );
    }
    // filters: through the transaction, by hash, by validated_at
    const filtered = await env.gql<{
      mempool: { nodes: { transaction_hash: string }[] };
    }>(
      `query($hash: Hex!) {
        mempool(node: "node-one", limit: 10, where: { transaction: { has_output: { token_category: { _is_null: false } } }, transaction_hash: { _neq: $hash } }) { nodes { transaction_hash } }
      }`,
      { hash: '00'.repeat(32) }
    );
    t.deepEqual(
      filtered.mempool.nodes.map((row) => row.transaction_hash),
      [env.data.redeemB.hash]
    );
  }
);

e2e(
  '[e2e] G1 R7: block tip with previous hash, height window, checked heights, by height; per node; equal to node_block_v ⋈ block_v',
  async (t) => {
    const env = await setup(t, 'g1_blocks');
    const tips = await env.gql<{
      a: { height: number; hash: string; previous_block_hash: string };
      b: { height: number; hash: string; previous_block_hash: string };
    }>(`{
      a: block_tip(node: "node-one") { height hash previous_block_hash }
      b: block_tip(node: "node-two") { height hash previous_block_hash }
    }`);
    t.deepEqual(tips.a, {
      hash: env.data.block2.hash,
      height: 2,
      previous_block_hash: env.data.block1.hash,
    });
    t.deepEqual(tips.b, {
      hash: env.data.block1.hash,
      height: 1,
      previous_block_hash: env.data.block0.hash,
    });
    await env.extendNodeOne([]);
    const accepted = async (node: string) =>
      sql<{ hash: string; height: number; previous: string }>(
        env,
        `SELECT lower(hex(b.hash)) AS hash, b.height AS height, lower(hex(b.previous_block_hash)) AS previous
         FROM block_v AS b INNER JOIN node_block_v(node = ${nodeId(
           env,
           node
         )}) AS nb ON nb.block_internal_id = b.internal_id
         ORDER BY b.height`
      );
    for (const node of [one, two]) {
      const expected = await accepted(node);
      const all = await env.gql<{
        blocks: { nodes: { hash: string; height: number }[] };
        tip: { hash: string };
      }>(
        `query($node: String!) {
          blocks(node: $node, limit: 100) { nodes { hash height } }
          tip: block_tip(node: $node) { hash }
        }`,
        { node }
      );
      t.deepEqual(
        all.blocks.nodes,
        expected.map(({ hash, height }) => ({ hash, height })),
        `${node}: all accepted blocks by height`
      );
      t.is(all.tip.hash, expected[expected.length - 1]!.hash);
      const window = await env.gql<{
        range: { nodes: { height: number }[] };
        checked: { nodes: { height: number }[] };
        zero: { nodes: { height: number; hash: string }[] };
        desc: { nodes: { height: number }[]; has_more: boolean };
      }>(
        `query($node: String!) {
          range: blocks(node: $node, limit: 100, where: { height: { _gt: 0, _lte: 2 } }) { nodes { height } }
          checked: blocks(node: $node, limit: 100, where: { height: { _in: [0, 2, 9] } }) { nodes { height } }
          zero: blocks(node: $node, limit: 100, where: { height: { _eq: 0 } }) { nodes { height hash } }
          desc: blocks(node: $node, limit: 1, order_by: { height: DESC }) { has_more nodes { height } }
        }`,
        { node }
      );
      const heights = expected.map((row) => row.height);
      t.deepEqual(
        window.range.nodes.map((row) => row.height),
        heights.filter((height) => height > 0 && height <= 2)
      );
      t.deepEqual(
        window.checked.nodes.map((row) => row.height),
        heights.filter((height) => [0, 2, 9].includes(height))
      );
      t.deepEqual(window.zero.nodes, [{ hash: expected[0]!.hash, height: 0 }]);
      t.deepEqual(window.desc.nodes, [
        { height: heights[heights.length - 1]! },
      ]);
      t.true(window.desc.has_more);
    }
  }
);

e2e(
  '[e2e] G1 R5: block inclusions, accepted_by, node validations and the validation timeline as data, per node and per named node; one snapshot',
  async (t) => {
    const env = await setup(t, 'g1_facts');
    const second = await extraMempool(env);
    // confirm redeemB in a node-one block, sweep it into the history; expire the second mempool tx
    await env.extendNodeOne([env.data.redeemB]);
    await env.store.archiveMempoolTransactionsAcceptedByBlocks();
    const [secondRow] = await sql<{ id: string }>(
      env,
      `SELECT toString(internal_id) AS id FROM transaction_v WHERE hash = unhex('${second.hash}')`
    );
    await env.store.archiveMempoolTransaction({
      nodeInternalId: env.nodes.node1,
      replacedAt: new Date(),
      transactionInternalId: Number(secondRow!.id),
    });
    await env.store.publishWatermarks();
    const hashes = [
      env.data.closeA.hash,
      env.data.fin2.hash,
      env.data.redeemB.hash,
      second.hash,
    ];
    const recorder = recordQueries(env.api);
    const before = env.api.db.stats.snapshots;
    const result = await env.gql<{
      transactions_by_hash: {
        hash: string;
        one_inclusions: {
          transaction_index: number;
          block: {
            hash: string;
            height: number;
            accepted_by: { node: { name: string } }[];
            two: { node: { name: string } }[];
          };
        }[];
        two_inclusions: { block: { height: number } }[];
        all_inclusions: {
          block: { height: number };
          on_one: boolean;
          on_two: boolean;
        }[];
        validations: { node: { name: string } }[];
        timeline: { replaced_at: string | null; node: { name: string } }[];
      }[];
    }>(
      `query($hashes: [Hex!]!) {
        transactions_by_hash(hashes: $hashes) {
          hash
          one_inclusions: block_inclusions(accepted_by: "node-one") {
            transaction_index
            block { hash height accepted_by(node: "node-one") { node { name } } two: accepted_by(node: "node-two") { node { name } } }
          }
          two_inclusions: block_inclusions(accepted_by: "node-two") { block { height } }
          all_inclusions: block_inclusions { block { height } on_one: accepted(node: "node-one") on_two: accepted(node: "node-two") }
          validations: node_validations(node: "node-one") { node { name } }
          timeline: node_validation_timeline(node: "node-one") { replaced_at node { name } }
        }
      }`,
      { hashes }
    );
    recorder.stop();
    t.is(env.api.db.stats.snapshots - before, 1, 'one readSnapshot');
    const snapshotEvent = recorder.events.find(
      (event) => event.kind === 'snapshot' && event.params.nodes !== undefined
    );
    if (snapshotEvent !== undefined) {
      t.deepEqual(
        sorted((snapshotEvent.params.nodes as number[]).map(String)),
        sorted([env.nodes.node1, env.nodes.node2].map(String)),
        'both named nodes pinned'
      );
    }
    t.deepEqual(
      sorted(result.transactions_by_hash.map((row) => row.hash)),
      sorted(hashes)
    );
    for (const row of result.transactions_by_hash) {
      for (const [field, node] of [
        ['one_inclusions', one],
        ['two_inclusions', two],
      ] as const) {
        const expected = await sql<{ height: number; index: number }>(
          env,
          `SELECT a.height AS height, bt.transaction_index AS index
           FROM tx_acceptance_v(node = ${nodeId(env, node)}) AS a
           INNER JOIN block_transaction_v AS bt ON bt.block_internal_id = a.block_internal_id AND bt.transaction_hash = a.transaction_hash
           WHERE a.transaction_hash = unhex('${
             row.hash
           }') AND a.block_internal_id != 0 ORDER BY a.height`
        );
        t.deepEqual(
          (row[field] as { block: { height: number } }[]).map(
            (inclusion) => inclusion.block.height
          ),
          expected.map((inclusion) => inclusion.height),
          `${row.hash.slice(0, 8)} ${field}`
        );
        if (field === 'one_inclusions') {
          t.deepEqual(
            row.one_inclusions.map((inclusion) => inclusion.transaction_index),
            expected.map((inclusion) => inclusion.index)
          );
          row.one_inclusions.forEach((inclusion) => {
            t.deepEqual(inclusion.block.accepted_by, [{ node: { name: one } }]);
            t.deepEqual(
              inclusion.block.two,
              inclusion.block.height <= 1 ? [{ node: { name: two } }] : []
            );
          });
        }
      }
      /* node-agnostic: every stored block with the transaction, and per-node acceptance of each */
      const stored = await sql<{ height: number; one: number; two: number }>(
        env,
        `SELECT b.height AS height,
           b.internal_id IN (SELECT block_internal_id FROM node_block_v(node = ${env.nodes.node1})) AS one,
           b.internal_id IN (SELECT block_internal_id FROM node_block_v(node = ${env.nodes.node2})) AS two
         FROM block_transaction_v AS bt INNER JOIN block_v AS b ON b.internal_id = bt.block_internal_id
         WHERE bt.transaction_hash = unhex('${row.hash}') ORDER BY b.height`
      );
      t.deepEqual(
        row.all_inclusions.map((inclusion) => [
          inclusion.block.height,
          inclusion.on_one,
          inclusion.on_two,
        ]),
        stored.map((inclusion) => [
          inclusion.height,
          Number(inclusion.one) === 1,
          Number(inclusion.two) === 1,
        ]),
        `${row.hash.slice(0, 8)} all inclusions`
      );
      const validations = await sql<{ n: string }>(
        env,
        `SELECT toString(count()) AS n FROM node_transaction_v(node = ${env.nodes.node1}) WHERE transaction_hash = unhex('${row.hash}')`
      );
      t.is(row.validations.length, Number(validations[0]!.n));
      const timeline = await sql<{ replaced: number }>(
        env,
        `SELECT isNotNull(h.replaced_at) AS replaced FROM node_transaction_history_v(node = ${env.nodes.node1}) AS h
         INNER JOIN transaction_v AS tx ON tx.internal_id = h.transaction_internal_id
         WHERE tx.hash = unhex('${row.hash}') ORDER BY h.internal_id`
      );
      t.deepEqual(
        row.timeline.map((entry) => entry.replaced_at !== null),
        timeline.map((entry) => Number(entry.replaced) === 1),
        `${row.hash.slice(0, 8)} timeline`
      );
    }
    const byHash = new Map(
      result.transactions_by_hash.map((row) => [row.hash, row])
    );
    t.is(byHash.get(env.data.redeemB.hash)!.timeline.length, 1, 'confirmed');
    t.is(byHash.get(env.data.redeemB.hash)!.timeline[0]!.replaced_at, null);
    t.not(byHash.get(second.hash)!.timeline[0]!.replaced_at, null, 'expired');
    t.deepEqual(byHash.get(env.data.fin2.hash)!.two_inclusions, []);
  }
);

e2e(
  '[e2e] G1 R8 + R14: node-agnostic lookups by hash (chunked lists), acceptance only with a named node, transaction watermark',
  async (t) => {
    const env = await setup(t, 'g1_lookup');
    const real = [env.data.fin2.hash, env.data.closeA.hash, env.data.c0.hash];
    // 2,500 hashes: bound in three chunks of at most 1,000
    const filler = Array.from({ length: 2_497 }, (_, index) =>
      createHash('sha256').update(`filler-${index}`).digest('hex')
    );
    const recorder = recordQueries(env.api);
    const result = await env.gql<{
      transactions_by_hash: { hash: string }[];
      outputs_by_transaction_hash: {
        transaction_hash: string;
        output_index: number;
        spent_by: { transaction: { hash: string } }[];
        all_spends: { transaction: { hash: string }; on_two: boolean }[];
      }[];
      blocks_by_hash: { height: number }[];
      transaction_watermark: string;
    }>(
      `query($hashes: [Hex!]!, $blocks: [Hex!]!) {
        transactions_by_hash(hashes: $hashes) { hash }
        outputs_by_transaction_hash(transaction_hashes: $hashes, where: { output_index: { _lt: 8 } }) {
          transaction_hash output_index spent_by(accepted_by: "node-two") { transaction { hash } }
          all_spends: spent_by { transaction { hash } on_two: accepted(node: "node-two") }
        }
        blocks_by_hash(hashes: $blocks) { height }
        transaction_watermark
      }`,
      {
        blocks: [env.data.block2.hash, env.data.block0.hash],
        hashes: [...filler, ...real],
      }
    );
    recorder.stop();
    t.deepEqual(
      sorted(result.transactions_by_hash.map((row) => row.hash)),
      sorted(real)
    );
    const chunked = recorder.events.find(
      (event) => event.label === 'transactions_by_hash'
    )!;
    const lists = Object.values(chunked.params).filter(
      (value) => Array.isArray(value) && value.length > 100
    ) as unknown[][];
    t.deepEqual(
      lists.map((list) => list.length).sort((a, b) => a - b),
      [500, 1_000, 1_000],
      'three bound chunks'
    );
    t.deepEqual(
      result.blocks_by_hash.map((row) => row.height),
      [0, 2],
      'node-agnostic: block 2 is found though node-two does not accept it'
    );
    // outputs (node-agnostic) with node-two's spends
    const outputs = await sql<{ hash: string; index: number }>(
      env,
      `SELECT lower(hex(transaction_hash)) AS hash, output_index AS index FROM output_v
       WHERE transaction_hash IN (${real
         .map((hash) => `unhex('${hash}')`)
         .join(
           ', '
         )}) AND output_index < 8 ORDER BY transaction_internal_id, output_index`
    );
    t.deepEqual(
      result.outputs_by_transaction_hash.map(
        (row) => `${row.transaction_hash}:${row.output_index}`
      ),
      outputs.map((row) => `${row.hash}:${row.index}`)
    );
    const spends = await sql<{ key: string; spender: string }>(
      env,
      `SELECT concat(lower(hex(i.outpoint_transaction_hash)), ':', toString(i.outpoint_index)) AS key, lower(hex(i.transaction_hash)) AS spender
       FROM input_v AS i WHERE i.transaction_hash IN (SELECT transaction_hash FROM tx_acceptance_v(node = ${env.nodes.node2}))`
    );
    const spentOnTwo = new Map(spends.map((row) => [row.key, row.spender]));
    const allSpends = await sql<{ key: string; spender: string }>(
      env,
      `SELECT concat(lower(hex(i.outpoint_transaction_hash)), ':', toString(i.outpoint_index)) AS key, lower(hex(i.transaction_hash)) AS spender
       FROM input_v AS i`
    );
    const spentAnywhere = new Map(
      allSpends.map((row) => [row.key, row.spender])
    );
    result.outputs_by_transaction_hash.forEach((row) => {
      const key = `${row.transaction_hash}:${row.output_index}`;
      t.deepEqual(
        row.spent_by.map((input) => input.transaction.hash),
        spentOnTwo.has(key) ? [spentOnTwo.get(key)] : [],
        key
      );
      t.deepEqual(
        row.all_spends.map((input) => [input.transaction.hash, input.on_two]),
        spentAnywhere.has(key)
          ? [[spentAnywhere.get(key), spentOnTwo.has(key)]]
          : [],
        `${key} (every stored spend)`
      );
    });
    t.true(
      result.outputs_by_transaction_hash.some(
        (row) => row.all_spends.length > 0 && row.spent_by.length === 0
      ),
      'a spend node-two does not accept is listed only without a node'
    );
    const [watermark] = await sql<{ id: string }>(
      env,
      'SELECT toString(max(internal_id)) AS id FROM transaction_v'
    );
    t.is(result.transaction_watermark, watermark!.id);

    // a per-node relationship below a node-agnostic root must name its node
    const refused = await env.gqlRaw(
      `query($hashes: [Hex!]!) { transactions_by_hash(hashes: $hashes) { hash node_validations { validated_at } } }`,
      { hashes: real }
    );
    t.regex(refused.errors?.[0]?.message ?? '', /name the node/u);
    // a node that is not named by the operation cannot be read
    const unnamed = await env.gqlRaw(
      `query($hashes: [Hex!]!, $other: String!) { transactions_by_hash(hashes: $hashes) { hash block_inclusions(accepted_by: $other) { transaction_index } } }`,
      { hashes: real, other: 'node-three' }
    );
    t.truthy(unnamed.errors?.[0]?.message);
    // accepted(node:) requires its node (no "any node" answer)
    const anyNode = await env.gqlRaw(
      `query($hashes: [Hex!]!) { transactions_by_hash(hashes: $hashes) { hash accepted } }`,
      { hashes: real }
    );
    t.regex(anyNode.errors?.[0]?.message ?? '', /node/u);
    // more than the cap is refused (visible, not cut)
    const tooMany = await env.gqlRaw(
      `query($hashes: [Hex!]!) { transactions_by_hash(hashes: $hashes) { hash } }`,
      {
        hashes: Array.from({ length: 10_001 }, (_, index) =>
          createHash('sha256').update(`x${index}`).digest('hex')
        ),
      }
    );
    t.regex(tooMany.errors?.[0]?.message ?? '', /At most 10000/u);
  }
);

e2e(
  '[e2e] G1 R35 + R32: node metadata, per-node mempool count and accepted blocks, watermark; equal to node_v and the views',
  async (t) => {
    const env = await setup(t, 'g1_nodes');
    const result = await env.gql<{
      nodes: { name: string; user_agent: string; protocol_version: number }[];
      a: {
        name: string;
        unconfirmed_transaction_count: string;
        unconfirmed_transactions: { transaction_hash: string }[];
        latest_tip: { height: number; block: { hash: string } }[];
        latest_connection_began_at: string;
      };
      b: {
        unconfirmed_transaction_count: string;
        latest_tip: { height: number }[];
      };
      wa: { visible: string; node: string };
      wb: { visible: string };
    }>(`{
      nodes { name user_agent protocol_version }
      a: node(node: "node-one") {
        name latest_connection_began_at unconfirmed_transaction_count
        unconfirmed_transactions(limit: 5, order_by: { validated_at: DESC }) { transaction_hash }
        latest_tip: accepted_blocks(limit: 1, order_by: { block: { height: DESC } }) { height block { hash } }
      }
      b: node(node: "node-two") { unconfirmed_transaction_count latest_tip: accepted_blocks(limit: 1, order_by: { block_internal_id: DESC }) { height } }
      wa: watermark(node: "node-one") { visible node }
      wb: watermark(node: "node-two") { visible }
    }`);
    const nodes = await sql<{
      name: string;
      user_agent: string;
      protocol_version: number;
    }>(
      env,
      'SELECT name, user_agent, protocol_version FROM node_v ORDER BY internal_id'
    );
    t.deepEqual(result.nodes, nodes);
    t.is(result.a.name, one);
    t.is(
      Date.parse(result.a.latest_connection_began_at),
      Date.parse('2026-10-09T00:00:00Z')
    );
    t.is(result.a.unconfirmed_transaction_count, '1');
    t.deepEqual(result.a.unconfirmed_transactions, [
      { transaction_hash: env.data.redeemB.hash },
    ]);
    t.deepEqual(result.a.latest_tip, [
      { block: { hash: env.data.block2.hash }, height: 2 },
    ]);
    t.is(result.b.unconfirmed_transaction_count, '0');
    t.deepEqual(result.b.latest_tip, [{ height: 1 }]);
    const marks = await sql<{ node: number; visible: string }>(
      env,
      'SELECT node_internal_id AS node, toString(max(visible_seq)) AS visible FROM visibility GROUP BY node'
    );
    const byNode = new Map(marks.map((row) => [Number(row.node), row.visible]));
    t.is(result.wa.visible, byNode.get(env.nodes.node1) ?? 'missing');
    t.is(result.wb.visible, byNode.get(env.nodes.node2) ?? 'missing');
    // per-node relationships below `nodes` (node-agnostic) are refused
    const refused = await env.gqlRaw(
      '{ nodes { name unconfirmed_transaction_count } }'
    );
    t.regex(refused.errors?.[0]?.message ?? '', /name the node/u);
  }
);
