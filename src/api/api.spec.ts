/* eslint-disable max-lines, @typescript-eslint/no-magic-numbers, functional/no-loop-statement, functional/no-let, no-await-in-loop, camelcase, @typescript-eslint/naming-convention, functional/no-throw-statement, @typescript-eslint/require-array-sort-compare, complexity, @typescript-eslint/no-loop-func, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/restrict-template-expressions, functional/no-mixed-type, max-params, no-bitwise, require-atomic-updates */
// cspell:ignore clickhouse paryon unhex pothos seqs
/**
 * Phase 2 spike proofs (docs/clickhouse-port/phase2-spike.md §3): S1–S6
 * over a two-node dataset written through the ClickHouse store, each result
 * compared with an independent computation (the in-memory model of
 * spec-dataset.ts, the store checker, or direct SQL on the same snapshot).
 */
import type { ExecutionContext } from 'ava';
import test from 'ava';
import { createClient as createWsClient } from 'graphql-ws';
import WebSocket from 'ws';

import { createClickHouseChecker } from '../store/clickhouse/checker.js';
import type { ClickHouseStore } from '../store/clickhouse/clickhouse-store.js';
import type { ClickHouseClient } from '../store/clickhouse/client.js';
import {
  acceptance,
  notSaved,
  registerNodes,
  scratch,
} from '../store/clickhouse/spec-fixtures.js';
import { e2eClickHouseUrl } from '../store/clickhouse/test-support.js';
import {
  nodeAgnosticId,
  pinnedView,
  readWatermark,
  snapshotParams,
} from '../store/clickhouse/visibility.js';

import type { QueryEvent } from './db.js';
import { schema } from './schema.js';
import type { Api, RunningServer } from './server.js';
import { createApi, listen } from './server.js';
import type { Dataset, ModelInput, ModelOutput } from './spec-dataset.js';
import {
  buildDataset,
  genesis,
  holder1,
  loanScript,
  Model,
  opReturn,
  paryon,
  priceScript,
  redeemer,
  user1,
  user2,
} from './spec-dataset.js';
import { snapshotParameterNames } from './sql.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const one = 'node-one';
const two = 'node-two';

/* ------------------------------------------------------------------ */
/* setup                                                                */
/* ------------------------------------------------------------------ */

interface Env {
  api: Api;
  server: RunningServer;
  store: ClickHouseStore;
  client: ClickHouseClient;
  data: Dataset;
  model: Model;
  nodes: { node1: number; node2: number };
  /** Accept blocks on node-one (model and store), publish. */
  extendNodeOne: (txs: Parameters<Dataset['laterBlock']>[2]) => Promise<void>;
  gql: <T = { [key: string]: unknown }>(
    query: string,
    variables?: { [key: string]: unknown }
  ) => Promise<T>;
}

const setup = async (
  t: ExecutionContext,
  label: string,
  utxo: 'off' | 'on' = 'off'
): Promise<Env> => {
  const { client, openStore } = await scratch(t, label, { utxo }, 'ch1_api');
  const store = await openStore();
  const nodes = await registerNodes(store);
  const data = buildDataset();
  const model = new Model();
  const both = [acceptance(nodes.node1), acceptance(nodes.node2)];
  for (const block of [data.block0, data.block1]) {
    await store.saveBlock({
      block,
      isSavedTransaction: notSaved,
      nodeAcceptances: both,
    });
    model.addBlock(block, [one, two]);
  }
  await store.saveBlock({
    block: data.block2,
    isSavedTransaction: notSaved,
    nodeAcceptances: [acceptance(nodes.node1)],
  });
  model.addBlock(data.block2, [one]);
  await store.saveMempoolTransaction(data.redeemB, [
    { nodeInternalId: nodes.node1, validatedAt: new Date() },
  ]);
  model.addMempool(data.redeemB, [one]);
  await store.publishWatermarks();

  const api = createApi(client, {
    live: {
      onError: (error) => {
        t.log('live error', error);
      },
      pollMs: 20,
    },
  });
  const server = await listen(api);
  t.teardown(async () => server.close());
  let tip = data.block2;
  const extendNodeOne = async (txs: Parameters<Dataset['laterBlock']>[2]) => {
    const block = data.laterBlock(tip.height + 1, tip, txs);
    await store.saveBlock({
      block,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(nodes.node1)],
    });
    model.addBlock(block, [one]);
    tip = block;
    await store.publishWatermarks();
  };
  const gql = async <T>(
    query: string,
    variables: { [key: string]: unknown } = {}
  ) => {
    const response = await fetch(server.url, {
      body: JSON.stringify({ query, variables }),
      headers: { 'content-type': 'application/json' },
      method: 'POST',
    });
    const body = (await response.json()) as {
      data?: T;
      errors?: unknown[];
    };
    if (body.errors !== undefined) {
      throw new Error(JSON.stringify(body.errors));
    }
    return body.data!;
  };
  return {
    api,
    client,
    data,
    extendNodeOne,
    gql,
    model,
    nodes,
    server,
    store,
  };
};

/** Every query an API request sends, recorded. */
const recordQueries = (api: Api) => {
  const events: QueryEvent[] = [];
  const previous = api.db.onQuery;
  api.db.onQuery = (event) => {
    events.push(event);
    previous?.(event);
  };
  return {
    events,
    stop: () => {
      api.db.onQuery = previous;
    },
  };
};

const sorted = (values: readonly string[]) => [...values].sort();

/** internal_id of every transaction, read directly (node-agnostic snapshot). */
const internalIds = async (env: Env) => {
  const snapshot = await env.api.db.readSnapshot(nodeAgnosticId);
  const rows = await env.client.query<{ hash: string; id: string }>(
    `SELECT lower(hex(hash)) AS hash, toString(internal_id) AS id FROM ${pinnedView(
      'transaction_at'
    )}`,
    snapshotParams(snapshot)
  );
  return new Map(rows.map((row) => [row.hash, BigInt(row.id)]));
};

/* ------------------------------------------------------------------ */
/* the documents (v2 shape)                                             */
/* ------------------------------------------------------------------ */

/** S1 / L24 UserInitiatedLoanClosures. */
const userInitiatedLoanClosures = `
query UserInitiatedLoanClosures($node: String!, $paryon: Hex!, $loan: Hex!, $users: [Hex!]!, $after: BigInt, $limit: Int!) {
  transactions(node: $node, after: $after, limit: $limit, where: {
    has_input: { outpoint: { token_category: { _eq: $paryon }, nonfungible_token_capability: { _eq: MUTABLE }, locking_bytecode: { _eq: $loan } } }
    _and: [{ has_input: { input_index: { _eq: 4 }, outpoint: { locking_bytecode: { _in: $users } } } }]
    _not: { has_output: { locking_bytecode: { _eq: $loan } } }
  }) {
    has_more
    end_cursor
    nodes {
      internal_id
      hash
      inputs(where: { input_index: { _eq: 4 } }) { outpoint { token_category locking_bytecode } }
    }
  }
}`;

/** S1 / L10 UserRedemptionInteractions: OR of EXISTS + per-node block window. */
const userRedemptionInteractions = `
query UserRedemptionInteractions($node: String!, $redeemer: Hex!, $commitments: [Hex!]!, $from: Int, $to: Int, $limit: Int!) {
  transactions(node: $node, limit: $limit, where: {
    has_input: { outpoint: { token_category: { _eq: $redeemer } } }
    block_window: { from_height: $from, to_height: $to }
    _or: [
      { has_output: { token_category: { _eq: $redeemer }, nonfungible_token_capability: { _eq: NONE }, nonfungible_token_commitment: { _in: $commitments } } }
      { has_input: { outpoint: { token_category: { _eq: $redeemer }, nonfungible_token_capability: { _eq: NONE }, nonfungible_token_commitment: { _in: $commitments } } } }
    ]
  }) {
    nodes {
      internal_id
      hash
      block_inclusions { block { height timestamp } }
      outputs { value_satoshis fungible_token_amount nonfungible_token_commitment nonfungible_token_capability token_category }
      inputs { outpoint_transaction_hash outpoint { value_satoshis nonfungible_token_commitment token_category } }
    }
  }
}`;

/* ------------------------------------------------------------------ */
/* S1                                                                   */
/* ------------------------------------------------------------------ */

e2e(
  '[e2e] API S1: relationship filters (same-row conjunction, NOT EXISTS, OR of EXISTS, per-node block window) equal the model',
  async (t) => {
    const env = await setup(t, 's1');
    const ids = await internalIds(env);
    const { model } = env;

    // L24: model
    const l24 = (node: string, users: string[]) =>
      model
        .accepted(node)
        .filter((entry) => {
          const inputs = model.inputs(entry.tx);
          const outputs = model.outputs(entry.tx);
          return (
            inputs.some(
              (input) =>
                input.outpoint?.token_category === paryon &&
                input.outpoint.nonfungible_token_capability === 'mutable' &&
                input.outpoint.locking_bytecode === loanScript
            ) &&
            inputs.some(
              (input) =>
                input.input_index === 4 &&
                users.includes(input.outpoint?.locking_bytecode ?? '')
            ) &&
            !outputs.some((output) => output.locking_bytecode === loanScript)
          );
        })
        .map((entry) => entry.tx.hash);

    for (const [node, users] of [
      [one, [user1]],
      [two, [user1]],
      [one, [user2]],
      [one, [user1, user2]],
    ] as const) {
      const result = await env.gql<{
        transactions: {
          nodes: {
            hash: string;
            internal_id: string;
            inputs: { outpoint: { locking_bytecode: string } }[];
          }[];
        };
      }>(userInitiatedLoanClosures, {
        limit: 100,
        loan: loanScript,
        node,
        paryon,
        users: [...users],
      });
      const hashes = result.transactions.nodes.map((row) => row.hash);
      t.deepEqual(
        sorted(hashes),
        sorted(l24(node, [...users])),
        `L24 ${node} ${users.length}`
      );
      result.transactions.nodes.forEach((row) => {
        t.is(row.internal_id, String(ids.get(row.hash)));
        t.true(users.includes(row.inputs[0]!.outpoint.locking_bytecode));
      });
    }
    // the closure exists and the interest payment (re-creates the loan) is excluded
    const l24One = l24(one, [user1]);
    t.deepEqual(l24One, [env.data.closeA.hash]);

    // L10: model (WIN per node: accepted in a block in [from, to) or in the node's mempool)
    const l10 = (
      node: string,
      commitments: string[],
      from: number,
      to: number
    ) =>
      model
        .accepted(node)
        .filter((entry) => {
          const height = entry.acceptedBy.get(node);
          const inWindow = height === null || (height! >= from && height! < to);
          const inputs = model.inputs(entry.tx);
          const sidecar = (output: ModelOutput | undefined) =>
            output?.token_category === redeemer &&
            output.nonfungible_token_capability === 'none' &&
            commitments.includes(output.nonfungible_token_commitment ?? '');
          return (
            inWindow &&
            inputs.some(
              (input) => input.outpoint?.token_category === redeemer
            ) &&
            (model.outputs(entry.tx).some(sidecar) ||
              inputs.some((input: ModelInput) => sidecar(input.outpoint)))
          );
        })
        .map((entry) => entry.tx.hash);
    for (const [node, commitments, from, to] of [
      [one, ['c1', 'c2'], 0, 10],
      [two, ['c1', 'c2'], 0, 10],
      [one, ['c2'], 0, 10],
      [one, ['c1'], 2, 10],
      [one, ['c1'], 0, 2],
    ] as const) {
      const result = await env.gql<{
        transactions: {
          nodes: {
            hash: string;
            block_inclusions: { block: { height: number } }[];
            outputs: unknown[];
          }[];
        };
      }>(userRedemptionInteractions, {
        commitments: [...commitments],
        from,
        limit: 100,
        node,
        redeemer,
        to,
      });
      const expected = l10(node, [...commitments], from, to);
      t.deepEqual(
        sorted(result.transactions.nodes.map((row) => row.hash)),
        sorted(expected),
        `L10 ${node} ${commitments.join(',')} [${from}, ${to})`
      );
      result.transactions.nodes.forEach((row) => {
        const height = model.txs.get(row.hash)!.acceptedBy.get(node);
        t.deepEqual(
          row.block_inclusions.map((inclusion) => inclusion.block.height),
          height === null || height === undefined ? [] : [height]
        );
        t.is(row.outputs.length, model.txs.get(row.hash)!.tx.outputs.length);
      });
    }
    // the mempool redemption is visible to node-one only
    t.true(l10(one, ['c2'], 0, 10).includes(env.data.redeemB.hash));
    t.deepEqual(l10(two, ['c1', 'c2'], 0, 10), [env.data.redeemA.hash]);
  }
);

/* ------------------------------------------------------------------ */
/* S2                                                                   */
/* ------------------------------------------------------------------ */

/** S2 / L17 finalizedRedemptions: composite keyset on (transaction_internal_id, input_index). */
const finalizedRedemptions = `
query finalizedRedemptions($node: String!, $paryon: Hex!, $after: String, $limit: Int!) {
  inputs(node: $node, after: $after, limit: $limit, where: {
    outpoint: { token_category: { _eq: $paryon }, nonfungible_token_capability: { _eq: NONE }, nonfungible_token_commitment: { _eq: "03" } }
  }) {
    has_more
    end_cursor
    nodes {
      transaction_internal_id
      input_index
      transaction {
        hash
        outputs(where: { output_index: { _in: [0, 1, 3] } }) { output_index token_category fungible_token_amount value_satoshis }
        inputs(where: { input_index: { _in: [0, 3, 4, 5] } }) { input_index outpoint { nonfungible_token_commitment fungible_token_amount value_satoshis } }
        block_inclusions { block { timestamp height } }
      }
    }
  }
}`;

/** S2 / L30 MonitorLoansWindow: two-sided keyset, variable direction. */
const monitorLoansWindow = `
query MonitorLoansWindow($node: String!, $paryon: Hex!, $loan: Hex!, $after: BigInt, $before: BigInt, $order: Order!, $limit: Int!) {
  transactions(node: $node, after: $after, before: $before, order: $order, limit: $limit, where: {
    has_input: { outpoint: { token_category: { _eq: $paryon }, nonfungible_token_capability: { _eq: MUTABLE }, locking_bytecode: { _eq: $loan } } }
  }) {
    has_more
    end_cursor
    nodes {
      internal_id
      hash
      outputs(where: { token_category: { _eq: $paryon }, nonfungible_token_capability: { _eq: MUTABLE }, locking_bytecode: { _eq: $loan } }) { nonfungible_token_commitment }
      inputs(where: { outpoint: { token_category: { _eq: $paryon }, nonfungible_token_capability: { _eq: MUTABLE }, locking_bytecode: { _eq: $loan } } }) { outpoint { nonfungible_token_commitment } }
    }
  }
}`;

interface InputPage {
  inputs: {
    has_more: boolean;
    end_cursor: string | null;
    nodes: {
      transaction_internal_id: string;
      input_index: number;
      transaction: {
        hash: string;
        outputs: { output_index: number }[];
        inputs: { input_index: number }[];
        block_inclusions: { block: { height: number } }[];
      };
    }[];
  };
}

e2e(
  '[e2e] API S2: composite keyset walk while blocks commit (no duplicate, no skip) and two-sided keyset both directions',
  async (t) => {
    const env = await setup(t, 's2');
    const { data, model } = env;
    const finalizeOutpoint = (index: number): [string, number] => [
      data.c0.hash,
      index,
    ];
    // blocks committed during the walk, each with more matching inputs
    const pending = [
      [genesis.finalize[4]!, genesis.finalize[5]!],
      [genesis.finalize[6]!],
      [genesis.finalize[7]!, genesis.finalize[8]!, genesis.finalize[9]!],
    ];
    const walked: string[] = [];
    let after: string | null = null;
    let pages = 0;
    for (;;) {
      const page: InputPage = await env.gql<InputPage>(finalizedRedemptions, {
        after,
        limit: 1,
        node: one,
        paryon,
      });
      pages += 1;
      page.inputs.nodes.forEach((row) => {
        walked.push(`${row.transaction.hash}:${row.input_index}`);
        // nested relationship arguments equal the model
        const { tx } = model.txs.get(row.transaction.hash)!;
        t.deepEqual(
          row.transaction.outputs.map((output) => output.output_index),
          [0, 1, 3].filter((index) => index < tx.outputs.length)
        );
        t.deepEqual(
          row.transaction.inputs.map((input) => input.input_index),
          [0, 3, 4, 5].filter((index) => index < tx.inputs.length)
        );
      });
      const next = pending.shift();
      if (next !== undefined) {
        await env.extendNodeOne(
          next.map((index) => ({
            ...data.redeemA,
            hash: `${index.toString(16).padStart(2, '0')}${'ee'.repeat(31)}`,
            inputs: [
              {
                outpointIndex: finalizeOutpoint(index)[1],
                outpointTransactionHash: finalizeOutpoint(index)[0],
                sequenceNumber: 0,
                unlockingBytecode: '51',
              },
            ],
          }))
        );
      }
      if (!page.inputs.has_more && next === undefined) break;
      after = page.inputs.end_cursor ?? after;
      t.true(pages < 50, 'walk terminates');
    }
    const ids = await internalIds(env);
    const expected = model
      .accepted(one)
      .flatMap((entry) => model.inputs(entry.tx))
      .filter(
        (input) =>
          input.outpoint?.token_category === paryon &&
          input.outpoint.nonfungible_token_capability === 'none' &&
          input.outpoint.nonfungible_token_commitment === '03'
      )
      .sort((a, b) => {
        const byId =
          ids.get(a.transaction_hash)! - ids.get(b.transaction_hash)!;
        return byId === 0n ? a.input_index - b.input_index : byId < 0n ? -1 : 1;
      })
      .map((input) => `${input.transaction_hash}:${input.input_index}`);
    t.is(new Set(walked).size, walked.length, 'no duplicate');
    t.deepEqual(walked, expected, 'every row once, in key order');
    t.is(expected.length, 10);
    t.log(
      `L17 walk: ${walked.length} rows in ${pages} pages, 3 blocks committed during the walk`
    );

    // node-two sees only its own (blocks 0-1): fin1's two inputs
    const two17 = await env.gql<InputPage>(finalizedRedemptions, {
      limit: 100,
      node: two,
      paryon,
    });
    t.deepEqual(
      two17.inputs.nodes.map(
        (row) => `${row.transaction.hash}:${row.input_index}`
      ),
      [`${data.fin1.hash}:0`, `${data.fin1.hash}:1`]
    );

    // L30: two-sided window, both directions, page size 1
    const loanTxs = model
      .accepted(one)
      .filter((entry) =>
        model
          .inputs(entry.tx)
          .some(
            (input) =>
              input.outpoint?.token_category === paryon &&
              input.outpoint.nonfungible_token_capability === 'mutable' &&
              input.outpoint.locking_bytecode === loanScript
          )
      )
      .map((entry) => entry.tx.hash)
      .sort((a, b) => (ids.get(a)! < ids.get(b)! ? -1 : 1));
    t.deepEqual(
      sorted(loanTxs),
      sorted([data.closeA.hash, data.interest.hash])
    );
    const window = async (
      order: 'ASC' | 'DESC',
      lower: bigint,
      upper: bigint
    ) => {
      const seen: string[] = [];
      let bounds = { after: lower, before: upper };
      for (;;) {
        const page: {
          transactions: {
            has_more: boolean;
            end_cursor: string | null;
            nodes: { hash: string; outputs: unknown[]; inputs: unknown[] }[];
          };
        } = await env.gql(monitorLoansWindow, {
          after: bounds.after.toString(),
          before: bounds.before.toString(),
          limit: 1,
          loan: loanScript,
          node: one,
          order,
          paryon,
        });
        page.transactions.nodes.forEach((row) => {
          seen.push(row.hash);
          t.is(row.inputs.length, 1);
        });
        if (!page.transactions.has_more) break;
        const cursor = BigInt(page.transactions.end_cursor!);
        bounds =
          order === 'ASC'
            ? { ...bounds, after: cursor }
            : { ...bounds, before: cursor };
      }
      return seen;
    };
    t.deepEqual(await window('ASC', 0n, 1n << 62n), loanTxs);
    t.deepEqual(await window('DESC', 0n, 1n << 62n), [...loanTxs].reverse());
    const first = ids.get(loanTxs[0]!)!;
    t.deepEqual(
      await window('DESC', first, 1n << 62n),
      loanTxs.slice(1).reverse()
    );
    t.deepEqual(await window('ASC', 0n, first + 1n), loanTxs.slice(0, 1));
  }
);

/* ------------------------------------------------------------------ */
/* S3 + S5                                                              */
/* ------------------------------------------------------------------ */

/** S5 / L12 AllHolders, v1-style (query-time unspent; UTXO tables off). */
const allHolders = `
query AllHolders($node: String!, $paryon: Hex!, $after: String, $limit: Int!) {
  unspent_outputs(node: $node, after: $after, limit: $limit, where: {
    token_category: { _eq: $paryon }
    nonfungible_token_capability: { _is_null: true }
    locking_bytecode: { _neq: "6a" }
  }) {
    has_more
    end_cursor
    nodes { transaction_hash output_index locking_bytecode fungible_token_amount value_satoshis }
  }
}`;

/** S3: holder balances grouped by locking bytecode (L12's client-side aggregation, server-side). */
const holderBalances = `
query HolderBalances($node: String!, $paryon: Hex!) {
  unspent_output_groups(node: $node, group_by: [LOCKING_BYTECODE], limit: 1000,
    order_by: { by: SUM_FUNGIBLE_TOKEN_AMOUNT, direction: DESC },
    where: { token_category: { _eq: $paryon }, nonfungible_token_capability: { _is_null: true }, locking_bytecode: { _neq: "6a" } }) {
    key { locking_bytecode }
    aggregate { count sum { fungible_token_amount value_satoshis } }
  }
}`;

interface HolderRow {
  transaction_hash: string;
  output_index: number;
  locking_bytecode: string;
  fungible_token_amount: string;
  value_satoshis: string;
}

const walkAllHolders = async (env: Env, node: string, limit: number) => {
  const rows: HolderRow[] = [];
  let after: string | null = null;
  for (;;) {
    const page: {
      unspent_outputs: {
        has_more: boolean;
        end_cursor: string | null;
        nodes: HolderRow[];
      };
    } = await env.gql(allHolders, { after, limit, node, paryon });
    rows.push(...page.unspent_outputs.nodes);
    if (!page.unspent_outputs.has_more) return rows;
    after = page.unspent_outputs.end_cursor;
  }
};

const holderModel = (model: Model, node: string) =>
  model
    .unspent(node)
    .filter(
      (output) =>
        output.token_category === paryon &&
        output.nonfungible_token_capability === null &&
        output.locking_bytecode !== opReturn
    );

const groupsOf = (
  rows: readonly {
    locking_bytecode: string;
    fungible_token_amount: bigint | string | null;
    value_satoshis: bigint | string;
  }[]
) => {
  const groups = new Map<
    string,
    { count: number; ft: bigint; value: bigint }
  >();
  rows.forEach((row) => {
    const group = groups.get(row.locking_bytecode) ?? {
      count: 0,
      ft: 0n,
      value: 0n,
    };
    group.count += 1;
    group.ft += BigInt(row.fungible_token_amount ?? 0);
    group.value += BigInt(row.value_satoshis);
    groups.set(row.locking_bytecode, group);
  });
  return [...groups.entries()]
    .map(([lockingBytecode, group]) => ({
      count: String(group.count),
      fungible_token_amount: group.ft.toString(),
      locking_bytecode: lockingBytecode,
      value_satoshis: group.value.toString(),
    }))
    .sort((a, b) => {
      const diff =
        BigInt(b.fungible_token_amount) - BigInt(a.fungible_token_amount);
      return diff === 0n
        ? a.locking_bytecode < b.locking_bytecode
          ? -1
          : 1
        : diff > 0n
        ? 1
        : -1;
    });
};

e2e(
  '[e2e] API S5 + S3: AllHolders v1-style with UTXO tables off and holder balances group-by equal the model, the checker and the client-side aggregation',
  async (t) => {
    const env = await setup(t, 's35', 'off');
    const [utxoRows] = await env.client.query<{ c: string }>(
      'SELECT count() AS c FROM utxo'
    );
    t.is(utxoRows?.c, '0', 'UTXO tables are off');
    const checker = createClickHouseChecker(env.client, env.client.database, {
      utxo: 'off',
    });
    for (const node of [one, two]) {
      const expected = holderModel(env.model, node);
      for (const limit of [1, 2, 1000]) {
        const rows = await walkAllHolders(env, node, limit);
        t.deepEqual(
          sorted(
            rows.map(
              (row) =>
                `${row.transaction_hash}:${row.output_index}:${row.locking_bytecode}:${row.fungible_token_amount}:${row.value_satoshis}`
            )
          ),
          sorted(
            expected.map(
              (output) =>
                `${output.transaction_hash}:${output.output_index}:${
                  output.locking_bytecode
                }:${String(output.fungible_token_amount)}:${
                  output.value_satoshis
                }`
            )
          ),
          `S5 ${node} page size ${limit}`
        );
      }
      // the store checker (independent SQL, same per-node rule) agrees on the outpoints
      const checkerOutpoints = new Set(
        (await checker.unspent(node, { category: paryon })).map(
          (row) => `${row.transactionHash}:${row.outputIndex}`
        )
      );
      t.deepEqual(
        sorted(expected.map((o) => `${o.transaction_hash}:${o.output_index}`)),
        sorted(
          env.model
            .unspent(node)
            .filter((o) =>
              checkerOutpoints.has(`${o.transaction_hash}:${o.output_index}`)
            )
            .filter(
              (o) =>
                o.nonfungible_token_capability === null &&
                o.locking_bytecode !== opReturn
            )
            .map((o) => `${o.transaction_hash}:${o.output_index}`)
        ),
        `checker ${node}`
      );
      t.is(
        checkerOutpoints.size,
        env.model.unspent(node).filter((o) => o.token_category === paryon)
          .length,
        `checker category count ${node}`
      );

      // S3
      const groups = await env.gql<{
        unspent_output_groups: {
          key: { locking_bytecode: string };
          aggregate: {
            count: string;
            sum: { fungible_token_amount: string; value_satoshis: string };
          };
        }[];
      }>(holderBalances, { node, paryon });
      const api = groups.unspent_output_groups.map((group) => ({
        count: group.aggregate.count,
        fungible_token_amount: group.aggregate.sum.fungible_token_amount,
        locking_bytecode: group.key.locking_bytecode,
        value_satoshis: group.aggregate.sum.value_satoshis,
      }));
      t.deepEqual(api, groupsOf(expected), `S3 = model aggregation, ${node}`);
      t.deepEqual(
        api,
        groupsOf(await walkAllHolders(env, node, 1000)),
        `S3 = client-side aggregation of S5, ${node}`
      );
    }
    // node-one: holder1 holds 50 + 40 (the 100 was split), holder3 30 + 60, holder2 200 + 5
    t.deepEqual(
      groupsOf(holderModel(env.model, one)).find(
        (g) => g.locking_bytecode === holder1
      )?.fungible_token_amount,
      '90'
    );
  }
);

/* ------------------------------------------------------------------ */
/* S6 + snapshot contract                                               */
/* ------------------------------------------------------------------ */

/** S6 / L33 ContractUtxoActivity: an activity page and the watermark root, one snapshot. */
const contractUtxoActivity = `
query ContractUtxoActivity($node: String!, $token: Hex!, $script: Hex!, $after: BigInt, $order: Order!, $limit: Int!) {
  transactions(node: $node, after: $after, order: $order, limit: $limit, where: {
    _or: [
      { has_output: { token_category: { _eq: $token }, nonfungible_token_capability: { _eq: MUTABLE }, locking_bytecode: { _eq: $script } } }
      { has_input: { outpoint: { token_category: { _eq: $token }, nonfungible_token_capability: { _eq: MUTABLE }, locking_bytecode: { _eq: $script } } } }
    ]
  }) {
    nodes {
      internal_id
      hash
      block_inclusions { block { timestamp height } }
      outputs { output_index locking_bytecode value_satoshis fungible_token_amount nonfungible_token_commitment nonfungible_token_capability token_category }
      inputs { outpoint_transaction_hash outpoint_index outpoint { value_satoshis nonfungible_token_commitment token_category } }
    }
  }
  transactionWatermark: transaction_watermark
}`;

e2e(
  '[e2e] API S6 + snapshot contract: two roots read one snapshot; one readSnapshot per request; no snapshot parameter reachable from the client',
  async (t) => {
    const env = await setup(t, 's6');
    const { model } = env;
    const variables = {
      after: '0',
      limit: 100,
      node: one,
      order: 'ASC',
      script: priceScript,
      token: paryon,
    };
    const recorder = recordQueries(env.api);
    const before = { ...env.api.db.stats };
    const result = await env.gql<{
      transactions: { nodes: { hash: string; internal_id: string }[] };
      transactionWatermark: string;
    }>(contractUtxoActivity, variables);
    recorder.stop();
    t.is(env.api.db.stats.snapshots - before.snapshots, 1, 'one readSnapshot');
    const data = recorder.events.filter((event) => event.kind === 'data');
    t.deepEqual(sorted(data.map((event) => event.label)), [
      'transaction_watermark',
      'transactions',
    ]);
    const snapshotOf = (event: QueryEvent) =>
      JSON.stringify(
        snapshotParameterNames.map((name) => String(event.params[name] ?? ''))
      );
    // both roots: identical node-agnostic gate parameters; the per-node root also the node's
    t.is(snapshotOf(data[0]!).length > 0, true);
    const [first, second] = data;
    (['visible0', 'tail', 'fence', 'void'] as const).forEach((name) =>
      t.deepEqual(
        String(first!.params[name]),
        String(second!.params[name]),
        name
      )
    );
    const pinned = recorder.events.find((event) => event.kind === 'snapshot')!;
    t.true((pinned.params.nodes as number[]).includes(env.nodes.node1));

    // independent: the model's activity set, the watermark by direct SQL at the same parameters
    const expected = model
      .accepted(one)
      .filter((entry) => {
        const isPrice = (o: ModelOutput | undefined) =>
          o?.token_category === paryon &&
          o.nonfungible_token_capability === 'mutable' &&
          o.locking_bytecode === priceScript;
        return (
          model.outputs(entry.tx).some(isPrice) ||
          model.inputs(entry.tx).some((i) => isPrice(i.outpoint))
        );
      })
      .map((entry) => entry.tx.hash);
    t.deepEqual(
      sorted(result.transactions.nodes.map((row) => row.hash)),
      sorted(expected)
    );
    const watermarkEvent = data.find(
      (event) => event.label === 'transaction_watermark'
    )!;
    const [direct] = await env.client.query<{ id: string }>(
      `SELECT toString(max(internal_id)) AS id FROM ${pinnedView(
        'transaction_at'
      )}`,
      watermarkEvent.params
    );
    t.is(result.transactionWatermark, direct!.id);
    result.transactions.nodes.forEach((row) =>
      t.true(BigInt(row.internal_id) <= BigInt(result.transactionWatermark))
    );

    // concurrent commits: every response is internally consistent (watermark >= page ids)
    const ids = await internalIds(env);
    const snapshotsBefore = env.api.db.stats.snapshots;
    const requests = 30;
    const responses = await Promise.all([
      ...Array.from({ length: requests }, async () =>
        env.gql<{
          transactions: { nodes: { internal_id: string }[] };
          transactionWatermark: string;
        }>(contractUtxoActivity, variables)
      ),
      env
        .extendNodeOne([
          {
            ...env.data.priceUpdate,
            hash: 'ab'.repeat(32),
            inputs: [
              {
                outpointIndex: 0,
                outpointTransactionHash: env.data.priceUpdate.hash,
                sequenceNumber: 0,
                unlockingBytecode: '51',
              },
            ],
            outputs: [
              {
                lockingBytecode: priceScript,
                nonfungibleTokenCapability: 'mutable',
                nonfungibleTokenCommitment: '02',
                tokenCategory: paryon,
                valueSatoshis: 1_000n,
              },
            ],
          },
        ])
        .then(() => undefined),
    ]);
    t.is(
      env.api.db.stats.snapshots - snapshotsBefore,
      requests,
      'one readSnapshot per request'
    );
    responses.forEach((response) => {
      if (response === undefined) return;
      response.transactions.nodes.forEach((row) =>
        t.true(BigInt(row.internal_id) <= BigInt(response.transactionWatermark))
      );
    });
    t.true(ids.size > 0);

    /*
     * fix pass 3: an operation naming two nodes pins both in ONE snapshot;
     * each root reads at its own node's watermark, every root shares the
     * node-agnostic part (visible0, tail, fence, void)
     */
    const twoNodes = `query($paryon: Hex!) {
      a: unspent_outputs(node: "node-one", limit: 1000, where: { token_category: { _eq: $paryon } }) { nodes { transaction_hash output_index } }
      b: unspent_outputs(node: "node-two", limit: 1000, where: { token_category: { _eq: $paryon } }) { nodes { transaction_hash output_index } }
    }`;
    interface Keys {
      nodes: { transaction_hash: string; output_index: number }[];
    }
    const keysOf = (page: Keys) =>
      sorted(
        page.nodes.map((row) => `${row.transaction_hash}:${row.output_index}`)
      );
    const pair = recordQueries(env.api);
    const snapshotsBeforePair = env.api.db.stats.snapshots;
    const both = await env.gql<{ a: Keys; b: Keys }>(twoNodes, { paryon });
    pair.stop();
    t.is(
      env.api.db.stats.snapshots - snapshotsBeforePair,
      1,
      'two nodes, one snapshot'
    );
    const single = async (node: string) =>
      (
        await env.gql<{ unspent_outputs: Keys }>(
          `query($node: String!, $paryon: Hex!) { unspent_outputs(node: $node, limit: 1000, where: { token_category: { _eq: $paryon } }) { nodes { transaction_hash output_index } } }`,
          { node, paryon }
        )
      ).unspent_outputs;
    t.deepEqual(keysOf(both.a), keysOf(await single(one)));
    t.deepEqual(keysOf(both.b), keysOf(await single(two)));
    t.notDeepEqual(keysOf(both.a), keysOf(both.b), 'the nodes differ');
    const pairData = pair.events.filter((event) => event.kind === 'data');
    t.is(pairData.length, 2);
    const byNode = new Map(
      pairData.map((event) => [Number(event.params.node), event.params])
    );
    t.deepEqual(
      [...byNode.keys()].sort(),
      [env.nodes.node1, env.nodes.node2].sort()
    );
    for (const nodeId of [env.nodes.node1, env.nodes.node2]) {
      t.is(
        String(byNode.get(nodeId)!.visible),
        String((await env.api.db.readSnapshot(nodeId)).visible),
        `node ${nodeId} reads at its own watermark`
      );
    }
    (['visible0', 'tail', 'fence', 'void'] as const).forEach((name) =>
      t.is(
        String(pairData[0]!.params[name]),
        String(pairData[1]!.params[name]),
        `shared ${name}`
      )
    );

    /*
     * no argument or input field anywhere in the schema can carry a snapshot parameter
     * (`node` is the node's name, resolved server-side to the id; the `node` view parameter is never client-set)
     */
    const forbidden = new Set<string>([
      ...snapshotParameterNames.filter((name) => name !== 'node'),
      'node_internal_id',
      'snapshot',
      'watermark',
      'commit_seq',
      'visible_seq',
    ]);
    const offending: string[] = [];
    Object.values(schema.getTypeMap()).forEach((type) => {
      if (type.name.startsWith('__')) return;
      if ('getFields' in type) {
        Object.values(type.getFields()).forEach((field) => {
          if ('args' in field) {
            field.args.forEach((arg: { name: string }) => {
              if (forbidden.has(arg.name))
                offending.push(`${type.name}.${field.name}(${arg.name})`);
            });
          }
          if (!('args' in field) && forbidden.has(field.name)) {
            offending.push(`${type.name}.${field.name} (input)`);
          }
        });
      }
    });
    t.deepEqual(offending, []);

    // a client value shaped like a parameter placeholder is data, not SQL
    const hostile = await env.gql<{ unspent_outputs: { nodes: unknown[] } }>(
      `query($v: Hex!) { unspent_outputs(node: "node-one", limit: 10, where: { locking_bytecode: { _eq: $v } }) { nodes { output_index } } }`,
      { v: Buffer.from('{visible:UInt64}').toString('hex') }
    );
    t.deepEqual(hostile.unspent_outputs.nodes, []);
    const bad = await fetch(env.server.url, {
      body: JSON.stringify({
        query: `{ unspent_outputs(node: "node-one", limit: 1, where: { locking_bytecode: { _eq: "{visible:UInt64}" } }) { has_more } }`,
      }),
      headers: { 'content-type': 'application/json' },
      method: 'POST',
    });
    t.regex(JSON.stringify(await bad.json()), /hexadecimal/u);
  }
);

/* ------------------------------------------------------------------ */
/* S4                                                                   */
/* ------------------------------------------------------------------ */

/** S4 / L36 MonitorPriceContractUTXOs as a live query. */
const monitorPriceContract = `
subscription MonitorPriceContractUTXOs($node: String!, $paryon: Hex!, $price: Hex!) {
  unspent_outputs(node: $node, limit: 5000, where: {
    token_category: { _eq: $paryon }
    nonfungible_token_capability: { _eq: MUTABLE }
    locking_bytecode: { _eq: $price }
  }) {
    nodes {
      output_index
      transaction_hash
      transaction {
        block_inclusions { block { timestamp height } }
        outputs { value_satoshis fungible_token_amount nonfungible_token_commitment nonfungible_token_capability token_category }
        inputs { outpoint { value_satoshis fungible_token_amount nonfungible_token_commitment nonfungible_token_capability token_category } }
      }
    }
  }
}`;

interface PricePayload {
  unspent_outputs: {
    nodes: {
      output_index: number;
      transaction_hash: string;
      transaction: {
        block_inclusions: { block: { height: number } }[];
        outputs: { nonfungible_token_commitment: string | null }[];
        inputs: unknown[];
      };
    }[];
  };
}

/** A subscriber's received payloads. */
interface Subscriber {
  received: PricePayload[];
  close: () => void;
}

/** SSE (GraphQL over SSE, distinct connections mode), parsed from the raw stream. */
const sseSubscriber = async (
  url: string,
  variables: { [key: string]: unknown }
): Promise<Subscriber> => {
  const controller = new AbortController();
  const received: PricePayload[] = [];
  const response = await fetch(url, {
    body: JSON.stringify({ query: monitorPriceContract, variables }),
    headers: {
      accept: 'text/event-stream',
      'content-type': 'application/json',
    },
    method: 'POST',
    signal: controller.signal,
  });
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  const pump = async () => {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      buffer += decoder.decode(value, { stream: true });
      let end = buffer.indexOf('\n\n');
      while (end !== -1) {
        const event = buffer.slice(0, end);
        buffer = buffer.slice(end + 2);
        const data = event
          .split('\n')
          .filter((line) => line.startsWith('data:'))
          .map((line) => line.slice(5).trim())
          .join('');
        if (data !== '' && event.includes('event: next')) {
          const parsed = JSON.parse(data) as { data: PricePayload };
          received.push(parsed.data);
        }
        end = buffer.indexOf('\n\n');
      }
    }
  };
  pump().catch(() => undefined);
  return {
    close: () => {
      controller.abort();
    },
    received,
  };
};

const wsSubscriber = (
  wsUrl: string,
  variables: { [key: string]: unknown }
): Subscriber => {
  const received: PricePayload[] = [];
  const client = createWsClient({
    lazy: false,
    url: wsUrl,
    webSocketImpl: WebSocket,
  });
  const unsubscribe = client.subscribe<PricePayload>(
    { query: monitorPriceContract, variables },
    {
      complete: () => undefined,
      error: () => undefined,
      next: (value) => {
        if (value.data) received.push(value.data);
      },
    }
  );
  return {
    close: () => {
      unsubscribe();
      Promise.resolve(client.dispose()).catch(() => undefined);
    },
    received,
  };
};

const eventually = async (
  condition: () => boolean,
  label: string,
  timeoutMs = 20_000
) => {
  const start = Date.now();
  while (!condition()) {
    if (Date.now() - start > timeoutMs) throw new Error(`timeout: ${label}`);
    await new Promise((resolve) => {
      setTimeout(resolve, 10);
    });
  }
};

const commitmentsOf = (payload: PricePayload | undefined) =>
  payload?.unspent_outputs.nodes.map(
    (row) =>
      row.transaction.outputs[row.output_index]?.nonfungible_token_commitment
  );

e2e(
  '[e2e] API S4: live query, 100 subscribers (50 SSE + 50 graphql-ws), one data query and one snapshot per watermark advance',
  async (t) => {
    t.timeout(120_000);
    const env = await setup(t, 's4');
    const { data, model } = env;
    const variables = { node: one, paryon, price: priceScript };
    const expectedPrice = () =>
      model
        .unspent(one)
        .filter(
          (o) =>
            o.token_category === paryon &&
            o.nonfungible_token_capability === 'mutable' &&
            o.locking_bytecode === priceScript
        )
        .map(
          (o) =>
            `${o.transaction_hash}:${o.output_index}:${o.nonfungible_token_commitment}`
        );
    const payloadKeys = (payload: PricePayload | undefined) =>
      (payload?.unspent_outputs.nodes ?? []).map(
        (row) =>
          `${row.transaction_hash}:${row.output_index}:${
            row.transaction.outputs[row.output_index]
              ?.nonfungible_token_commitment
          }`
      );

    const statsBefore = { ...env.api.db.stats };
    const subscribers: Subscriber[] = [];
    for (let index = 0; index < 50; index += 1) {
      subscribers.push(await sseSubscriber(env.server.url, variables));
      subscribers.push(wsSubscriber(env.server.wsUrl, variables));
    }
    t.teardown(() => {
      subscribers.forEach((s) => {
        s.close();
      });
    });
    await eventually(
      () => subscribers.every((s) => s.received.length >= 1),
      'initial payload'
    );
    const initialRuns = env.api.hub.stats.runs;
    t.is(env.api.hub.topicStats().length, 1, 'one topic for 100 subscribers');
    t.is(env.api.hub.topicStats()[0]!.subscribers, 100);
    t.is(initialRuns, 1, 'initial result computed once');
    t.is(
      env.api.db.stats.dataQueries - statsBefore.dataQueries,
      1,
      'one data query for 100 initial results'
    );
    subscribers.forEach((s) =>
      t.deepEqual(payloadKeys(s.received[0]), expectedPrice())
    );
    t.deepEqual(commitmentsOf(subscribers[0]!.received[0]), ['01']);

    const priceUpdate = (label: number, spend: string, commitment: string) => ({
      ...data.priceUpdate,
      hash: `${label.toString(16).padStart(2, '0')}${'cd'.repeat(31)}`,
      inputs: [
        {
          outpointIndex: 0,
          outpointTransactionHash: spend,
          sequenceNumber: 0,
          unlockingBytecode: '51',
        },
      ],
      outputs: [
        {
          lockingBytecode: priceScript,
          nonfungibleTokenCapability: 'mutable' as const,
          nonfungibleTokenCommitment: commitment,
          tokenCategory: paryon,
          valueSatoshis: 1_000n,
        },
      ],
    });

    const advance = async (
      label: string,
      txs: Parameters<Env['extendNodeOne']>[0],
      expectPush: boolean
    ) => {
      const queriesBefore = env.api.db.stats.dataQueries;
      const snapshotsBefore = env.api.db.stats.snapshots;
      const runsBefore = env.api.hub.stats.runs;
      const counts = subscribers.map((s) => s.received.length);
      const committing = Date.now();
      await env.extendNodeOne(txs);
      const started = Date.now();
      const commitMs = started - committing;
      await eventually(
        () => env.api.hub.stats.runs > runsBefore,
        `${label}: re-run`
      );
      const pushesBefore = env.api.hub.stats.pushes;
      const hubPushed = eventually(
        () => env.api.hub.stats.pushes >= pushesBefore + 100,
        'hub pushes',
        5_000
      ).then(
        () => Date.now() - started,
        () => -1
      );
      if (expectPush) {
        await eventually(
          () =>
            subscribers.every(
              (s, index) => s.received.length === counts[index]! + 1
            ),
          `${label}: all pushed`
        );
      }
      const pushMs = Date.now() - started;
      const hubMs = expectPush ? await hubPushed : -1;
      // settle: polls continue, nothing else may run
      await new Promise((resolve) => {
        setTimeout(resolve, 300);
      });
      const queries = env.api.db.stats.dataQueries - queriesBefore;
      const snapshots = env.api.db.stats.snapshots - snapshotsBefore;
      t.is(
        queries,
        1,
        `${label}: one data query per advance for 100 subscribers`
      );
      t.is(snapshots, 1, `${label}: one readSnapshot per re-run`);
      t.is(env.api.hub.stats.runs - runsBefore, 1, `${label}: one run`);
      subscribers.forEach((s, index) =>
        t.is(
          s.received.length,
          counts[index]! + (expectPush ? 1 : 0),
          `${label}: pushes`
        )
      );
      t.log(
        `${label}: ${queries} data query, ${snapshots} snapshot; ${
          expectPush
            ? 'all 100 subscribers pushed'
            : 'result unchanged, no push'
        } ${pushMs} ms after the watermark was published (hub: run ${env.api.hub.stats.lastRunMs.toFixed(
          1
        )} ms, handed to all sinks at ${hubMs} ms) (save + publish took ${commitMs} ms; poll interval 20 ms)`
      );
    };

    const p2 = priceUpdate(1, data.priceUpdate.hash, '02');
    await advance('price 01 -> 02', [p2], true);
    subscribers.forEach((s) =>
      t.deepEqual(payloadKeys(s.received.at(-1)), expectedPrice())
    );
    t.deepEqual(commitmentsOf(subscribers[1]!.received.at(-1)), ['02']);

    // an advance that does not change the result: re-run, no push
    await advance('unrelated block', [], false);

    const p3 = priceUpdate(2, p2.hash, '03');
    await advance('price 02 -> 03', [p3], true);
    subscribers.forEach((s) =>
      t.deepEqual(payloadKeys(s.received.at(-1)), expectedPrice())
    );

    // never re-delivers an older result: delivered watermarks strictly increase
    const [topic] = env.api.hub.topicStats();
    const { deliveries } = topic!;
    t.is(deliveries.length, 3);
    t.true(
      deliveries.every(
        (visible, index) => index === 0 || visible > deliveries[index - 1]!
      )
    );
    t.log(
      `watermark polls during the test: ${
        env.api.db.stats.watermarkPolls - statsBefore.watermarkPolls
      } (one query per 20 ms tick, all nodes)`
    );

    // a late subscriber gets the current result at once, without a query
    const queriesBefore = env.api.db.stats.dataQueries;
    const late = wsSubscriber(env.server.wsUrl, variables);
    t.teardown(late.close);
    await eventually(() => late.received.length === 1, 'late subscriber');
    t.deepEqual(payloadKeys(late.received[0]), expectedPrice());
    t.is(env.api.db.stats.dataQueries, queriesBefore);

    // node-two's subscription is a separate topic; node-one's advances do not re-run it
    const other = wsSubscriber(env.server.wsUrl, { ...variables, node: two });
    t.teardown(other.close);
    await eventually(() => other.received.length === 1, 'node-two subscriber');
    t.deepEqual(commitmentsOf(other.received[0]), ['00']);
  }
);

e2e(
  '[e2e] fix pass 3: request snapshots are reused while the watermarks stand still and never trail the watermark at request start',
  async (t) => {
    const env = await setup(t, 'cache');
    const variables = { limit: 5000, node: one, paryon };
    const first = await env.gql(allHolders, variables);
    const hits = env.api.db.stats.snapshotCacheHits;
    const recorder = recordQueries(env.api);
    const second = await env.gql(allHolders, variables);
    recorder.stop();
    t.is(env.api.db.stats.snapshotCacheHits, hits + 1, 'served by the cache');
    t.deepEqual(second, first);
    t.deepEqual(
      recorder.events
        .filter((event) => event.kind === 'snapshot')
        .map((event) => event.sql.includes('void_seqs')),
      [false],
      'one cheap watermark query, no full snapshot read'
    );
    // a commit advances node-one's watermark: the next request reads a new snapshot
    await env.extendNodeOne([]);
    const published = await readWatermark(env.client, env.nodes.node1);
    const after = recordQueries(env.api);
    await env.gql(allHolders, variables);
    after.stop();
    t.is(env.api.db.stats.snapshotCacheHits, hits + 1, 'a miss');
    const data = after.events.find((event) => event.kind === 'data')!;
    t.true(
      BigInt(String(data.params.visible)) >= published,
      'not staler than the watermark published before the request'
    );
  }
);

/* ------------------------------------------------------------------ */
/* overhead                                                             */
/* ------------------------------------------------------------------ */

const percentile = (values: readonly number[], p: number) => {
  const ordered = [...values].sort((a, b) => a - b);
  return ordered[
    Math.min(ordered.length - 1, Math.floor((ordered.length - 1) * p))
  ]!;
};

e2e(
  '[e2e] API overhead vs direct SQL, p50 over 200 warm requests (S3, S5)',
  async (t) => {
    t.timeout(120_000);
    const env = await setup(t, 'cost', 'off');
    const runs = 200;
    const report: string[] = [];
    for (const [label, query] of [
      ['S5 AllHolders', allHolders],
      ['S3 HolderBalances', holderBalances],
    ] as const) {
      const variables = { limit: 5000, node: one, paryon };
      // capture the exact statement and parameters of one request
      const recorder = recordQueries(env.api);
      await env.gql(query, variables);
      recorder.stop();
      const statement = recorder.events.find((event) => event.kind === 'data')!;
      // warm up
      for (let index = 0; index < 20; index += 1) {
        await env.gql(query, variables);
        await env.client.query(statement.sql, statement.params);
      }
      const api: number[] = [];
      const sql: number[] = [];
      const snapshotAndSql: number[] = [];
      const cachedAndSql: number[] = [];
      const snapshotOnly: number[] = [];
      const cachedOnly: number[] = [];
      for (let index = 0; index < runs; index += 1) {
        let start = performance.now();
        await env.gql(query, variables);
        api.push(performance.now() - start);
        start = performance.now();
        await env.client.query(statement.sql, statement.params);
        sql.push(performance.now() - start);
        start = performance.now();
        await env.api.db.readSnapshot(env.nodes.node1);
        snapshotOnly.push(performance.now() - start);
        await env.client.query(statement.sql, statement.params);
        snapshotAndSql.push(performance.now() - start);
        start = performance.now();
        await env.api.db.pinnedSnapshot([env.nodes.node1]);
        cachedOnly.push(performance.now() - start);
        await env.client.query(statement.sql, statement.params);
        cachedAndSql.push(performance.now() - start);
      }
      const p50 = {
        api: percentile(api, 0.5),
        cachedAndSql: percentile(cachedAndSql, 0.5),
        cachedOnly: percentile(cachedOnly, 0.5),
        snapshotAndSql: percentile(snapshotAndSql, 0.5),
        snapshotOnly: percentile(snapshotOnly, 0.5),
        sql: percentile(sql, 0.5),
      };
      const overhead = p50.api - p50.snapshotAndSql;
      report.push(
        `${label}: p50 API ${p50.api.toFixed(
          2
        )} ms, direct SQL ${p50.sql.toFixed(
          2
        )} ms, readSnapshot + SQL ${p50.snapshotAndSql.toFixed(
          2
        )} ms; overhead vs snapshot+SQL ${overhead.toFixed(
          2
        )} ms, vs SQL alone ${(p50.api - p50.sql).toFixed(
          2
        )} ms; p95 API ${percentile(api, 0.95).toFixed(
          2
        )} ms; full readSnapshot ${p50.snapshotOnly.toFixed(
          2
        )} ms, cached request snapshot ${p50.cachedOnly.toFixed(
          2
        )} ms, cached snapshot + SQL ${p50.cachedAndSql.toFixed(2)} ms`
      );
      t.true(
        overhead <= 5,
        `${label}: API overhead p50 ${overhead.toFixed(2)} ms <= 5 ms`
      );
    }
    report.forEach((line) => {
      t.log(line);
    });
  }
);
