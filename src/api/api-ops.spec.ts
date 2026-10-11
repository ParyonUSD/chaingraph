/* eslint-disable @typescript-eslint/no-magic-numbers, camelcase, @typescript-eslint/naming-convention, functional/no-loop-statement, functional/no-let, no-await-in-loop, complexity, functional/no-throw-statement, sort-keys, functional/no-mixed-type, @typescript-eslint/no-unnecessary-condition, @typescript-eslint/no-loop-func */
// cspell:ignore clickhouse sidecar
/**
 * Phase 2 G5 (docs/clickhouse-port/phase2-api.md): the 12 live queries of
 * the library and chaingraph.cash on the shared watermark hub (R31), change
 * signals and the watermark root (R32), the row cap (R33), value types,
 * query limits and trusted documents (R34), the watermark-keyed response
 * cache (R37) and send_transaction (R38), on the two-node scratch dataset.
 */
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';

import test from 'ava';
import { createClient as createWsClient } from 'graphql-ws';
import WebSocket from 'ws';

import { makeTx, p2pkh } from '../store/clickhouse/spec-fixtures.js';
import { e2eClickHouseUrl } from '../store/clickhouse/test-support.js';

import { documentText, loadCatalogue } from './catalogue.js';
import { documentHash } from './plugins.js';
import { createApi, listen } from './server.js';
import {
  loanScript,
  paryon,
  priceScript,
  redeemer,
  user1,
  user2,
} from './spec-dataset.js';
import type { Env } from './spec-env.js';
import { one, recordQueries, setup } from './spec-env.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

interface Data {
  [key: string]: unknown;
}

interface Subscriber {
  received: Data[];
  close: () => void;
}

const sse = async (
  url: string,
  body: { [key: string]: unknown }
): Promise<Subscriber> => {
  const controller = new AbortController();
  const received: Data[] = [];
  const response = await fetch(url, {
    body: JSON.stringify(body),
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
          received.push((JSON.parse(data) as { data: Data }).data);
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

const ws = (
  url: string,
  payload: { query: string; variables?: Data; extensions?: Data }
): Subscriber => {
  const received: Data[] = [];
  const client = createWsClient({ lazy: false, url, webSocketImpl: WebSocket });
  const unsubscribe = client.subscribe<Data>(payload as never, {
    complete: () => undefined,
    error: (error) => {
      received.push({ error: String((error as Error).message ?? error) });
    },
    next: (value) => {
      if (value.data) received.push(value.data);
      if (value.errors) received.push({ error: JSON.stringify(value.errors) });
    },
  });
  return {
    close: () => {
      unsubscribe();
      Promise.resolve(client.dispose()).catch(() => undefined);
    },
    received,
  };
};

const eventually = async (condition: () => boolean, label: string) => {
  const start = Date.now();
  while (!condition()) {
    if (Date.now() - start > 20_000) throw new Error(`timeout: ${label}`);
    await new Promise((resolve) => {
      setTimeout(resolve, 10);
    });
  }
};

/** The 12 live documents of the catalogue with scratch-dataset variables. */
const liveDocuments = () => {
  const catalogue = new Map(loadCatalogue().map((entry) => [entry.id, entry]));
  const variables: { [id: string]: Data } = {
    L35: {
      paryonTokenId: paryon,
      loanContractLockingBytecode: loanScript,
      limit: 10_000,
    },
    L36: {
      paryonTokenId: paryon,
      priceContractLockingBytecode: priceScript,
      limit: 5_000,
    },
    L37: {
      redemptionTokenId: redeemer,
      redemptionContractLockingBytecode: user2,
      limit: 5_000,
    },
    L38: { poolTokenId: paryon, poolLockingBytecode: loanScript },
    L39: { poolTokenId: paryon, poolLockingBytecode: loanScript },
    L40: { paryonTokenId: paryon, loanContractLockingBytecode: loanScript },
    L41: {},
    L44: { paryonTokenId: paryon, loanContractLockingBytecode: loanScript },
    L45: { tokenId: paryon, lockingBytecode: priceScript },
    X1: { limit: 5 },
    X2: {},
    X3: { lockingBytecodes: [user1] },
  };
  return Object.entries(variables).map(([id, values]) => {
    const entry = catalogue.get(id)!;
    const query = documentText(entry.file);
    if (!/^\s*subscription\b/u.test(query))
      throw new Error(`${id} is not live`);
    return {
      id,
      query,
      twin: query.replace(/^\s*subscription\b/u, 'query'),
      variables: { ...values, node: one },
    };
  });
};

/** Drop per-sync timestamps (validated_at and co change on every save). */
const stable = (data: unknown) =>
  JSON.stringify(data, (key, value: unknown) =>
    key.endsWith('_at') ? null : value
  );

e2e(
  '[e2e] G5 R31 + R32: the 12 live queries (library + chaingraph.cash) on the shared hub: initial results equal their query twins; one advance re-runs each topic once on one snapshot and pushes exactly the changed results',
  async (t) => {
    const env = await setup(t, 'g5_live');
    const documents = liveDocuments();
    const subscribers = await Promise.all(
      documents.map(async (document, index) =>
        index % 2 === 0
          ? sse(env.server.url, {
              query: document.query,
              variables: document.variables,
            })
          : ws(env.server.wsUrl, {
              query: document.query,
              variables: document.variables,
            })
      )
    );
    // a second subscriber of one document joins the same topic
    const twinSubscriber = ws(env.server.wsUrl, {
      query: documents[1]!.query,
      variables: documents[1]!.variables,
    });
    t.teardown(() => {
      [...subscribers, twinSubscriber].forEach((subscriber) => {
        subscriber.close();
      });
    });
    await eventually(
      () =>
        [...subscribers, twinSubscriber].every(
          (subscriber) => subscriber.received.length >= 1
        ),
      'initial payloads'
    );
    t.is(
      env.api.hub.topicStats().length,
      documents.length,
      'one topic per document (two subscribers share one)'
    );
    const before = await Promise.all(
      documents.map(async (document) =>
        env.gql(document.twin, document.variables)
      )
    );
    documents.forEach((document, index) => {
      t.is(
        stable(subscribers[index]!.received[0]),
        stable(before[index]),
        `${document.id} initial = query twin`
      );
      t.false(
        JSON.stringify(subscribers[index]!.received[0]).includes('"error"'),
        document.id
      );
    });
    // watermark (R32) stands still without a commit
    const marks = async () =>
      (
        await env.gql<{ watermark: { visible: string } }>(
          `{ watermark(node: "node-one") { visible } }`
        )
      ).watermark.visible;
    const mark0 = await marks();
    t.is(await marks(), mark0);

    // one block on node-one: a price update spending the current price NFT, and a payment to user1
    const statsBefore = { ...env.api.db.stats };
    const runsBefore = env.api.hub.stats.runs;
    const priceUpdate2 = makeTx({
      label: 'g5-price-2',
      outputs: [
        {
          lockingBytecode: priceScript,
          nonfungibleTokenCapability: 'mutable',
          nonfungibleTokenCommitment: '02',
          tokenCategory: paryon,
          valueSatoshis: 1_000n,
        },
        { lockingBytecode: user1, valueSatoshis: 777n },
      ],
      spends: [[env.data.priceUpdate.hash, 0]],
    });
    const loanUpdate = makeTx({
      label: 'g5-loan-2',
      outputs: [
        {
          lockingBytecode: loanScript,
          nonfungibleTokenCapability: 'mutable',
          nonfungibleTokenCommitment: '01bd',
          tokenCategory: paryon,
          valueSatoshis: 1_000n,
        },
      ],
      spends: [[env.data.interest.hash, 0]],
    });
    await env.extendNodeOne([priceUpdate2, loanUpdate]);
    t.true(BigInt(await marks()) > BigInt(mark0), 'the watermark moved');
    await eventually(
      () => env.api.hub.stats.runs >= runsBefore + documents.length,
      'one run per topic'
    );
    const after = await Promise.all(
      documents.map(async (document) =>
        env.gql(document.twin, document.variables)
      )
    );
    const changed = documents.filter(
      (_, index) => stable(before[index]) !== stable(after[index])
    );
    t.true(
      changed.length >= 5,
      `${changed.map((document) => document.id).join(', ')} changed`
    );
    await eventually(
      () =>
        documents.every((document, index) =>
          changed.includes(document)
            ? subscribers[index]!.received.length >= 2
            : true
        ),
      'pushes of changed results'
    );
    await new Promise((resolve) => {
      setTimeout(resolve, 300);
    });
    documents.forEach((document, index) => {
      const { received } = subscribers[index]!;
      if (changed.includes(document)) {
        t.is(received.length, 2, `${document.id} pushed once`);
        t.is(
          stable(received[1]),
          stable(after[index]),
          `${document.id} = query twin after the block`
        );
      } else {
        t.is(received.length, 1, `${document.id} unchanged: no push`);
      }
    });
    t.is(
      twinSubscriber.received.length,
      subscribers[1]!.received.length,
      'shared topic, same pushes'
    );
    t.is(
      env.api.hub.stats.runs - runsBefore,
      documents.length,
      'one run per topic'
    );
    /* snapshots since: the hub's one for the advance, plus one per request made here (marks, query twins) */
    t.is(
      env.api.db.stats.snapshots - statsBefore.snapshots,
      1 + 1 + documents.length,
      'one snapshot for the advance'
    );
  }
);

e2e(
  '[e2e] G5 R33 + R34: row cap error, value types, query limits (depth, aliases, root fields, fields)',
  async (t) => {
    const env = await setup(t, 'g5_limits');
    const types = await env.gql<{
      transactions_by_hash: {
        internal_id: unknown;
        version: unknown;
        input_count: unknown;
        fee_satoshis: unknown;
        outputs: { output_index: unknown; value_satoshis: unknown }[];
        inputs: { input_index: unknown; outpoint_index: unknown }[];
      }[];
      blocks: { nodes: { height: unknown; timestamp: unknown }[] };
    }>(
      `query($hash: Hex!) {
        transactions_by_hash(hashes: [$hash]) { internal_id version input_count fee_satoshis outputs { output_index value_satoshis } inputs { input_index outpoint_index } }
        blocks(node: "node-one", limit: 1) { nodes { height timestamp } }
      }`,
      { hash: env.data.c0.hash }
    );
    const [tx] = types.transactions_by_hash;
    t.is(typeof tx!.internal_id, 'string');
    t.is(typeof tx!.version, 'number');
    t.is(typeof tx!.input_count, 'number');
    t.is(typeof tx!.fee_satoshis, 'string');
    t.is(typeof tx!.outputs[0]!.output_index, 'number');
    t.is(typeof tx!.outputs[0]!.value_satoshis, 'string');
    t.is(
      tx!.inputs[0]!.outpoint_index,
      '4294967295',
      'a coinbase outpoint index is a BigInt'
    );
    t.is(typeof types.blocks.nodes[0]!.height, 'number');
    t.is(typeof types.blocks.nodes[0]!.timestamp, 'string');

    const refused = async (query: string, pattern: RegExp) => {
      const result = await env.gqlRaw(query);
      t.regex(result.errors?.[0]?.message ?? '', pattern, query.slice(0, 80));
    };
    let deep = 'hash';
    for (let level = 0; level < 9; level += 1) {
      deep = `outputs { transaction { ${deep} } }`;
    }
    await refused(
      `{ transactions_by_hash(hashes: ["${env.data.c0.hash}"]) { ${deep} } }`,
      /Query depth \d+ exceeds 16/u
    );
    await refused(
      `{ ${Array.from(
        { length: 65 },
        (_, index) => `a${index}: transaction_watermark`
      ).join(' ')} }`,
      /aliases exceed 64/u
    );
    await refused(
      `{ transaction_watermark ${Array.from(
        { length: 32 },
        (_, index) => `n${index}: nodes { name }`
      ).join(' ')} }`,
      /root fields exceed 32|aliases exceed/u
    );
    await refused(
      `{ nodes { ${Array.from({ length: 2_001 }, () => 'name').join(' ')} } }`,
      /fields exceed 2000/u
    );
    // R33: more rows than the cap (all levels) is an error, never a silent cut
    const capped = createApi(env.client, { maxResultRows: 10 });
    const server = await listen(capped);
    t.teardown(async () => server.close());
    const response = await fetch(server.url, {
      body: JSON.stringify({
        query: `{ transactions(node: "node-one", limit: 3) { nodes { outputs { output_index } } } }`,
      }),
      headers: { 'content-type': 'application/json' },
      method: 'POST',
    });
    const body = (await response.json()) as {
      errors?: { message: string; extensions?: { code?: string } }[];
    };
    t.is(body.errors?.[0]?.extensions?.code, 'RESULT_TOO_LARGE');
  }
);

e2e(
  '[e2e] G5 R34 trusted documents (HTTP and graphql-ws) + R37 watermark-keyed response cache + R38 send_transaction through the agent',
  async (t) => {
    const env: Env = await setup(t, 'g5_ops');
    // trusted documents only
    const tip =
      'query Tip($node: String!) { block_tip(node: $node) { height } }';
    const live =
      'subscription LiveTip($node: String!) { block_tip(node: $node) { height } }';
    const trustedApi = createApi(env.client, {
      trustedDocuments: {
        allowArbitraryDocuments: false,
        documents: new Map([
          [documentHash(tip), tip],
          [documentHash(live), live],
        ]),
      },
    });
    const trusted = await listen(trustedApi);
    t.teardown(async () => trusted.close());
    const post = async (url: string, body: Data) =>
      (await (
        await fetch(url, {
          body: JSON.stringify(body),
          headers: { 'content-type': 'application/json' },
          method: 'POST',
        })
      ).json()) as {
        data?: Data;
        errors?: { message: string; extensions?: { code?: string } }[];
      };
    const byHash = await post(trusted.url, {
      extensions: {
        persistedQuery: { sha256Hash: documentHash(tip), version: 1 },
      },
      variables: { node: one },
    });
    t.deepEqual(byHash.data, { block_tip: { height: 2 } });
    t.is(
      (await post(trusted.url, { query: tip, variables: { node: one } }))
        .errors?.[0]?.extensions?.code,
      'PERSISTED_QUERY_ONLY'
    );
    t.is(
      (
        await post(trusted.url, {
          documentId: 'ab'.repeat(32),
          variables: { node: one },
        })
      ).errors?.[0]?.extensions?.code,
      'PERSISTED_QUERY_NOT_FOUND'
    );
    t.is(
      (
        await post(trusted.url, {
          extensions: { persistedQuery: { sha256Hash: documentHash(tip) } },
          query: live,
          variables: { node: one },
        })
      ).errors?.[0]?.extensions?.code,
      'PERSISTED_QUERY_MISMATCH'
    );
    const wsTrusted = ws(trusted.wsUrl, {
      extensions: {
        persistedQuery: { sha256Hash: documentHash(live), version: 1 },
      },
      query: '',
      variables: { node: one },
    } as never);
    t.teardown(wsTrusted.close);
    await eventually(
      () => wsTrusted.received.length >= 1,
      'trusted subscription'
    );
    t.deepEqual(wsTrusted.received[0], { block_tip: { height: 2 } });

    // response cache: a repeated request costs one watermark read; a commit invalidates
    const cachedApi = createApi(env.client, { responseCache: {} });
    const cached = await listen(cachedApi);
    t.teardown(async () => cached.close());
    const document = `query($node: String!, $paryon: Hex!) {
      unspent_outputs(node: $node, limit: 100, where: { token_category: { _eq: $paryon } }) { nodes { transaction_hash output_index } }
      watermark(node: $node) { visible }
    }`;
    const variables = { node: one, paryon };
    const first = await post(cached.url, { query: document, variables });
    const stats0 = { ...cachedApi.db.stats };
    const second = await post(cached.url, { query: document, variables });
    t.deepEqual(second, first);
    t.is(
      cachedApi.db.stats.dataQueries - stats0.dataQueries,
      0,
      'no data query'
    );
    t.is(
      cachedApi.db.stats.snapshots - stats0.snapshots,
      0,
      'no snapshot read'
    );
    t.is(
      cachedApi.db.stats.watermarkPolls - stats0.watermarkPolls,
      1,
      'one watermark read'
    );
    t.is(cachedApi.responseCache!.stats.hits, 1);
    await env.extendNodeOne([
      makeTx({
        label: 'g5-cache',
        outputs: [{ lockingBytecode: p2pkh('g5'), valueSatoshis: 1n }],
        spends: [[env.data.c0.hash, 24]],
      }),
    ]);
    const third = await post(cached.url, { query: document, variables });
    t.not(
      JSON.stringify(third),
      JSON.stringify(first),
      'recomputed after the commit'
    );
    const fresh = await env.gql(document, variables);
    t.deepEqual(third.data, fresh);
    t.is(cachedApi.responseCache!.stats.misses, 2);
    // node connection metadata is not cached
    await post(cached.url, { query: '{ nodes { name user_agent } }' });
    await post(cached.url, { query: '{ nodes { name user_agent } }' });
    t.is(cachedApi.responseCache!.stats.skipped, 2);

    // send_transaction: relayed to the agent's internal API with the node's internal id
    const requests: Data[] = [];
    const agent = createServer((request, response) => {
      let body = '';
      request.on('data', (chunk: Buffer) => {
        body += chunk.toString();
      });
      request.on('end', () => {
        requests.push({ body: JSON.parse(body) as Data, url: request.url });
        response.writeHead(200, { 'content-type': 'application/json' });
        response.end(
          JSON.stringify({
            transaction_hash: 'cd'.repeat(32),
            transmission_success: true,
            validation_success: true,
          })
        );
      });
    });
    await new Promise<void>((resolve) => {
      agent.listen(0, '127.0.0.1', resolve);
    });
    t.teardown(() => agent.close());
    const sendApi = createApi(env.client, {
      agentUrl: `http://127.0.0.1:${(agent.address() as AddressInfo).port}`,
    });
    const send = await listen(sendApi);
    t.teardown(async () => send.close());
    const sent = await post(send.url, {
      query: `mutation($tx: Hex!) { send_transaction(node: "node-two", encoded_hex: $tx) { transaction_hash validation_success transmission_success transmission_error_message } }`,
      variables: { tx: '0200' },
    });
    t.deepEqual(sent.data, {
      send_transaction: {
        transaction_hash: 'cd'.repeat(32),
        transmission_error_message: null,
        transmission_success: true,
        validation_success: true,
      },
    });
    t.deepEqual(requests, [
      {
        body: {
          action: { name: 'send_transaction' },
          input: {
            request: { encoded_hex: '0200', node_internal_id: env.nodes.node2 },
          },
          request_query: '',
          session_variables: { 'x-hasura-role': 'public' },
        },
        url: '/send-transaction',
      },
    ]);
    const unconfigured = await env.gqlRaw(
      'mutation { send_transaction(node: "node-one", encoded_hex: "00") { transaction_hash } }'
    );
    t.regex(unconfigured.errors?.[0]?.message ?? '', /not configured/u);
    t.truthy(recordQueries);
  }
);
