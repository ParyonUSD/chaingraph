/* eslint-disable functional/no-loop-statement, functional/no-let, no-await-in-loop, functional/no-throw-statement, @typescript-eslint/no-magic-numbers, require-atomic-updates, @typescript-eslint/naming-convention */
// cspell:ignore clickhouse
/**
 * The two-node scratch dataset of the API specs (spec-dataset.ts) written
 * through the ClickHouse store, with an API server on it.
 */
import type { ExecutionContext } from 'ava';

import type { ClickHouseStore } from '../store/clickhouse/clickhouse-store.js';
import type { ClickHouseClient } from '../store/clickhouse/client.js';
import {
  acceptance,
  notSaved,
  registerNodes,
  scratch,
} from '../store/clickhouse/spec-fixtures.js';

import type { QueryEvent } from './db.js';
import type { Api, RunningServer } from './server.js';
import { createApi, listen } from './server.js';
import type { Dataset } from './spec-dataset.js';
import { buildDataset, Model } from './spec-dataset.js';

export const one = 'node-one';
export const two = 'node-two';

export interface Env {
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
  /** The raw response (data and errors). */
  gqlRaw: (
    query: string,
    variables?: { [key: string]: unknown }
  ) => Promise<{
    data?: { [key: string]: unknown };
    errors?: { message: string }[];
  }>;
}

export const setup = async (
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
  const gqlRaw = async (
    query: string,
    variables: { [key: string]: unknown } = {}
  ) => {
    const response = await fetch(server.url, {
      body: JSON.stringify({ query, variables }),
      headers: { 'content-type': 'application/json' },
      method: 'POST',
    });
    return (await response.json()) as {
      data?: { [key: string]: unknown };
      errors?: { message: string }[];
    };
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
    gqlRaw,
    model,
    nodes,
    server,
    store,
  };
};

/** Every query an API request sends, recorded. */
export const recordQueries = (api: Api) => {
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
