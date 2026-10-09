/**
 * Ingestion gate scenarios. Each scenario gets a fresh database (all
 * migrations – or, with `--store clickhouse`, the ClickHouse DDL – of the
 * agent under test), fresh mock nodes and a fresh agent,
 * waits until the agent reaches steady state (initial sync done, managed
 * indexes built, mempool tracking enabled – i.e. the production write path,
 * with triggers enabled), then measures.
 *
 * Throughput is always `block_transaction rows for the measured blocks / wall
 * clock`, never the agent's own "active seconds" statistics.
 *
 * Every database read goes through `context.backend` (lib/postgres.mjs or
 * lib/clickhouse.mjs, same surface); a "client" below is that backend's
 * session.
 */
import { AgentProcess } from './agent.mjs';
import { assembleBlock, doubleSha256, generateTransactionPayload, loadOrGenerateBlockSequence } from './fixtures.mjs';
import { genesisBlockRaw, genesisFromRaw, makeBlock, MockNode, testnetGenesisBlockRaw } from './mock-node.mjs';
import { waitFor } from './postgres.mjs';

/* eslint-disable no-bitwise */
const magicFromLabel = (label) => Buffer.from(label, 'utf8').map((byte) => byte | 128).toString('hex');
// cspell:disable-next-line
const mainnetLikeMagic = magicFromLabel('grph');
// cspell:disable-next-line
const chipnetLikeMagic = magicFromLabel('grpc');
/* eslint-enable no-bitwise */

const databaseName = 'chaingraph_ingestion_gate';
const baseChainLength = 5;
let nonceCounter = 1;
let nextPort = 19433;

const nowSeconds = () => Math.floor(Date.now() / 1000);

/** Turn cached payloads into linked blocks on top of `previousHash`. */
const linkBlocks = (payloadBlocks, previousHash) => {
  const blocks = [];
  let previous = previousHash;
  const time = nowSeconds();
  payloadBlocks.forEach((payloadBlock) => {
    const { header, raw } = assembleBlock({ nonce: nonceCounter++, payload: payloadBlock.payload, previousHash: previous, time });
    const block = makeBlock({ header, raw, stats: payloadBlock.stats });
    blocks.push(block);
    previous = block.hash;
  });
  return blocks;
};

/** A tiny, recent base chain so initial sync completes in seconds. */
const makeBaseChain = (genesis, seed) => {
  const payloads = Array.from({ length: baseChainLength }, (_, index) =>
    generateTransactionPayload({ seed: `${seed}:base:${index}`, transactionCount: 2 })
  );
  return linkBlocks(payloads, genesis.hash);
};

/** Run async work one item at a time (a pg.Client runs one query at a time). */
const sequential = async (items, work) => {
  const results = [];
  for (const item of items) results.push(await work(item));
  return results;
};

const sumTransactions = (blocks) => blocks.reduce((total, block) => total + block.stats.transactions, 0);
const sumBytes = (blocks) => blocks.reduce((total, block) => total + block.raw.length, 0);
const seconds = (ms) => ms / 1000;

/**
 * Start mock nodes (each with its genesis + base chain), a fresh DB and the
 * agent; resolve once the agent is in steady state.
 */
const startEnvironment = async (context, label, nodeSpecs) => {
  const { backend } = context;
  const client = await backend.recreateDatabase({ agentDirectory: context.agentDirectory, baseUrl: context.baseUrl, databaseName });
  const nodes = nodeSpecs.map((spec) => {
    const node = new MockNode({ genesis: spec.genesis, magicHex: spec.magicHex, name: spec.name, port: nextPort++ });
    node.extend(spec.baseChain);
    node.listen();
    return node;
  });
  const genesisBlocks = [...new Map(nodeSpecs.map((spec) => [spec.magicHex, `${spec.magicHex}:${spec.genesisRaw}`])).values()].join(',');
  const agent = new AgentProcess({
    agentDirectory: context.agentDirectory,
    environment: backend.agentEnvironment({ baseUrl: context.baseUrl, databaseName }),
    genesisBlocks,
    label,
    runDirectory: context.runDirectory,
    settings: context.settings,
    trustedNodes: nodes.map((node) => node.trustedNodeEntry()).join(','),
  });
  const cleanup = async () => {
    await agent.stop();
    nodes.forEach((node) => node.close());
    await backend.closeSession(client);
  };
  try {
    // fail fast if the agent exits before connecting (e.g. an unsupported store)
    const agentExited = agent.exitPromise.then((code) => {
      throw new Error(`agent ${label} exited (code ${code}) before connecting to the mock nodes\n--- last output ---\n${agent.stdoutBuffer.slice(-3000)}`);
    });
    await Promise.race([Promise.all(nodes.map((node) => node.waitForPeer())), agentExited]);
    agentExited.catch(() => {});
    await agent.waitForSteadyState();
    // let post-sync housekeeping (incomplete block repair, expiration scan) settle
    await new Promise((resolve) => setTimeout(resolve, 1000));
  } catch (error) {
    await cleanup();
    throw error;
  }
  return { agent, cleanup, client, nodes };
};

const mainnetLikeSpec = (seed, name = 'mainnet_like') => {
  const genesis = genesisFromRaw(genesisBlockRaw);
  return { baseChain: makeBaseChain(genesis, `${seed}:${name}`), genesis, genesisRaw: genesisBlockRaw, magicHex: mainnetLikeMagic, name };
};
const chipnetLikeSpec = (seed, name = 'chipnet_like') => {
  const genesis = genesisFromRaw(testnetGenesisBlockRaw);
  return { baseChain: makeBaseChain(genesis, `${seed}:${name}`), genesis, genesisRaw: testnetGenesisBlockRaw, magicHex: chipnetLikeMagic, name };
};

/** Shared measurement: announce, wait until all accepted, collect metrics. */
const measureIngestion = async ({ backend, environment, node, blocks, announce, timeoutMs, pollMs = 25 }) => {
  const { client, agent } = environment;
  const { acceptedBlockCount, blockTransactionCount, countRows } = backend;
  const hashes = blocks.map((block) => block.hash);
  const metricsStart = await backend.writeMetricsStart(client);
  const transactionRowsBefore = await countRows(client, 'transaction');
  const started = Date.now();
  announce();
  const finished = await waitFor(async () => (await acceptedBlockCount(client, node.name, hashes)) === hashes.length, {
    description: `${node.name} accepting ${hashes.length} block(s)`,
    intervalMs: pollMs,
    timeoutMs,
  });
  const wallMs = finished - started;
  const writeMetrics = await backend.writeMetricsSince(client, metricsStart);
  const blockTransactionRows = await blockTransactionCount(client, hashes);
  const newTransactionRows = (await countRows(client, 'transaction')) - transactionRowsBefore;
  const expectedTransactions = sumTransactions(blocks);
  const heap = agent.heapPeak(started, finished);
  return {
    blockBytes: sumBytes(blocks),
    blockTransactionRows,
    blocks: blocks.length,
    correct: blockTransactionRows === expectedTransactions,
    expectedTransactions,
    heap,
    newTransactionRows,
    transactionsPerSecond: blockTransactionRows / seconds(wallMs),
    ...writeMetrics,
    wallSeconds: seconds(wallMs),
  };
};

export const maxBlockScenario = async (context) => {
  const [payload] = loadOrGenerateBlockSequence({
    blockCount: 1,
    cacheDirectory: context.cacheDirectory,
    log: context.log,
    name: 'dense',
    seed: context.seed,
    transactionsPerBlock: context.settings.denseTransactionsPerBlock,
  });
  const spec = mainnetLikeSpec(context.seed);
  const environment = await startEnvironment(context, 'max-block', [spec]);
  try {
    const [node] = environment.nodes;
    const blocks = linkBlocks([payload], node.tip().hash);
    context.log(`max-block: announcing ${(blocks[0].raw.length / 1e6).toFixed(2)} MB block with ${blocks[0].stats.transactions} txs`);
    const result = await measureIngestion({ announce: () => node.announceViaHeaders(blocks), backend: context.backend, blocks, environment, node, timeoutMs: 600_000 });
    return { ...result, peakHeapBytes: result.heap.peakHeapUsed };
  } finally {
    await environment.cleanup();
  }
};

export const burstScenario = async (context) => {
  const payloads = loadOrGenerateBlockSequence({
    blockCount: 3,
    cacheDirectory: context.cacheDirectory,
    log: context.log,
    name: 'burst',
    seed: context.seed,
    transactionsPerBlock: context.settings.denseTransactionsPerBlock,
  });
  const environment = await startEnvironment(context, 'burst', [mainnetLikeSpec(context.seed)]);
  try {
    const [node] = environment.nodes;
    const blocks = linkBlocks(payloads, node.tip().hash);
    context.log(`burst: announcing 3 blocks (${(sumBytes(blocks) / 1e6).toFixed(2)} MB) back-to-back`);
    const result = await measureIngestion({ announce: () => node.announceViaHeaders(blocks), backend: context.backend, blocks, environment, node, timeoutMs: 1_200_000 });
    return { ...result, drainSeconds: result.wallSeconds, peakHeapBytes: result.heap.peakHeapUsed };
  } finally {
    await environment.cleanup();
  }
};

export const reorgScenario = async (context) => {
  const { reorgTransactionsPerBlock } = context.settings;
  const branchA = loadOrGenerateBlockSequence({ blockCount: 100, cacheDirectory: context.cacheDirectory, log: context.log, name: 'reorg-a', seed: context.seed, transactionsPerBlock: reorgTransactionsPerBlock });
  const branchBTail = loadOrGenerateBlockSequence({ blockCount: 100, cacheDirectory: context.cacheDirectory, log: context.log, name: 'reorg-b', seed: context.seed, transactionsPerBlock: reorgTransactionsPerBlock });
  // B[0] confirms 30 transactions that were first seen in both mempools
  const confirmedMempool = generateTransactionPayload({ mempoolTransactionCount: 30, seed: `${context.seed}:reorg-mempool-confirmed`, transactionCount: 30 });
  const unconfirmedMempool = generateTransactionPayload({ mempoolTransactionCount: 10, seed: `${context.seed}:reorg-mempool-unconfirmed`, transactionCount: 10 });
  const specA = mainnetLikeSpec(context.seed, 'reorg_node_1');
  const specB = { ...specA, name: 'reorg_node_2' };
  const environment = await startEnvironment(context, 'reorg', [specA, specB]);
  const { acceptedBlockCount, acceptedChain, blockTransactionCount, confirmedButInMempoolCount, historyNodeCount, mempoolRowCount } = context.backend;
  try {
    const { client, nodes } = environment;
    const forkHeight = nodes[0].chain.length - 1;
    const forkHash = nodes[0].tip().hash;
    const chainA = linkBlocks(branchA, forkHash);
    const chainB = linkBlocks([{ payload: confirmedMempool.payload, stats: confirmedMempool.stats }, ...branchBTail], forkHash);
    const chainAHashes = chainA.map((block) => block.hash);
    const chainBHashes = chainB.map((block) => block.hash);

    context.log('reorg: syncing 100-block branch A on both nodes');
    nodes.forEach((node) => node.appendViaInventory(chainA));
    await waitFor(async () => (await sequential(nodes, (node) => acceptedBlockCount(client, node.name, chainAHashes))).every((count) => count === 100), { description: 'branch A accepted by both nodes', intervalMs: 100, timeoutMs: 600_000 });

    const toHash = (raw) => Buffer.from(raw).reverse().toString('hex');
    const confirmedHashes = confirmedMempool.mempoolTransactions.map((raw) => toHash(doubleSha256(raw)));
    const unconfirmedHashes = unconfirmedMempool.mempoolTransactions.map((raw) => toHash(doubleSha256(raw)));
    nodes.forEach((node) => {
      [...confirmedMempool.mempoolTransactions, ...unconfirmedMempool.mempoolTransactions].forEach((raw) => node.sendTransaction(raw));
    });
    const allMempoolHashes = [...confirmedHashes, ...unconfirmedHashes];
    await waitFor(async () => (await sequential(nodes, (node) => mempoolRowCount(client, node.name, allMempoolHashes))).every((count) => count === allMempoolHashes.length), { description: '40 mempool transactions recorded for both nodes', intervalMs: 50, timeoutMs: 60_000 });

    context.log('reorg: switching both nodes to 101-block branch B');
    const metricsStart = await context.backend.writeMetricsStart(client);
    const started = Date.now();
    nodes.forEach((node) => node.reorgTo(forkHeight, chainB));
    const finished = await waitFor(
      async () => {
        const counts = (await sequential(nodes, async (node) => [await acceptedBlockCount(client, node.name, chainBHashes), await acceptedBlockCount(client, node.name, chainAHashes)])).flat();
        return counts[0] === 101 && counts[1] === 0 && counts[2] === 101 && counts[3] === 0;
      },
      { description: 'both nodes converged on branch B', intervalMs: 50, timeoutMs: 600_000 }
    );
    const writeMetrics = await context.backend.writeMetricsSince(client, metricsStart);
    // give mempool cleanup triggers/agent a moment, then check final state
    await new Promise((resolve) => setTimeout(resolve, 500));

    const expectedChain = [nodes[0].genesis, ...nodes[0].chain.slice(1)].map((block) => block.hash);
    const checks = {};
    const chainDetails = {};
    for (const node of nodes) {
      const chain = await acceptedChain(client, node.name);
      const firstMismatch = expectedChain.findIndex((hash, index) => chain[index]?.hash !== hash || Number(chain[index]?.height) !== index);
      chainDetails[node.name] = { firstMismatchHeight: firstMismatch, observedLength: chain.length, observedAtMismatch: chain[firstMismatch] ?? null };
      const acceptedHashes = new Set(chain.map((row) => row.hash));
      checks[`${node.name}: accepts exactly the 101 B blocks above the fork and none of A`] =
        chainB.every((block) => acceptedHashes.has(block.hash)) && chainA.every((block) => !acceptedHashes.has(block.hash)) && chain.length <= expectedChain.length;
      checks[`${node.name}: still accepts genesis+base blocks at or below the fork point`] = expectedChain
        .slice(0, forkHeight + 1)
        .every((hash) => acceptedHashes.has(hash));
      checks[`${node.name}: 30 confirmed txs left the mempool`] = (await mempoolRowCount(client, node.name, confirmedHashes)) === 0;
      checks[`${node.name}: 30 confirmed txs archived in node_transaction_history`] = (await historyNodeCount(client, node.name, confirmedHashes)) === 30;
      checks[`${node.name}: 10 unconfirmed txs still in mempool`] = (await mempoolRowCount(client, node.name, unconfirmedHashes)) === 10;
      checks[`${node.name}: no mempool transaction is confirmed in a block the same node accepts`] = (await confirmedButInMempoolCount(client, node.name)) === 0;
    }
    const blockTransactionRows = await blockTransactionCount(client, chainBHashes);
    const expectedTransactions = sumTransactions(chainB);
    checks[`block_transaction rows for B match (${expectedTransactions})`] = blockTransactionRows === expectedTransactions;
    const failedChecks = Object.entries(checks).filter(([, passed]) => !passed).map(([name]) => name);
    const wallMs = finished - started;
    return {
      blockTransactionRows,
      chainDetails,
      checks,
      convergeSeconds: seconds(wallMs),
      correct: failedChecks.length === 0,
      expectedTransactions,
      failedChecks,
      transactionsPerSecond: blockTransactionRows / seconds(wallMs),
      ...writeMetrics,
      wallSeconds: seconds(wallMs),
    };
  } finally {
    await environment.cleanup();
  }
};

const concurrentWorkload = (context) => ({
  chipnet: loadOrGenerateBlockSequence({ blockCount: context.settings.concurrentChipnetBlocks, cacheDirectory: context.cacheDirectory, log: context.log, name: 'concurrent-chipnet', seed: context.seed, transactionsPerBlock: context.settings.concurrentChipnetTransactionsPerBlock }),
  mainnet: loadOrGenerateBlockSequence({ blockCount: context.settings.concurrentMainnetBlocks, cacheDirectory: context.cacheDirectory, log: context.log, name: 'concurrent-mainnet', seed: context.seed, transactionsPerBlock: context.settings.concurrentMainnetTransactionsPerBlock }),
});

const runConcurrentCase = async (context, label, workloads) => {
  const specs = [];
  if (workloads.mainnet) specs.push(mainnetLikeSpec(context.seed));
  if (workloads.chipnet) specs.push(chipnetLikeSpec(context.seed));
  const environment = await startEnvironment(context, label, specs);
  const { backend } = context;
  try {
    const { client, nodes } = environment;
    const plans = nodes.map((node) => {
      const payloads = node.name === 'mainnet_like' ? workloads.mainnet : workloads.chipnet;
      return { blocks: linkBlocks(payloads, node.tip().hash), node };
    });
    const allHashes = plans.flatMap((plan) => plan.blocks.map((block) => block.hash));
    const metricsStart = await backend.writeMetricsStart(client);
    const perNodeFinish = {};
    /*
     * Each network is a sequential writer, like a node following its tip:
     * announce one block, wait until it is saved, announce the next. Each
     * writer polls with its own connection so the two loops are independent.
     */
    const pollers = await Promise.all(plans.map(() => backend.openSession({ agentDirectory: context.agentDirectory, baseUrl: context.baseUrl, databaseName })));
    const started = Date.now();
    await Promise.all(
      plans.map(async (plan, index) => {
        for (const block of plan.blocks) {
          plan.node.announceViaHeaders([block]);
          await waitFor(async () => (await backend.acceptedBlockCount(pollers[index], plan.node.name, [block.hash])) === 1, {
            description: `${plan.node.name} block ${block.hash}`,
            intervalMs: 5,
            timeoutMs: 600_000,
          });
        }
        perNodeFinish[plan.node.name] = seconds(Date.now() - started);
      })
    );
    const finished = Date.now();
    await Promise.all(pollers.map((poller) => backend.closeSession(poller)));
    const wallMs = finished - started;
    const writeMetrics = await backend.writeMetricsSince(client, metricsStart);
    const blockTransactionRows = await backend.blockTransactionCount(client, allHashes);
    const expectedTransactions = plans.reduce((total, plan) => total + sumTransactions(plan.blocks), 0);
    return {
      blockTransactionRows,
      correct: blockTransactionRows === expectedTransactions,
      expectedTransactions,
      perNodeFinishSeconds: perNodeFinish,
      transactionsPerSecond: blockTransactionRows / seconds(wallMs),
      ...writeMetrics,
      wallSeconds: seconds(wallMs),
    };
  } finally {
    await environment.cleanup();
  }
};

export const concurrentScenario = async (context) => {
  const workloads = concurrentWorkload(context);
  context.log('concurrent: mainnet-like alone');
  const mainnetAlone = await runConcurrentCase(context, 'concurrent-mainnet-alone', { mainnet: workloads.mainnet });
  context.log('concurrent: chipnet-like alone');
  const chipnetAlone = await runConcurrentCase(context, 'concurrent-chipnet-alone', { chipnet: workloads.chipnet });
  context.log('concurrent: both together');
  const together = await runConcurrentCase(context, 'concurrent-both', workloads);
  const sumAlone = mainnetAlone.transactionsPerSecond + chipnetAlone.transactionsPerSecond;
  return {
    chipnetAlone,
    concurrencyRatio: together.transactionsPerSecond / sumAlone,
    correct: mainnetAlone.correct && chipnetAlone.correct && together.correct,
    mainnetAlone,
    sumAloneTransactionsPerSecond: sumAlone,
    together,
    transactionsPerSecond: together.transactionsPerSecond,
    wallSeconds: together.wallSeconds,
  };
};

export const catchUpScenario = async (context) => {
  const blockCount = context.settings.catchUpBlocks;
  const payloads = loadOrGenerateBlockSequence({ blockCount, cacheDirectory: context.cacheDirectory, log: context.log, name: 'catchup', seed: context.seed, transactionsPerBlock: context.settings.catchUpTransactionsPerBlock });
  const environment = await startEnvironment(context, 'catch-up', [mainnetLikeSpec(context.seed)]);
  const { acceptedBlockCount, blockTransactionCount, nodeBlockCount } = context.backend;
  try {
    const { client } = environment;
    const [node] = environment.nodes;
    const blocks = linkBlocks(payloads, node.tip().hash);
    const hashes = blocks.map((block) => block.hash);
    const expectedNodeBlocks = node.chain.length + blocks.length;
    context.log(`catch-up: announcing ${blockCount} blocks`);
    const metricsStart = await context.backend.writeMetricsStart(client);
    const started = Date.now();
    node.appendViaInventory(blocks);
    const finished = await waitFor(async () => (await nodeBlockCount(client, node.name)) === expectedNodeBlocks, { description: `${blockCount} catch-up blocks accepted`, intervalMs: 100, timeoutMs: 3_600_000 });
    const wallMs = finished - started;
    const writeMetrics = await context.backend.writeMetricsSince(client, metricsStart);
    const blockTransactionRows = await blockTransactionCount(client, hashes);
    const expectedTransactions = sumTransactions(blocks);
    return {
      blockTransactionRows,
      blocks: blockCount,
      blocksPerSecond: blockCount / seconds(wallMs),
      correct: blockTransactionRows === expectedTransactions && (await acceptedBlockCount(client, node.name, [hashes[hashes.length - 1]])) === 1,
      expectedTransactions,
      peakHeapBytes: environment.agent.heapPeak(started, finished).peakHeapUsed,
      transactionsPerSecond: blockTransactionRows / seconds(wallMs),
      ...writeMetrics,
      wallSeconds: seconds(wallMs),
    };
  } finally {
    await environment.cleanup();
  }
};

export const scenarios = {
  'max-block': maxBlockScenario,
  burst: burstScenario,
  reorg: reorgScenario,
  concurrent: concurrentScenario,
  'catch-up': catchUpScenario,
};
