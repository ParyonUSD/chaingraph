/**
 * Ingestion gate scenarios. Each scenario gets a fresh database (all
 * migrations of the agent under test), fresh mock nodes and a fresh agent,
 * waits until the agent reaches steady state (initial sync done, managed
 * indexes built, mempool tracking enabled – i.e. the production write path,
 * with triggers enabled), then measures.
 *
 * Throughput is always `block_transaction rows for the measured blocks / wall
 * clock`, never the agent's own "active seconds" statistics.
 */
import { readFileSync } from 'node:fs';

import { AgentProcess } from './agent.mjs';
import { assembleBlock, doubleSha256, generateTransactionPayload, loadOrGenerateBlockSequence } from './fixtures.mjs';
import { genesisBlockRaw, genesisFromRaw, makeBlock, MockNode, testnetGenesisBlockRaw } from './mock-node.mjs';
import {
  acceptedBlockCount,
  blockTransactionCount,
  connectClient,
  countRows,
  currentWalLsn,
  recreateDatabase,
  waitFor,
  walBytesSince,
} from './postgres.mjs';

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
  const client = await recreateDatabase({ agentDirectory: context.agentDirectory, baseUrl: context.baseUrl, databaseName });
  const nodes = nodeSpecs.map((spec) => {
    const node = new MockNode({ genesis: spec.genesis, magicHex: spec.magicHex, name: spec.name, port: nextPort++ });
    node.extend(spec.baseChain);
    node.listen();
    return node;
  });
  const genesisBlocks = [...new Map(nodeSpecs.map((spec) => [spec.magicHex, `${spec.magicHex}:${spec.genesisRaw}`])).values()].join(',');
  const agent = new AgentProcess({
    agentDirectory: context.agentDirectory,
    connectionString: `${context.baseUrl}/${databaseName}`,
    genesisBlocks,
    label,
    runDirectory: context.runDirectory,
    settings: context.settings,
    trustedNodes: nodes.map((node) => node.trustedNodeEntry()).join(','),
  });
  const cleanup = async () => {
    await agent.stop();
    nodes.forEach((node) => node.close());
    await client.end();
  };
  try {
    await Promise.all(nodes.map((node) => node.waitForPeer()));
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
const measureIngestion = async ({ environment, node, blocks, announce, timeoutMs, pollMs = 25 }) => {
  const { client, agent } = environment;
  const hashes = blocks.map((block) => block.hash);
  const startLsn = await currentWalLsn(client);
  const transactionRowsBefore = await countRows(client, 'transaction');
  const started = Date.now();
  announce();
  const finished = await waitFor(async () => (await acceptedBlockCount(client, node.name, hashes)) === hashes.length, {
    description: `${node.name} accepting ${hashes.length} block(s)`,
    intervalMs: pollMs,
    timeoutMs,
  });
  const wallMs = finished - started;
  const walBytes = await walBytesSince(client, startLsn);
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
    walBytes,
    wallSeconds: seconds(wallMs),
  };
};


/**
 * `CHAINGRAPH_UNSPENT_TRACKING` (experiment): compare the stored read model
 * with the F1g predicate on every output created after `watermark` (a
 * transaction internal_id taken once the agent reached steady state, so the
 * unaudited base chain is excluded). marker/settable track spends across all
 * nodes; bitmask is compared per node, creator acceptance included. Returns
 * mismatch counts in both directions (all 0 when correct; `null` in `off`).
 */
const unspentTrackingMode = process.env.CHAINGRAPH_UNSPENT_TRACKING ?? 'off';
const unspentReadModelCheck = async (client, watermark) => {
  if (unspentTrackingMode === 'off') return null;
  await client.query('ANALYZE output; ANALYZE input; ANALYZE transaction; ANALYZE block_transaction;');
  const acceptedAnyNode = `(SELECT bt.transaction_internal_id AS id FROM block_transaction bt JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id
                            UNION SELECT transaction_internal_id FROM node_transaction)`;
  if (unspentTrackingMode === 'bitmask') {
    const nodes = (await client.query('SELECT internal_id::int AS id FROM node ORDER BY internal_id')).rows.map((row) => row.id);
    const perNode = {};
    for (const node of nodes) {
      const acceptedByNode = `(SELECT bt.transaction_internal_id AS id FROM block_transaction bt JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id AND nb.node_internal_id = ${node}
                               UNION SELECT transaction_internal_id FROM node_transaction WHERE node_internal_id = ${node})`;
      const row = (
        await client.query(`
          WITH acc AS MATERIALIZED ${acceptedByNode},
          spent AS (SELECT DISTINCT i.outpoint_transaction_hash AS h, i.outpoint_index AS i FROM input i JOIN acc ON acc.id = i.transaction_internal_id),
          d AS (SELECT o.transaction_hash AS h, o.output_index AS i, (o.unspent_node_bits & (1::bigint << ${node})) <> 0 AS stored,
                       (t.internal_id IN (SELECT id FROM acc)) AS created
                  FROM output o JOIN transaction t ON t.hash = o.transaction_hash
                  WHERE t.internal_id > $1 AND o.unspent_node_bits IS NOT NULL)
          SELECT count(*)::int AS outputs,
                 count(*) FILTER (WHERE d.stored AND NOT (d.created AND s.h IS NULL))::int AS stored_unspent_wrong,
                 count(*) FILTER (WHERE NOT d.stored AND d.created AND s.h IS NULL)::int AS stored_spent_wrong
            FROM d LEFT JOIN spent s ON s.h = d.h AND s.i = d.i`,
          [watermark]
        )
      ).rows[0];
      perNode[node] = row;
    }
    const values = Object.values(perNode);
    return {
      mode: unspentTrackingMode,
      outputs: Math.max(0, ...values.map((value) => value.outputs)),
      perNode,
      storedSpentButUnspent: values.reduce((total, value) => total + value.stored_spent_wrong, 0),
      storedUnspentButSpent: values.reduce((total, value) => total + value.stored_unspent_wrong, 0),
    };
  }
  const stored =
    unspentTrackingMode === 'marker'
      ? { domain: 'o.spent_by_transaction_internal_id IS NOT NULL', unspent: 'o.spent_by_transaction_internal_id = 0' }
      : { domain: 'true', unspent: 'EXISTS (SELECT 1 FROM unspent_output_set u WHERE u.transaction_hash = o.transaction_hash AND u.output_index = o.output_index)' };
  const row = (
    await client.query(
      `WITH acc AS MATERIALIZED ${acceptedAnyNode},
       spent AS (SELECT DISTINCT i.outpoint_transaction_hash AS h, i.outpoint_index AS i FROM input i JOIN acc ON acc.id = i.transaction_internal_id),
       d AS (SELECT o.transaction_hash AS h, o.output_index AS i, ${stored.unspent} AS stored_unspent
               FROM output o JOIN transaction t ON t.hash = o.transaction_hash
               WHERE t.internal_id > $1 AND ${stored.domain})
       SELECT count(*)::int AS outputs,
              count(*) FILTER (WHERE d.stored_unspent AND s.h IS NOT NULL)::int AS stored_unspent_but_spent,
              count(*) FILTER (WHERE NOT d.stored_unspent AND s.h IS NULL)::int AS stored_spent_but_unspent
         FROM d LEFT JOIN spent s ON s.h = d.h AND s.i = d.i`,
      [watermark]
    )
  ).rows[0];
  return {
    mode: unspentTrackingMode,
    outputs: row.outputs,
    storedSpentButUnspent: row.stored_spent_but_unspent,
    storedUnspentButSpent: row.stored_unspent_but_spent,
  };
};
const transactionWatermark = async (client) =>
  Number((await client.query('SELECT COALESCE(max(internal_id), 0)::bigint AS id FROM transaction')).rows[0].id);
const readModelCorrect = (check) => check === null || (check.storedSpentButUnspent === 0 && check.storedUnspentButSpent === 0);

/**
 * Per-block tracking timings from the agent's log ("Inserting block … |
 * unspent tracking (…): mark X ms, resolve Y ms, post-commit Z ms (fixed A
 * new-output, B spent-output rows …)"), summed over the blocks logged after
 * `sinceMs`.
 */
/** The agent's log is buffered: stop the agent first, then read it. */
const trackingTimingsAfterStop = async (agent, sinceMs, minHeight = 0) => {
  await agent.stop();
  return trackingTimingsFromLog(agent, sinceMs, minHeight);
};
const trackingTimingsFromLog = (agent, sinceMs, minHeight = 0) => {
  const pattern = /mark (\d+) ms, resolve (\d+) ms(?:, post-commit (\d+) ms \(fixed (\d+) new-output, (\d+) spent-output rows(?:; (\d+) failed)?)?/u;
  const totals = { blocks: 0, failedAttempts: 0, markMs: 0, maxPostCommitMs: 0, postCommitMs: 0, postCommitNewOutputsFixed: 0, postCommitSpentOutputsFixed: 0, resolveMs: 0 };
  let text = '';
  try {
    text = readFileSync(agent.logPath, 'utf8');
  } catch {
    return totals;
  }
  text.split('\n').forEach((line) => {
    if (!line.includes('Inserting block')) return;
    let entry;
    try {
      entry = JSON.parse(line);
    } catch {
      return;
    }
    if (entry.time < sinceMs) return;
    if (Number(/Inserting block (\d+)/u.exec(entry.msg ?? '')?.[1] ?? 0) < minHeight) return;
    const match = pattern.exec(entry.msg ?? '');
    if (!match) return;
    totals.blocks += 1;
    totals.markMs += Number(match[1]);
    totals.resolveMs += Number(match[2]);
    if (match[3] !== undefined) {
      totals.postCommitMs += Number(match[3]);
      totals.maxPostCommitMs = Math.max(totals.maxPostCommitMs, Number(match[3]));
      totals.postCommitNewOutputsFixed += Number(match[4]);
      totals.postCommitSpentOutputsFixed += Number(match[5]);
      totals.failedAttempts += Number(match[6] ?? 0);
    }
  });
  return totals;
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
    const watermark = await transactionWatermark(environment.client);
    const blocks = linkBlocks([payload], node.tip().hash);
    context.log(`max-block: announcing ${(blocks[0].raw.length / 1e6).toFixed(2)} MB block with ${blocks[0].stats.transactions} txs`);
    const sinceMs = Date.now();
    const result = await measureIngestion({ announce: () => node.announceViaHeaders(blocks), blocks, environment, node, timeoutMs: 600_000 });
    // stop first: the post-commit pass runs after the node_block rows are visible
    await environment.agent.stop();
    const unspentReadModel = await unspentReadModelCheck(environment.client, watermark);
    return { ...result, correct: result.correct && readModelCorrect(unspentReadModel), peakHeapBytes: result.heap.peakHeapUsed, tracking: await trackingTimingsAfterStop(environment.agent, sinceMs), unspentReadModel };
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
    const watermark = await transactionWatermark(environment.client);
    const blocks = linkBlocks(payloads, node.tip().hash);
    context.log(`burst: announcing 3 blocks (${(sumBytes(blocks) / 1e6).toFixed(2)} MB) back-to-back`);
    const sinceMs = Date.now();
    const result = await measureIngestion({ announce: () => node.announceViaHeaders(blocks), blocks, environment, node, timeoutMs: 1_200_000 });
    // block n+1 spends block n and all three are saved concurrently: the parent/child race
    // stop first: the post-commit pass runs after the node_block rows are visible
    await environment.agent.stop();
    const unspentReadModel = await unspentReadModelCheck(environment.client, watermark);
    return { ...result, correct: result.correct && readModelCorrect(unspentReadModel), drainSeconds: result.wallSeconds, peakHeapBytes: result.heap.peakHeapUsed, tracking: await trackingTimingsAfterStop(environment.agent, sinceMs), unspentReadModel };
  } finally {
    await environment.cleanup();
  }
};

/**
 * The max-block shape with real spends: a 100,000-tx parent block is saved
 * first (not measured), then the measured 100,000-tx child spends output 0 of
 * every parent transaction (the burst fixture's blocks 1 and 2), so the
 * tracking statements update 100,000 committed outputs.
 */
export const maxBlockSpendScenario = async (context) => {
  const payloads = loadOrGenerateBlockSequence({
    blockCount: 3,
    cacheDirectory: context.cacheDirectory,
    log: context.log,
    name: 'burst',
    seed: context.seed,
    transactionsPerBlock: context.settings.denseTransactionsPerBlock,
  });
  const environment = await startEnvironment(context, 'max-block-spend', [mainnetLikeSpec(context.seed)]);
  try {
    const [node] = environment.nodes;
    const watermark = await transactionWatermark(environment.client);
    const parentHeight = node.chain.length;
    const [parent, child] = linkBlocks(payloads.slice(0, 2), node.tip().hash);
    context.log('max-block-spend: saving the parent block (not measured)');
    node.announceViaHeaders([parent]);
    await waitFor(async () => (await acceptedBlockCount(environment.client, node.name, [parent.hash])) === 1, { description: 'parent block accepted', intervalMs: 50, timeoutMs: 600_000 });
    await environment.client.query('CHECKPOINT');
    context.log(`max-block-spend: announcing ${(child.raw.length / 1e6).toFixed(2)} MB child block with ${child.stats.transactions} txs`);
    const sinceMs = Date.now();
    const result = await measureIngestion({ announce: () => node.announceViaHeaders([child]), blocks: [child], environment, node, timeoutMs: 600_000 });
    // stop first: the post-commit pass runs after the node_block rows are visible
    await environment.agent.stop();
    const unspentReadModel = await unspentReadModelCheck(environment.client, watermark);
    return { ...result, correct: result.correct && readModelCorrect(unspentReadModel), peakHeapBytes: result.heap.peakHeapUsed, tracking: await trackingTimingsAfterStop(environment.agent, sinceMs, parentHeight + 1), unspentReadModel };
  } finally {
    await environment.cleanup();
  }
};

const acceptedChain = async (client, nodeName) =>
  (
    await client.query(
      `SELECT block.height::int AS height, encode(block.hash, 'hex') AS hash
         FROM node_block
         JOIN node ON node.internal_id = node_block.node_internal_id
         JOIN block ON block.internal_id = node_block.block_internal_id
        WHERE node.name = $1
        ORDER BY block.height`,
      [nodeName]
    )
  ).rows;

const mempoolRowCount = async (client, nodeName, transactionHashes) =>
  Number(
    (
      await client.query(
        `SELECT count(*)::bigint AS count
           FROM node_transaction
           JOIN node ON node.internal_id = node_transaction.node_internal_id
           JOIN transaction ON transaction.internal_id = node_transaction.transaction_internal_id
          WHERE node.name = $1 AND transaction.hash = ANY($2::bytea[])`,
        [nodeName, transactionHashes.map((hash) => Buffer.from(hash, 'hex'))]
      )
    ).rows[0].count
  );

const historyNodeCount = async (client, nodeName, transactionHashes) =>
  Number(
    (
      await client.query(
        `SELECT count(DISTINCT node_transaction_history.transaction_internal_id)::bigint AS count
           FROM node_transaction_history
           JOIN node ON node.internal_id = node_transaction_history.node_internal_id
           JOIN transaction ON transaction.internal_id = node_transaction_history.transaction_internal_id
          WHERE node.name = $1 AND transaction.hash = ANY($2::bytea[])`,
        [nodeName, transactionHashes.map((hash) => Buffer.from(hash, 'hex'))]
      )
    ).rows[0].count
  );

/** node_transaction rows whose transaction is in a block the same node accepted. */
const confirmedButInMempoolCount = async (client) =>
  Number(
    (
      await client.query(
        `SELECT count(*)::bigint AS count
           FROM node_transaction
           JOIN block_transaction ON block_transaction.transaction_internal_id = node_transaction.transaction_internal_id
           JOIN node_block ON node_block.block_internal_id = block_transaction.block_internal_id
                          AND node_block.node_internal_id = node_transaction.node_internal_id`
      )
    ).rows[0].count
  );

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
  try {
    const { client, nodes } = environment;
    const watermark = await transactionWatermark(client);
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
    const startLsn = await currentWalLsn(client);
    const started = Date.now();
    nodes.forEach((node) => node.reorgTo(forkHeight, chainB));
    const finished = await waitFor(
      async () => {
        const counts = (await sequential(nodes, async (node) => [await acceptedBlockCount(client, node.name, chainBHashes), await acceptedBlockCount(client, node.name, chainAHashes)])).flat();
        return counts[0] === 101 && counts[1] === 0 && counts[2] === 101 && counts[3] === 0;
      },
      { description: 'both nodes converged on branch B', intervalMs: 50, timeoutMs: 600_000 }
    );
    const walBytes = await walBytesSince(client, startLsn);
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
    }
    const blockTransactionRows = await blockTransactionCount(client, chainBHashes);
    const expectedTransactions = sumTransactions(chainB);
    checks[`block_transaction rows for B match (${expectedTransactions})`] = blockTransactionRows === expectedTransactions;
    checks['no node_transaction row is confirmed in a block accepted by the same node'] = (await confirmedButInMempoolCount(client)) === 0;
    await environment.agent.stop();
    const unspentReadModel = await unspentReadModelCheck(client, watermark);
    if (unspentReadModel !== null) checks['unspent read model equals the F1g reference'] = readModelCorrect(unspentReadModel);
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
      unspentReadModel,
      walBytes,
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
  try {
    const { client, nodes } = environment;
    const plans = nodes.map((node) => {
      const payloads = node.name === 'mainnet_like' ? workloads.mainnet : workloads.chipnet;
      return { blocks: linkBlocks(payloads, node.tip().hash), node };
    });
    const allHashes = plans.flatMap((plan) => plan.blocks.map((block) => block.hash));
    const startLsn = await currentWalLsn(client);
    const perNodeFinish = {};
    /*
     * Each network is a sequential writer, like a node following its tip:
     * announce one block, wait until it is saved, announce the next. Each
     * writer polls with its own connection so the two loops are independent.
     */
    const pollers = await Promise.all(plans.map(() => connectClient(`${context.baseUrl}/${databaseName}`)));
    const started = Date.now();
    await Promise.all(
      plans.map(async (plan, index) => {
        for (const block of plan.blocks) {
          plan.node.announceViaHeaders([block]);
          await waitFor(async () => (await acceptedBlockCount(pollers[index], plan.node.name, [block.hash])) === 1, {
            description: `${plan.node.name} block ${block.hash}`,
            intervalMs: 5,
            timeoutMs: 600_000,
          });
        }
        perNodeFinish[plan.node.name] = seconds(Date.now() - started);
      })
    );
    const finished = Date.now();
    await Promise.all(pollers.map((poller) => poller.end()));
    const wallMs = finished - started;
    const blockTransactionRows = await blockTransactionCount(client, allHashes);
    const expectedTransactions = plans.reduce((total, plan) => total + sumTransactions(plan.blocks), 0);
    return {
      blockTransactionRows,
      correct: blockTransactionRows === expectedTransactions,
      expectedTransactions,
      perNodeFinishSeconds: perNodeFinish,
      transactionsPerSecond: blockTransactionRows / seconds(wallMs),
      walBytes: await walBytesSince(client, startLsn),
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
  try {
    const { client } = environment;
    const [node] = environment.nodes;
    const blocks = linkBlocks(payloads, node.tip().hash);
    const hashes = blocks.map((block) => block.hash);
    const expectedNodeBlocks = node.chain.length + blocks.length;
    context.log(`catch-up: announcing ${blockCount} blocks`);
    const startLsn = await currentWalLsn(client);
    const started = Date.now();
    node.appendViaInventory(blocks);
    const nodeBlockCount = async () =>
      Number((await client.query(`SELECT count(*)::bigint AS count FROM node_block JOIN node ON node.internal_id = node_block.node_internal_id WHERE node.name = $1`, [node.name])).rows[0].count);
    const finished = await waitFor(async () => (await nodeBlockCount()) === expectedNodeBlocks, { description: `${blockCount} catch-up blocks accepted`, intervalMs: 100, timeoutMs: 3_600_000 });
    const wallMs = finished - started;
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
      walBytes: await walBytesSince(client, startLsn),
      wallSeconds: seconds(wallMs),
    };
  } finally {
    await environment.cleanup();
  }
};

export const scenarios = {
  'max-block': maxBlockScenario,
  'max-block-spend': maxBlockSpendScenario,
  burst: burstScenario,
  reorg: reorgScenario,
  concurrent: concurrentScenario,
  'catch-up': catchUpScenario,
};
