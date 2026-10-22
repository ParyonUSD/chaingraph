/* eslint-disable @typescript-eslint/no-magic-numbers */
import type { ExecutionContext } from 'ava';
import test from 'ava';

import type { Agent as AgentClass } from './agent.js';
import { BlockBuffer } from './components/block-buffer.js';
import type { ChaingraphBlock } from './types/chaingraph.js';

/**
 * These tests drive the agent's block download scheduling (`fillBlockBuffer`,
 * `requestBlock`, `handleBlockFromNode`) against one stub node, without a
 * network connection or a database.
 */

const maxPendingDownloads = 200;
const nodeName = 'node1';
const kilobyte = 1_000;
const megabyte = 1_000_000;

const hashForHeight = (height: number) => height.toString(16).padStart(64, '0');

const loadAgentClass = async () => {
  process.env.CHAINGRAPH_TRUSTED_NODES = `${nodeName}:127.0.0.1:18333:testnet4`;
  return (await import('./agent.js')).Agent;
};

const createTestAgent = (
  agentClass: typeof AgentClass,
  { bufferTargetBytes }: { bufferTargetBytes: number }
) => {
  const sentRequests: string[] = [];
  const peer = {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    messages: { GetData: { forBlock: (hash: string) => hash } },
    sendMessage: (hash: string) => {
      sentRequests.push(hash);
    },
    status: 'ready',
  };
  const state = {
    fillScheduled: false,
    nextHeight: 1,
  };
  const noop = () => undefined;
  const agent = Object.create(agentClass.prototype) as AgentClass;
  Object.assign(agent, {
    blockBuffer: new BlockBuffer({
      freedSpaceCallback: () => {
        state.fillScheduled = true;
      },
      targetSize: bufferTargetBytes,
    }),
    blockDownloads: [],
    blockTree: { getNodesWithBlock: () => [nodeName] },
    logger: { debug: noop, error: noop, trace: noop, warn: noop },
    nodes: {
      [nodeName]: {
        peer,
        syncState: { markHeightAsPendingSync: noop },
      },
    },
    scheduleBlockBufferFill: () => {
      state.fillScheduled = true;
    },
    scheduleBlockParse: noop,
    selectNextBlockToDownload: () => {
      const height = state.nextHeight;
      state.nextHeight += 1;
      return {
        hash: hashForHeight(height),
        height,
        sourceNodes: [nodeName],
      };
    },
    syncedAtLastFillAttempt: false,
    willShutdown: false,
  });

  /**
   * Run scheduled fills until the agent stops scheduling them (the real
   * `scheduleBlockBufferFill` uses `setTimeout`).
   */
  const runScheduledFills = () => {
    agent.fillBlockBuffer();
    // eslint-disable-next-line functional/no-loop-statement
    while (state.fillScheduled) {
      state.fillScheduled = false;
      agent.fillBlockBuffer();
    }
  };

  const sentDownloads = () =>
    agent.blockDownloads.filter(
      (download) => download.requestedFromNode !== undefined
    );

  /**
   * Deliver every download which was sent to the node.
   */
  const deliverSentDownloads = () => {
    sentDownloads().forEach((download) => {
      agent.handleBlockFromNode(nodeName, {
        header: { hash: download.hash },
      } as unknown as Parameters<AgentClass['handleBlockFromNode']>[1]);
    });
  };

  const clearAllDownloads = () => {
    [...agent.blockDownloads].forEach((download) => {
      agent.clearDownload(download);
    });
  };

  return {
    agent,
    clearAllDownloads,
    deliverSentDownloads,
    peer,
    runScheduledFills,
    sentDownloads,
    sentRequests,
  };
};

const bufferedBlockOfSize = (sizeBytes: number) =>
  ({ sizeBytes } as unknown as ChaingraphBlock);

const assertNoDeferredReservations = (
  t: ExecutionContext,
  agent: AgentClass
) => {
  t.is(
    agent.blockBuffer.reservedBlocks,
    agent.blockDownloads.filter(
      (download) => download.requestedFromNode !== undefined
    ).length,
    'every buffer reservation belongs to a download which was sent'
  );
};

test.serial(
  'fillBlockBuffer reserves buffer space only for downloads it can send',
  async (t) => {
    const agentClass = await loadAgentClass();
    const { agent, clearAllDownloads, runScheduledFills, sentRequests } =
      createTestAgent(agentClass, { bufferTargetBytes: 100 * megabyte });
    /**
     * With one small block buffered, the expected block size is small, so
     * the 100 MB target has room for about 1,000 reservations: far more than
     * the node's download slots.
     */
    const smallBlock = bufferedBlockOfSize(100 * kilobyte);
    agent.blockBuffer.addBlock(smallBlock);
    runScheduledFills();

    t.is(sentRequests.length, maxPendingDownloads);
    t.is(agent.blockDownloads.length, maxPendingDownloads);
    assertNoDeferredReservations(t, agent);
    clearAllDownloads();
  }
);

test.serial(
  'downloads continue after the expected block size grows (no stall on reservations for unsent requests)',
  async (t) => {
    const agentClass = await loadAgentClass();
    const {
      agent,
      clearAllDownloads,
      deliverSentDownloads,
      runScheduledFills,
      sentDownloads,
      sentRequests,
    } = createTestAgent(agentClass, { bufferTargetBytes: 100 * megabyte });
    const smallBlock = bufferedBlockOfSize(100 * kilobyte);
    agent.blockBuffer.addBlock(smallBlock);
    runScheduledFills();

    /**
     * The small block is saved, so the buffer is empty and the expected size
     * of each reserved block becomes the 1 MB assumed maximum. If unsent
     * requests held reservations (about 1,000 × 1 MB), the buffer would stay
     * "full" with nothing buffered and nothing downloading.
     */
    agent.blockBuffer.removeBlock(smallBlock);
    deliverSentDownloads();
    runScheduledFills();

    t.is(agent.blockBuffer.count(), 0);
    t.true(
      sentDownloads().length > 0,
      'the agent sends new block requests once the pending downloads finish'
    );
    t.true(sentRequests.length > maxPendingDownloads);
    assertNoDeferredReservations(t, agent);
    clearAllDownloads();
  }
);

test.serial(
  'deferred downloads are sent as soon as the node can take them',
  async (t) => {
    const agentClass = await loadAgentClass();
    const { agent, clearAllDownloads, peer, runScheduledFills, sentRequests } =
      createTestAgent(agentClass, { bufferTargetBytes: 10 * megabyte });
    runScheduledFills();
    const sentBeforeDisconnect = sentRequests.length;
    t.true(sentBeforeDisconnect > 0);

    peer.status = 'disconnected';
    agent.rescheduleDownloadsForNode(nodeName);
    const deferred = agent.blockDownloads.filter(
      (download) => download.requestedFromNode === undefined
    );
    t.is(deferred.length, sentBeforeDisconnect);

    peer.status = 'ready';
    runScheduledFills();
    t.is(
      agent.blockDownloads.filter(
        (download) => download.requestedFromNode === undefined
      ).length,
      0,
      'no download waits for the download timeout'
    );
    t.is(sentRequests.length, 2 * sentBeforeDisconnect);
    assertNoDeferredReservations(t, agent);
    clearAllDownloads();
  }
);
