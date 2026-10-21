/* eslint-disable @typescript-eslint/no-magic-numbers */
import bitcoreP2pCash from '@chaingraph/bitcore-p2p-cash';
import type { Message } from '@chaingraph/bitcore-p2p-cash';
import test from 'ava';
import LRU from 'lru-cache';

import type {
  MempoolResyncCacheItem,
  MempoolResyncOptions,
  MempoolResyncTrigger,
} from './mempool-resync.js';
import {
  forgetSavedAcknowledgementsFromNode,
  MempoolResync,
} from './mempool-resync.js';

interface MockNode {
  name: string;
  peer: InstanceType<typeof bitcoreP2pCash.Peer>;
  ready: boolean;
  sentMessages: Message[];
}

const createMockNode = (name: string): MockNode => {
  const sentMessages: Message[] = [];
  const peer = new bitcoreP2pCash.Peer({});
  peer.sendMessage = (message) => {
    sentMessages.push(message);
  };
  return { name, peer, ready: true, sentMessages };
};

const commandStart = 4;
const commandEnd = 16;
const commandsSent = (node: MockNode) =>
  node.sentMessages.map((message) =>
    message
      .toBuffer()
      .subarray(commandStart, commandEnd)
      .toString('latin1')
      .replace(/\0+$/u, '')
  );

const setup = (options: Partial<MempoolResyncOptions<MockNode>> = {}) => {
  const nodes = {
    node1: createMockNode('node1'),
    node2: createMockNode('node2'),
  };
  const transactionCache = new LRU<string, MempoolResyncCacheItem<MockNode>>({
    max: 10,
  });
  const clock = { now: 0 };
  const timers: { callback: () => void; dueAt: number; cleared: boolean }[] =
    [];
  const advance = (ms: number) => {
    clock.now += ms;
    timers
      .filter((timer) => !timer.cleared && timer.dueAt <= clock.now)
      .forEach((timer) => {
        timer.cleared = true;
        timer.callback();
      });
  };
  const requestsSent: [string, MempoolResyncTrigger[]][] = [];
  const mempoolResync = new MempoolResync<MockNode>({
    clearTimer: (timer) => {
      (timer as (typeof timers)[number]).cleared = true;
    },
    enabled: true,
    getReadyNode: (nodeName) => {
      const node = nodes[nodeName as keyof typeof nodes];
      return node.ready ? node : undefined;
    },
    minimumIntervalMs: 1000,
    now: () => clock.now,
    onRequestSent: (nodeName, triggers) => {
      requestsSent.push([nodeName, triggers]);
    },
    setTimer: (callback, ms) => {
      const timer = { callback, cleared: false, dueAt: clock.now + ms };
      timers.push(timer);
      return timer;
    },
    transactionCache,
    ...options,
  });
  return {
    advance,
    mempoolResync,
    nodes,
    requestsSent,
    timers,
    transactionCache,
  };
};

test('sends the BIP35 mempool wire message', (t) => {
  const { mempoolResync, nodes } = setup();
  mempoolResync.enableMempoolTracking(['node1']);
  t.deepEqual(commandsSent(nodes.node1), ['mempool']);
  const wire = nodes.node1.sentMessages[0]!.toBuffer();
  t.is(wire.subarray(4, 16).toString('hex'), '6d656d706f6f6c0000000000');
  t.is(wire.readUInt32LE(16), 0);
  t.is(wire.length, 24);
});

test('requests every node once mempool tracking is enabled', (t) => {
  const { mempoolResync, nodes, requestsSent } = setup();
  mempoolResync.enableMempoolTracking(['node1', 'node2']);
  t.deepEqual(commandsSent(nodes.node1), ['mempool']);
  t.deepEqual(commandsSent(nodes.node2), ['mempool']);
  t.deepEqual(requestsSent, [
    ['node1', ['mempool-tracking-enabled']],
    ['node2', ['mempool-tracking-enabled']],
  ]);
});

test('requests a node on (re)connect, but only after mempool tracking is enabled', (t) => {
  const { advance, mempoolResync, nodes, requestsSent } = setup();
  mempoolResync.handleConnect('node1');
  t.deepEqual(commandsSent(nodes.node1), []);
  mempoolResync.enableMempoolTracking([]);
  mempoolResync.handleConnect('node1');
  t.deepEqual(commandsSent(nodes.node1), ['mempool']);
  advance(1000);
  mempoolResync.handleConnect('node1');
  t.deepEqual(commandsSent(nodes.node1), ['mempool', 'mempool']);
  t.deepEqual(requestsSent, [
    ['node1', ['connect']],
    ['node1', ['connect']],
  ]);
});

test('requests a node after a re-organization, but only after mempool tracking is enabled', (t) => {
  const { mempoolResync, nodes, requestsSent } = setup();
  mempoolResync.handleReorganization('node2');
  t.deepEqual(commandsSent(nodes.node2), []);
  mempoolResync.enableMempoolTracking([]);
  mempoolResync.handleReorganization('node2');
  t.deepEqual(commandsSent(nodes.node1), []);
  t.deepEqual(commandsSent(nodes.node2), ['mempool']);
  t.deepEqual(requestsSent, [['node2', ['reorganization']]]);
});

test('sends nothing when disabled', (t) => {
  const { mempoolResync, nodes, timers } = setup({ enabled: false });
  mempoolResync.enableMempoolTracking(['node1', 'node2']);
  mempoolResync.handleConnect('node1');
  mempoolResync.handleReorganization('node2');
  t.deepEqual(commandsSent(nodes.node1), []);
  t.deepEqual(commandsSent(nodes.node2), []);
  t.is(timers.length, 0);
});

test('forgets saved acknowledgements from the node before each request', (t) => {
  const { mempoolResync, nodes, transactionCache } = setup();
  const { node1, node2 } = nodes;
  transactionCache.set('saved-both', { db: true, nodes: [node1, node2] });
  transactionCache.set('saved-node1', { db: true, nodes: [node1] });
  transactionCache.set('saved-node2', { db: true, nodes: [node2] });
  transactionCache.set('unsaved', { db: false, nodes: [node1, node2] });
  node1.peer.sendMessage = () => {
    t.deepEqual(transactionCache.get('saved-node1')!.nodes, []);
  };
  mempoolResync.handleConnect('node1');
  mempoolResync.enableMempoolTracking(['node1']);
  t.deepEqual(transactionCache.get('saved-both'), {
    db: true,
    nodes: [node2],
  });
  t.deepEqual(transactionCache.get('saved-node1'), { db: true, nodes: [] });
  t.deepEqual(transactionCache.get('saved-node2'), {
    db: true,
    nodes: [node2],
  });
  t.deepEqual(transactionCache.get('unsaved'), {
    db: false,
    nodes: [node1, node2],
  });
  t.is(transactionCache.size, 4);
  t.plan(6);
});

test('forgetSavedAcknowledgementsFromNode never removes cache entries', (t) => {
  const { nodes, transactionCache } = setup();
  transactionCache.set('a', { db: true, nodes: [nodes.node1] });
  forgetSavedAcknowledgementsFromNode(nodes.node1, transactionCache);
  forgetSavedAcknowledgementsFromNode(nodes.node1, transactionCache);
  t.deepEqual(transactionCache.get('a'), { db: true, nodes: [] });
  t.is(transactionCache.size, 1);
});

test('rate-limits each node, coalescing triggers into one deferred request', (t) => {
  const { advance, mempoolResync, nodes, requestsSent, timers } = setup();
  mempoolResync.enableMempoolTracking(['node1', 'node2']);
  advance(100);
  mempoolResync.handleReorganization('node1');
  mempoolResync.handleConnect('node1');
  mempoolResync.handleReorganization('node1');
  t.deepEqual(commandsSent(nodes.node1), ['mempool']);
  t.is(timers.length, 1);
  advance(899);
  t.deepEqual(commandsSent(nodes.node1), ['mempool']);
  advance(1);
  t.deepEqual(commandsSent(nodes.node1), ['mempool', 'mempool']);
  t.deepEqual(commandsSent(nodes.node2), ['mempool']);
  t.deepEqual(requestsSent.slice(2), [
    ['node1', ['reorganization', 'connect']],
  ]);
  advance(500);
  mempoolResync.handleReorganization('node1');
  t.deepEqual(commandsSent(nodes.node1), ['mempool', 'mempool']);
  advance(500);
  t.deepEqual(commandsSent(nodes.node1), ['mempool', 'mempool', 'mempool']);
});

test('a flapping node is requested at most once per interval', (t) => {
  const { advance, mempoolResync, nodes } = setup();
  mempoolResync.enableMempoolTracking([]);
  Array.from({ length: 100 }).forEach(() => {
    mempoolResync.handleConnect('node1');
    advance(50);
  });
  /**
   * 100 reconnects over 5 seconds: sent at 0, 1000, 2000, 3000, 4000, and
   * (coalescing the reconnects after 4000) 5000.
   */
  t.is(commandsSent(nodes.node1).length, 6);
});

test('skips a deferred request if the node disconnected', (t) => {
  const { advance, mempoolResync, nodes, requestsSent } = setup();
  mempoolResync.enableMempoolTracking(['node1']);
  mempoolResync.handleReorganization('node1');
  nodes.node1.ready = false;
  advance(1000);
  t.deepEqual(commandsSent(nodes.node1), ['mempool']);
  nodes.node1.ready = true;
  mempoolResync.handleConnect('node1');
  t.deepEqual(commandsSent(nodes.node1), ['mempool', 'mempool']);
  t.deepEqual(requestsSent, [
    ['node1', ['mempool-tracking-enabled']],
    ['node1', ['connect']],
  ]);
});

test('stop cancels deferred requests and ignores later triggers', (t) => {
  const { advance, mempoolResync, nodes, timers } = setup();
  mempoolResync.enableMempoolTracking(['node1']);
  mempoolResync.handleReorganization('node1');
  mempoolResync.stop();
  t.true(timers[0]!.cleared);
  advance(5000);
  mempoolResync.handleReorganization('node1');
  mempoolResync.handleConnect('node2');
  t.deepEqual(commandsSent(nodes.node1), ['mempool']);
  t.deepEqual(commandsSent(nodes.node2), []);
});

test('uses real timers by default', async (t) => {
  const node = createMockNode('node1');
  const mempoolResync = new MempoolResync<MockNode>({
    enabled: true,
    getReadyNode: () => node,
    minimumIntervalMs: 20,
    transactionCache: [],
  });
  mempoolResync.enableMempoolTracking(['node1']);
  mempoolResync.handleReorganization('node1');
  t.deepEqual(commandsSent(node), ['mempool']);
  await new Promise((resolve) => {
    setTimeout(resolve, 50);
  });
  t.deepEqual(commandsSent(node), ['mempool', 'mempool']);
  mempoolResync.stop();
});
