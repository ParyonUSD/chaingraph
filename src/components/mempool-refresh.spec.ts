/* eslint-disable @typescript-eslint/no-magic-numbers */
import bitcoreP2pCash from '@chaingraph/bitcore-p2p-cash';
import type { Message } from '@chaingraph/bitcore-p2p-cash';
import test from 'ava';
import LRU from 'lru-cache';

import { requestMempoolRefresh } from './mempool-refresh.js';

const setup = () => {
  const sent: Message[] = [];
  const node = { peer: new bitcoreP2pCash.Peer({}) };
  const otherNode = { peer: new bitcoreP2pCash.Peer({}) };
  node.peer.sendMessage = (message) => {
    sent.push(message);
  };
  const cache = new LRU<string, { db: boolean; nodes: (typeof node)[] }>({
    max: 10,
  });
  return { cache, node, otherNode, sent };
};

test('refresh sends the installed library BIP35 mempool wire message', (t) => {
  const { cache, node, sent } = setup();
  requestMempoolRefresh(node, cache);
  t.is(sent.length, 1);
  const wire = sent[0]!.toBuffer();
  t.is(wire.subarray(0, 4).toString('hex'), 'e3e1f3e8');
  t.is(wire.subarray(4, 16).toString('hex'), '6d656d706f6f6c0000000000');
  t.is(wire.readUInt32LE(16), 0);
  t.is(wire.length, 24);
});

test('refresh forgets saved source acknowledgments while preserving other node acceptance', (t) => {
  const { cache, node, otherNode } = setup();
  cache.set('both', { db: true, nodes: [node, otherNode] });
  cache.set('source', { db: true, nodes: [node] });
  cache.set('other', { db: true, nodes: [otherNode] });
  cache.set('body-pending', { db: false, nodes: [node, otherNode] });
  requestMempoolRefresh(node, cache);
  t.deepEqual(cache.get('both'), { db: true, nodes: [otherNode] });
  t.deepEqual(cache.get('source'), { db: true, nodes: [] });
  t.deepEqual(cache.get('other'), { db: true, nodes: [otherNode] });
  t.deepEqual(cache.get('body-pending'), {
    db: false,
    nodes: [node, otherNode],
  });
});

test('silent, empty, or incomplete snapshots cannot delete cached bodies or other acceptances', (t) => {
  const { cache, node, otherNode, sent } = setup();
  cache.set('retained', { db: true, nodes: [node, otherNode] });
  requestMempoolRefresh(node, cache);
  requestMempoolRefresh(node, cache);
  t.is(cache.size, 1);
  t.deepEqual(cache.get('retained'), { db: true, nodes: [otherNode] });
  t.is(sent.length, 2);
});
