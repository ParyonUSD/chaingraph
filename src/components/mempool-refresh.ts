import type { Peer } from '@chaingraph/bitcore-p2p-cash';

/**
 * Ask a trusted node to announce its current mempool, including transactions
 * it has already announced. Forget only saved announcement markers for this
 * node: a later positive announcement must be able to restore its acceptance
 * after confirmation or a reorganization. Keep other nodes and pending bodies.
 *
 * BIP35 inventory can span multiple messages and has no completion marker.
 * Neither an empty response nor missing hashes prove absence; the existing
 * inventory handler adds acceptances and never removes them from this response.
 * BCHN must permit this request via NODE_BLOOM or the peer's mempool permission.
 */
export const requestMempoolRefresh = <Node extends { peer: Peer }>(
  node: Node,
  transactionCache: Iterable<[string, { db: boolean; nodes: Node[] }]>
) => {
  // eslint-disable-next-line functional/no-loop-statement
  for (const [, transaction] of transactionCache) {
    if (transaction.db) {
      transaction.nodes = transaction.nodes.filter((source) => source !== node);
    }
  }
  node.peer.sendMessage(new node.peer.messages.MemPool());
};
