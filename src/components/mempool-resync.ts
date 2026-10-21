import type { Peer } from '@chaingraph/bitcore-p2p-cash';

/**
 * The events after which a node's mempool is requested.
 */
export type MempoolResyncTrigger =
  | 'connect'
  | 'mempool-tracking-enabled'
  | 'reorganization';

export interface MempoolResyncNode {
  peer: Pick<Peer, 'messages' | 'sendMessage'>;
}

export interface MempoolResyncCacheItem<Node> {
  db: boolean;
  nodes: Node[];
}

export interface MempoolResyncOptions<Node extends MempoolResyncNode> {
  /**
   * If `false`, no `mempool` requests are ever sent.
   */
  enabled: boolean;
  /**
   * The minimum number of milliseconds between two `mempool` requests to the
   * same node. Requests made during this window are coalesced into a single
   * request sent when the window ends.
   */
  minimumIntervalMs: number;
  /**
   * Return the node if it is connected and registered (ready to receive a
   * `mempool` request and to have its announcements recorded), otherwise
   * `undefined`.
   */
  getReadyNode: (nodeName: string) => Node | undefined;
  /**
   * The agent's transaction cache. Before each request, the node is removed
   * from the acknowledgements of already-saved transactions.
   */
  transactionCache: Iterable<[string, MempoolResyncCacheItem<Node>]>;
  /**
   * Called after each `mempool` request is sent.
   */
  onRequestSent?: (nodeName: string, triggers: MempoolResyncTrigger[]) => void;
  now?: () => number;
  setTimer?: (callback: () => void, ms: number) => unknown;
  clearTimer?: (timer: unknown) => void;
}

/**
 * Remove `node` from the cached acknowledgements of every transaction which
 * has already been saved to the database, so a later announcement of the
 * transaction by this node is recorded again (rather than ignored as a
 * duplicate). Acknowledgements of transactions not yet saved are kept: they
 * are still needed to record validations when the transaction is saved.
 */
export const forgetSavedAcknowledgementsFromNode = <Node>(
  node: Node,
  transactionCache: Iterable<[string, MempoolResyncCacheItem<Node>]>
) => {
  // eslint-disable-next-line functional/no-loop-statement
  for (const [, cacheItem] of transactionCache) {
    if (cacheItem.db && cacheItem.nodes.includes(node)) {
      cacheItem.nodes = cacheItem.nodes.filter((source) => source !== node);
    }
  }
};

/**
 * Requests the mempool of trusted nodes with BIP35 `mempool` messages.
 *
 * Nodes don't announce transactions which a re-organization returns to their
 * mempool, and the agent otherwise only hears about mempool transactions from
 * unsolicited announcements. To keep `node_transaction` current, the node's
 * mempool is requested:
 * - for every connected node once mempool tracking is enabled,
 * - when a node (re)connects after mempool tracking is enabled, and
 * - after a re-organization reported by the node has been handled.
 *
 * The node answers with `inv` messages, which are handled by the existing
 * announcement path. BIP35 replies may be split across many `inv` messages
 * and have no completion marker, so a reply can only ever add
 * `node_transaction` rows – a missing hash never proves absence.
 *
 * Requests are rate-limited per node: at most one request is sent per
 * `minimumIntervalMs`, and requests during that window are coalesced into one
 * request sent at the end of the window (so a re-organization shortly after a
 * request is never lost, and a flapping node can't cause a request storm).
 */
export class MempoolResync<Node extends MempoolResyncNode> {
  private readonly options: Required<MempoolResyncOptions<Node>>;

  private trackingEnabled = false;

  private stopped = false;

  private readonly lastRequestAt = new Map<string, number>();

  private readonly deferredRequests = new Map<
    string,
    { timer: unknown; triggers: Set<MempoolResyncTrigger> }
  >();

  constructor(options: MempoolResyncOptions<Node>) {
    this.options = {
      clearTimer: (timer) => {
        clearTimeout(timer as ReturnType<typeof setTimeout>);
      },
      now: Date.now,
      onRequestSent: () => undefined,
      setTimer: (callback, ms) => setTimeout(callback, ms),
      ...options,
    };
  }

  /**
   * Call when mempool tracking is enabled: requests the mempool of each given
   * node, and enables requests on later connections and re-organizations.
   */
  enableMempoolTracking(nodeNames: string[]) {
    this.trackingEnabled = true;
    nodeNames.forEach((nodeName) => {
      this.request(nodeName, 'mempool-tracking-enabled');
    });
  }

  /**
   * Call after a node has (re)connected and been registered.
   */
  handleConnect(nodeName: string) {
    this.request(nodeName, 'connect');
  }

  /**
   * Call after a re-organization reported by the node has been handled.
   */
  handleReorganization(nodeName: string) {
    this.request(nodeName, 'reorganization');
  }

  /**
   * Cancel any deferred requests and ignore all future triggers.
   */
  stop() {
    this.stopped = true;
    this.deferredRequests.forEach(({ timer }) => {
      this.options.clearTimer(timer);
    });
    this.deferredRequests.clear();
  }

  private isActive() {
    return this.options.enabled && this.trackingEnabled && !this.stopped;
  }

  private millisecondsUntilNextRequestAllowed(nodeName: string) {
    const lastRequestAt = this.lastRequestAt.get(nodeName);
    if (lastRequestAt === undefined) {
      return 0;
    }
    return Math.max(
      0,
      lastRequestAt + this.options.minimumIntervalMs - this.options.now()
    );
  }

  private request(nodeName: string, trigger: MempoolResyncTrigger) {
    if (!this.isActive()) {
      return;
    }
    const deferred = this.deferredRequests.get(nodeName);
    if (deferred !== undefined) {
      deferred.triggers.add(trigger);
      return;
    }
    const delay = this.millisecondsUntilNextRequestAllowed(nodeName);
    if (delay === 0) {
      this.send(nodeName, [trigger]);
      return;
    }
    const triggers = new Set<MempoolResyncTrigger>([trigger]);
    const timer = this.options.setTimer(() => {
      this.deferredRequests.delete(nodeName);
      this.send(nodeName, [...triggers]);
    }, delay);
    this.deferredRequests.set(nodeName, { timer, triggers });
  }

  private send(nodeName: string, triggers: MempoolResyncTrigger[]) {
    if (this.stopped) {
      return;
    }
    const node = this.options.getReadyNode(nodeName);
    if (node === undefined) {
      /**
       * The node is disconnected: it will be requested again on reconnect.
       */
      return;
    }
    forgetSavedAcknowledgementsFromNode(node, this.options.transactionCache);
    node.peer.sendMessage(new node.peer.messages.MemPool());
    this.lastRequestAt.set(nodeName, this.options.now());
    this.options.onRequestSent(nodeName, triggers);
  }
}
