/* eslint-disable functional/no-let, functional/no-mixed-type, @typescript-eslint/no-empty-function, @typescript-eslint/parameter-properties, @typescript-eslint/member-ordering */
// cspell:ignore notfound
/**
 * Transactions the agent requested from a node (`getdata` after an `inv`)
 * and has not handled yet, per node. A transaction received from a node
 * whose parent (a transaction its inputs spend) is still requested from that
 * node waits until the parent has been handled (saved, or found already
 * saved), so a node's transactions are saved parents first, whatever order
 * they arrive in (pre-phase2-concurrency-mempool.md §2).
 *
 * A request that is never answered stops holding its children after
 * `timeoutMs`, and on `notfound` or a disconnect (`settleNode`).
 */
import type { ChaingraphTransaction } from '../types/chaingraph.js';

interface Request {
  settled: Promise<void>;
  settle: () => void;
  timer: ReturnType<typeof setTimeout>;
}

export class RequestedTransactions {
  private readonly byNode = new Map<string, Map<string, Request>>();

  constructor(private readonly timeoutMs: number) {}

  private requestsOf(node: string) {
    let requests = this.byNode.get(node);
    if (requests === undefined) {
      requests = new Map();
      this.byNode.set(node, requests);
    }
    return requests;
  }

  /** Note a request of `hash` from `node`; false if one is pending already. */
  request(node: string, hash: string): boolean {
    const requests = this.requestsOf(node);
    if (requests.has(hash)) return false;
    let settle = () => {};
    const settled = new Promise<void>((resolve) => {
      settle = resolve;
    });
    const timer = setTimeout(() => {
      this.settle(node, hash);
    }, this.timeoutMs);
    timer.unref();
    requests.set(hash, { settle, settled, timer });
    return true;
  }

  /** `hash` from `node` has been handled (or will never come). */
  settle(node: string, hash: string) {
    const requests = this.byNode.get(node);
    const request = requests?.get(hash);
    if (request === undefined) return;
    clearTimeout(request.timer);
    requests!.delete(hash);
    request.settle();
  }

  /** Every request pending from `node` (e.g. it disconnected). */
  settleNode(node: string) {
    [...(this.byNode.get(node)?.keys() ?? [])].forEach((hash) => {
      this.settle(node, hash);
    });
  }

  pendingCount(node: string) {
    return this.byNode.get(node)?.size ?? 0;
  }

  /**
   * The parents of `transaction` still requested from `node`; resolves once
   * each of them is settled (returns their hashes).
   */
  async awaitParents(
    node: string,
    transaction: ChaingraphTransaction
  ): Promise<string[]> {
    const requests = this.byNode.get(node);
    if (requests === undefined || requests.size === 0) return [];
    const parents = [
      ...new Set(
        transaction.inputs.map((input) => input.outpointTransactionHash)
      ),
    ].filter((hash) => hash !== transaction.hash && requests.has(hash));
    await Promise.all(parents.map(async (hash) => requests.get(hash)!.settled));
    return parents;
  }
}
