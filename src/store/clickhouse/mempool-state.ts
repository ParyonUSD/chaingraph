/* eslint-disable max-classes-per-file, @typescript-eslint/member-ordering */
// cspell:ignore clickhouse
/**
 * Per-node mempool state of the ClickHouse writer: SKELETON (WP5a-core).
 *
 * WP5a-mempool fills this in: the per-node mempool graphs (rebuilt at start
 * from `node_transaction_at` ⋈ `input`), the orphan pool for mempool txs whose
 * parents are unknown (mempool commits are never incomplete, WP4 §3), and
 * the planner calls for replacement, cascade and expiry.
 *
 * WP5a-core only needs the block-path hook: when a node accepts a block (block
 * save or header acceptance), the block commit asks the node's mempool what
 * to archive (`planAcceptedBlockCleanup`). In WP5a-core every mempool is
 * empty, so the plan is always empty and nothing is written.
 */
import type {
  AcceptedInclusion,
  MempoolEntry,
  NodeMempool,
  PlannedArchive,
  TxKey,
} from '../mempool-graph.js';
import { planAcceptedBlockCleanup } from '../mempool-graph.js';

/** Thrown by the mempool methods WP5a-core leaves to the next task. */
export class MempoolNotImplementedError extends Error {
  constructor(method: string) {
    super(
      `WP5a-mempool: ${method} is not implemented by the ClickHouse store yet.`
    );
  }
}

/** A mempool tx held back because a parent is unknown (WP5a-mempool). */
export interface OrphanEntry {
  receivedAt: Date;
  missingParents: readonly TxKey[];
}

export class MempoolState {
  /** Set by `enableMempoolTracking` (after the bulk horizon). */
  tracking = false;

  /** node internal id → that node's mempool (node_transaction live rows). */
  private readonly mempools = new Map<number, Map<TxKey, MempoolEntry>>();

  /** Orphan pool (WP5a-mempool): tx hash → entry. Always empty here. */
  readonly orphans = new Map<TxKey, OrphanEntry>();

  nodeMempool(nodeInternalId: number): NodeMempool {
    return {
      txs: this.mempools.get(nodeInternalId) ?? new Map<TxKey, MempoolEntry>(),
    };
  }

  isEmpty(nodeInternalId: number) {
    return (this.mempools.get(nodeInternalId)?.size ?? 0) === 0;
  }

  /**
   * Block-path hook: node `nodeInternalId` accepts blocks whose transactions
   * are `inclusions()` (built lazily: only needed if the mempool is not
   * empty). Returns the archives to write in the same commit (confirmations,
   * conflicts and their cascade). WP5a-core: always `[]` (empty mempools);
   * a non-empty plan means WP5a-mempool must write the rows, so the commit
   * refuses rather than silently dropping them.
   */
  planBlockAcceptance(
    nodeInternalId: number,
    inclusions: () => readonly AcceptedInclusion[]
  ): PlannedArchive[] {
    if (this.isEmpty(nodeInternalId)) {
      return [];
    }
    const plan = planAcceptedBlockCleanup(
      this.nodeMempool(nodeInternalId),
      inclusions()
    );
    if (plan.length > 0) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new MempoolNotImplementedError(
        'archiving mempool transactions confirmed or conflicted by a block'
      );
    }
    return plan;
  }

  /**
   * Header-acceptance hook (the inclusions live in the store, not in memory).
   * WP5a-core: the mempool is empty, so there is nothing to plan.
   */
  assertNoMempoolForHeaderAcceptance(nodeInternalId: number) {
    if (!this.isEmpty(nodeInternalId)) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new MempoolNotImplementedError(
        'mempool cleanup for header-accepted blocks'
      );
    }
  }
}
