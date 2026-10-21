export interface InitialSyncState {
  fullySyncedUpToHeight: number;
  pendingSyncOfHeights: number[];
  additionalSyncedHeights: number[];
}

/**
 * A simple data structure to keep track of the known database syncing progress
 * for a particular node.
 */
export class SyncState {
  fullySyncedUpToHeight: number;

  /**
   * The block time claimed by the latest synced block. Chaingraph assumes the
   * trusted node is itself not fully-synced if this time is older than 2 hours
   * (the consensus limit beyond which an inaccurate block time would render the
   * block invalid).
   *
   * As this value is used to estimate the sync progress of the trusted node, it
   * is not affected by block reorganizations.
   *
   * A value of 'caught-up' indicates to chaingraph that this node is not
   * actively syncing an unknown chain tip, and the sync state of this node can
   * be ignored when checking for initial sync completion. (If this node has
   * been synced or partially synced before, it will initialize with a value of
   * `caught-up` – if further syncing is required, this value will be quickly
   * replaced with the next synced block time.)
   *
   * A value of undefined indicates that this node has 1) not been synced
   * before, and 2) not yet successfully saved a block, so initial sync is not
   * complete.
   *
   * TODO: if other nodes were previously fully synced, initial sync should be skipped by default (to continue service for those nodes while the new node syncs)
   */
  latestSyncedBlockTime: Date | 'caught-up' | undefined;

  private pendingSyncHeight: number;

  private pendingSyncHeights: Set<number>;

  private additionalSyncedHeightSet: Set<number>;

  constructor(initialState: InitialSyncState) {
    this.fullySyncedUpToHeight = initialState.fullySyncedUpToHeight;
    this.pendingSyncHeights = new Set(initialState.pendingSyncOfHeights);
    this.additionalSyncedHeightSet = new Set(
      initialState.additionalSyncedHeights
    );
    this.pendingSyncHeight = this.computePendingSyncHeightFrom(
      this.fullySyncedUpToHeight
    );
    this.latestSyncedBlockTime =
      initialState.fullySyncedUpToHeight > 0 ? 'caught-up' : undefined;
  }

  /**
   * Heights currently pending sync, in the order they were marked. (Derived
   * from a `Set`: removing a height is O(1), so marking many heights as synced
   * – e.g. after `catchUpViaHeaders` accepts a long run of known blocks – is
   * linear rather than quadratic.)
   */
  get pendingSyncOfHeights() {
    return [...this.pendingSyncHeights];
  }

  /**
   * Heights above `fullySyncedUpToHeight` which have been synced, in the order
   * they were marked.
   */
  get additionalSyncedHeights() {
    return [...this.additionalSyncedHeightSet];
  }

  // eslint-disable-next-line complexity
  markHeightAsSynced(height: number, claimedBlockTime: Date | 'caught-up') {
    if (claimedBlockTime === 'caught-up') {
      this.latestSyncedBlockTime = 'caught-up';
    } else if (
      !(this.latestSyncedBlockTime instanceof Date) ||
      this.latestSyncedBlockTime < claimedBlockTime
    ) {
      this.latestSyncedBlockTime = claimedBlockTime;
    }

    if (
      this.fullySyncedUpToHeight < height &&
      !this.additionalSyncedHeightSet.has(height)
    ) {
      this.additionalSyncedHeightSet.add(height);
      this.pendingSyncHeights.delete(height);
    }

    // eslint-disable-next-line functional/no-let
    let nextHeight = this.fullySyncedUpToHeight + 1;
    // eslint-disable-next-line functional/no-loop-statement
    while (this.additionalSyncedHeightSet.delete(nextHeight)) {
      this.fullySyncedUpToHeight = nextHeight;
      nextHeight += 1;
    }
    this.updatePendingSyncHeight();
  }

  markHeightAsPendingSync(height: number) {
    if (
      this.fullySyncedUpToHeight < height &&
      !this.pendingSyncHeights.has(height) &&
      !this.additionalSyncedHeightSet.has(height)
    ) {
      this.pendingSyncHeights.add(height);
      this.updatePendingSyncHeight();
    }
  }

  /**
   * Discard all syncing state at and above the provided height (any previous
   * syncing progress has been replaced by a new chain). The new
   * `fullySyncedUpToHeight` will be the block below this `height`.
   *
   * @param height - the height at which to begin discarding (inclusive)
   */
  blockReorganizationAtHeight(height: number) {
    this.fullySyncedUpToHeight =
      height > this.fullySyncedUpToHeight
        ? this.fullySyncedUpToHeight
        : height - 1;

    this.pendingSyncHeights = new Set(
      this.pendingSyncOfHeights.filter((pending) => pending < height)
    );
    this.additionalSyncedHeightSet = new Set(
      this.additionalSyncedHeights.filter((completed) => completed < height)
    );
    this.resetPendingSyncHeight();
  }

  /**
   * Get the current height which has been or is currently being synced. This is
   * useful for prioritizing syncing.
   */
  getPendingSyncHeight() {
    return this.pendingSyncHeight;
  }

  private isHeightHandled(height: number) {
    return (
      this.pendingSyncHeights.has(height) ||
      this.additionalSyncedHeightSet.has(height)
    );
  }

  private computePendingSyncHeightFrom(height: number) {
    // eslint-disable-next-line functional/no-let
    let pendingSyncHeight = height;
    // eslint-disable-next-line functional/no-loop-statement
    while (this.isHeightHandled(pendingSyncHeight + 1)) {
      pendingSyncHeight += 1;
    }
    return pendingSyncHeight;
  }

  private updatePendingSyncHeight() {
    this.pendingSyncHeight = this.computePendingSyncHeightFrom(
      Math.max(this.pendingSyncHeight, this.fullySyncedUpToHeight)
    );
  }

  private resetPendingSyncHeight() {
    this.pendingSyncHeight = this.computePendingSyncHeightFrom(
      this.fullySyncedUpToHeight
    );
  }
}
