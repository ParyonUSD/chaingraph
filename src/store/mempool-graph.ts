/**
 * Pure, DB-free planner for per-node mempool cleanup.
 *
 * Every function here takes exactly ONE node's mempool and returns the
 * node_transaction rows that node must archive to node_transaction_history.
 * Nothing reasons across nodes: confirmations, conflicts and cascades seen by
 * one node never touch another node's mempool. Callers that track several
 * nodes call the planner once per node.
 *
 * The semantics reproduce the Postgres implementation exactly, so both
 * backends archive the same rows with the same `replaced_at`:
 * - `planAcceptedBlockCleanup`: the `archiveMempoolTransactionsAcceptedByBlocks`
 *   sweep in `src/db.ts` (and, for a single newly accepted block, the
 *   `trigger_node_block_insert` trigger);
 * - `planMempoolReplacement`: the `trigger_node_transaction_insert` trigger;
 * - `planMempoolExpiry`: `archiveMempoolTransaction` in `src/db.ts`;
 * - `planDescendantCascade`: the `trigger_node_transaction_history_insert`
 *   cascade (migration `1778151011521_cascade_invalidate_mempool_descendants`),
 *   which every variant above fires.
 *
 * The shared vectors in `mempool-cleanup.vectors.ts` are run through both this
 * planner (`mempool-graph.spec.ts`) and the SQL (`e2e/mempool-cleanup.spec.ts`).
 */

/**
 * A transaction key: the transaction hash as lowercase hex (display order is
 * irrelevant as long as every caller uses the same encoding).
 */
export type TxKey = string;

/**
 * An outpoint: `<transaction hash hex>:<output index>`.
 */
export type Outpoint = `${string}:${number}`;

export interface MempoolEntry {
  /**
   * The node's `validated_at` for this transaction.
   */
  validatedAt: Date;
  /**
   * The outpoints spent by this transaction's inputs.
   */
  spends: readonly Outpoint[];
}

/**
 * One node's mempool (its node_transaction rows).
 */
export interface NodeMempool {
  txs: ReadonlyMap<TxKey, MempoolEntry>;
}

/**
 * A transaction included in a block that this node has accepted (one
 * block_transaction row joined to one of this node's node_block rows). A
 * transaction included in several accepted blocks appears once per block.
 */
export interface AcceptedInclusion {
  tx: TxKey;
  /**
   * The outpoints spent by the included transaction's inputs.
   */
  spends: readonly Outpoint[];
  /**
   * The node's `node_block.accepted_at` for the including block: NULL when the
   * block was not accepted live (historical sync, header acceptance).
   */
  acceptedAt: Date | null;
}

/**
 * Why a row is archived:
 * - `confirmed`: the transaction itself is in an accepted block;
 * - `conflict`: a different transaction spending one of its outpoints is in an
 *   accepted block;
 * - `replaced`: the node validated a different transaction spending one of its
 *   outpoints;
 * - `expired`: archived by the expiry sweep;
 * - `descendant`: spends an output of a transaction archived with a non-null
 *   `replacedAt` (cascade).
 *
 * The Postgres sweep returns only the direct rows (everything but
 * `descendant`); descendant rows are written by the history trigger.
 */
export type ArchiveCause =
  | 'confirmed'
  | 'conflict'
  | 'descendant'
  | 'expired'
  | 'replaced';

export interface PlannedArchive {
  tx: TxKey;
  /**
   * NULL means "confirmed" to readers of node_transaction_history; see the
   * NULL `accepted_at` quirk on `planAcceptedBlockCleanup` for the exception.
   */
  replacedAt: Date | null;
  cause: ArchiveCause;
}

/**
 * Lookup structures over one node's mempool.
 */
export interface MempoolGraphIndex {
  /**
   * Outpoint → mempool transactions spending it.
   */
  spenders: ReadonlyMap<Outpoint, readonly TxKey[]>;
  /**
   * Transaction → mempool transactions spending any of its outputs.
   */
  children: ReadonlyMap<TxKey, readonly TxKey[]>;
}

/**
 * The null outpoint hash used by coinbase inputs.
 */
const hashBytes = 32;
export const coinbaseOutpointHash = '00'.repeat(hashBytes);

export const outpoint = (txHash: TxKey, outputIndex: number): Outpoint =>
  `${txHash}:${outputIndex}`;

export const outpointTxHash = (spentOutpoint: Outpoint): TxKey =>
  spentOutpoint.slice(0, spentOutpoint.lastIndexOf(':'));

const pushToMapList = <Key, Value>(
  map: Map<Key, Value[]>,
  key: Key,
  value: Value
) => {
  const list = map.get(key);
  if (list === undefined) {
    map.set(key, [value]);
  } else if (!list.includes(value)) {
    list.push(value);
  }
};

/**
 * Build the spender and children indexes for one node's mempool. The
 * ClickHouse backend keeps these incrementally; this builds them from scratch.
 */
export const indexMempool = (mempool: NodeMempool): MempoolGraphIndex => {
  const spenders = new Map<Outpoint, TxKey[]>();
  const children = new Map<TxKey, TxKey[]>();
  mempool.txs.forEach((entry, tx) => {
    entry.spends.forEach((spentOutpoint) => {
      pushToMapList(spenders, spentOutpoint, tx);
      pushToMapList(children, outpointTxHash(spentOutpoint), tx);
    });
  });
  return { children, spenders };
};

const earlier = (a: Date | null, b: Date | null) =>
  a === null ? b : b === null ? a : a.getTime() <= b.getTime() ? a : b;

const compareArchives = (a: PlannedArchive, b: PlannedArchive) =>
  a.tx < b.tx ? -1 : a.tx > b.tx ? 1 : 0;

/**
 * The descendant cascade of `trigger_node_transaction_history_insert`.
 *
 * Seeds are the directly archived rows with a non-null `replacedAt`; rows with
 * a NULL `replacedAt` (confirmations) never cascade. The walk only visits
 * transactions still in the mempool after the direct archive, i.e. it never
 * passes through a directly archived transaction: in Postgres the direct rows are already deleted
 * from node_transaction when the AFTER STATEMENT trigger runs, so a directly
 * archived transaction keeps its own `replacedAt` and only seeds its own
 * descendants. A descendant reachable from several seeds gets the earliest
 * seed's `replacedAt` (`MIN(replaced_at)` in the trigger).
 *
 * @param mempool - the node's mempool before the direct archive
 * @param direct - every directly archived row (any `replacedAt`)
 * @param index - indexes over `mempool`
 */
export const planDescendantCascade = (
  mempool: NodeMempool,
  direct: readonly PlannedArchive[],
  index: MempoolGraphIndex = indexMempool(mempool)
): PlannedArchive[] => {
  const removed = new Set(direct.map((archive) => archive.tx));
  const best = new Map<TxKey, Date>();
  const queue: { tx: TxKey; replacedAt: Date }[] = [];
  direct.forEach((seed) => {
    if (seed.replacedAt !== null) {
      queue.push({ replacedAt: seed.replacedAt, tx: seed.tx });
    }
  });
  // eslint-disable-next-line functional/no-loop-statement
  while (queue.length > 0) {
    const parent = queue.shift()!;
    (index.children.get(parent.tx) ?? []).forEach((child) => {
      if (!mempool.txs.has(child) || removed.has(child)) return;
      const current = best.get(child);
      if (
        current === undefined ||
        parent.replacedAt.getTime() < current.getTime()
      ) {
        best.set(child, parent.replacedAt);
        queue.push({ replacedAt: parent.replacedAt, tx: child });
      }
    });
  }
  return [...best.entries()]
    .map(
      ([tx, replacedAt]): PlannedArchive => ({
        cause: 'descendant',
        replacedAt,
        tx,
      })
    )
    .sort(compareArchives);
};

const withCascade = (
  mempool: NodeMempool,
  direct: PlannedArchive[],
  index: MempoolGraphIndex
) => {
  const sortedDirect = [...direct].sort(compareArchives);
  return [
    ...sortedDirect,
    ...planDescendantCascade(mempool, sortedDirect, index),
  ];
};

/**
 * Plan the archive caused by blocks this node has accepted
 * (`archiveMempoolTransactionsAcceptedByBlocks`).
 *
 * 1. Confirmed: mempool transactions that are themselves included →
 *    `replacedAt` NULL. Confirmation wins over any conflict
 *    (`bool_or(replaced_at IS NULL)` in the sweep).
 * 2. Conflicts: other mempool transactions spending an outpoint that an
 *    inclusion spends (the coinbase null-hash outpoint is skipped; a
 *    transaction never conflicts with itself) → `replacedAt` = the earliest
 *    `acceptedAt` over the inclusions it conflicts with (`MIN(accepted_at)`).
 * 3. Cascade: mempool descendants of the conflicts only; descendants of
 *    confirmed transactions stay.
 *
 * NULL `accepted_at` quirk (reproduced deliberately, candidate fix for BOTH
 * backends): `MIN` ignores NULLs, so a conflict whose only conflicting
 * inclusions are in blocks with a NULL `accepted_at` (blocks not accepted
 * live, header-accepted blocks) is archived with `replacedAt` NULL, which
 * readers cannot tell apart from a confirmation, and, because the cascade
 * only follows non-null rows, its descendants are left in the mempool even
 * though they can no longer confirm. If one inclusion has a non-null
 * `acceptedAt`, that one is used. See the `null-accepted-at` vector.
 *
 * The result lists direct rows (sorted by tx) followed by descendant rows
 * (sorted by tx). Each transaction appears at most once.
 */
export const planAcceptedBlockCleanup = (
  mempool: NodeMempool,
  inclusions: readonly AcceptedInclusion[],
  index: MempoolGraphIndex = indexMempool(mempool)
): PlannedArchive[] => {
  const confirmed = new Set(
    inclusions
      .map((inclusion) => inclusion.tx)
      .filter((tx) => mempool.txs.has(tx))
  );
  const conflicts = new Map<TxKey, Date | null>();
  inclusions.forEach((inclusion) => {
    inclusion.spends.forEach((spentOutpoint) => {
      if (outpointTxHash(spentOutpoint) === coinbaseOutpointHash) return;
      (index.spenders.get(spentOutpoint) ?? []).forEach((spender) => {
        if (spender === inclusion.tx || confirmed.has(spender)) return;
        if (!mempool.txs.has(spender)) return;
        conflicts.set(
          spender,
          conflicts.has(spender)
            ? earlier(conflicts.get(spender)!, inclusion.acceptedAt)
            : inclusion.acceptedAt
        );
      });
    });
  });
  const direct: PlannedArchive[] = [
    ...[...confirmed].map(
      (tx): PlannedArchive => ({ cause: 'confirmed', replacedAt: null, tx })
    ),
    ...[...conflicts.entries()].map(
      ([tx, replacedAt]): PlannedArchive => ({
        cause: 'conflict',
        replacedAt,
        tx,
      })
    ),
  ];
  return withCascade(mempool, direct, index);
};

/**
 * Plan the archive caused by this node validating one new transaction
 * (`trigger_node_transaction_insert`): every other mempool transaction
 * spending one of the new transaction's outpoints is archived with
 * `replacedAt` = the new transaction's `validatedAt`, then cascades.
 *
 * Unlike the block sweep, the trigger does not skip the coinbase outpoint (a
 * mempool transaction never spends it). Pass `mempool` as it is after the new
 * transaction is added, as the AFTER INSERT trigger sees it; the new
 * transaction is never its own conflict. The trigger is statement-level: a
 * batch inserting two mutually conflicting transactions would archive both,
 * and a row conflicting with two new transactions gets an arbitrary one's
 * `validated_at`. This variant plans one transaction at a time, which is the
 * only well-defined case.
 */
export const planMempoolReplacement = (
  mempool: NodeMempool,
  replacement: {
    tx: TxKey;
    spends: readonly Outpoint[];
    validatedAt: Date;
  },
  index: MempoolGraphIndex = indexMempool(mempool)
): PlannedArchive[] => {
  const replaced = new Set<TxKey>();
  replacement.spends.forEach((spentOutpoint) => {
    (index.spenders.get(spentOutpoint) ?? []).forEach((spender) => {
      if (spender !== replacement.tx && mempool.txs.has(spender)) {
        replaced.add(spender);
      }
    });
  });
  return withCascade(
    mempool,
    [...replaced].map(
      (tx): PlannedArchive => ({
        cause: 'replaced',
        replacedAt: replacement.validatedAt,
        tx,
      })
    ),
    index
  );
};

/**
 * Plan the archive of one expired mempool transaction
 * (`archiveMempoolTransaction`): the transaction with the given `replacedAt`,
 * then its cascade. Returns `[]` if the transaction is not in the mempool.
 */
export const planMempoolExpiry = (
  mempool: NodeMempool,
  expired: { tx: TxKey; replacedAt: Date },
  index: MempoolGraphIndex = indexMempool(mempool)
): PlannedArchive[] =>
  mempool.txs.has(expired.tx)
    ? withCascade(
        mempool,
        [{ cause: 'expired', replacedAt: expired.replacedAt, tx: expired.tx }],
        index
      )
    : [];

/**
 * Remove planned archives from a mempool, returning the remaining mempool.
 */
export const applyArchives = (
  mempool: NodeMempool,
  archives: readonly PlannedArchive[]
): NodeMempool => {
  const removed = new Set(archives.map((archive) => archive.tx));
  return {
    txs: new Map([...mempool.txs].filter(([tx]) => !removed.has(tx))),
  };
};
