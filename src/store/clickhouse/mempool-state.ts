/* eslint-disable max-classes-per-file, @typescript-eslint/member-ordering, complexity, functional/no-mixed-type, max-params, functional/no-try-statement, class-methods-use-this */
// cspell:ignore clickhouse
/**
 * Per-node mempool state of the ClickHouse writer (WP5a-mempool).
 *
 * The in-memory mirror of every node's live `node_transaction` rows, rebuilt
 * at `init()` from the store and kept in step by every commit that changes a
 * node's mempool (mempool saves, validations, block and header acceptance,
 * the repair sweep, expiry). It is the input of the pure planner
 * (`../mempool-graph.ts`); nothing here reasons across nodes: every plan and
 * every row is for exactly one node.
 *
 * Per node: the entries (validated_at, spends, internal id and the spends
 * whose UTXO −1 row is still outstanding because the spent output was unknown
 * when the entry was added), the spender index and the children index.
 * Shared: the facts of every transaction in some node's mempool (outputs and
 * resolved spent outputs), reference-counted by node membership.
 *
 * Design and the end-state tables: docs/clickhouse-port/wp5a-mempool.md.
 */
import type {
  AcceptedInclusion,
  MempoolEntry,
  MempoolGraphIndex,
  NodeMempool,
  Outpoint,
  PlannedArchive,
  TxKey,
} from '../mempool-graph.js';
import {
  outpointTxHash,
  planAcceptedBlockCleanup,
  planDescendantCascade,
  planMempoolExpiry,
  planMempoolReplacement,
} from '../mempool-graph.js';

import type { Deferred, StoreOperation } from './node-state.js';
import type { UtxoOutput } from './utxo.js';

/** Kept for callers of WP5a-core; no method throws it any more. */
export class MempoolNotImplementedError extends Error {
  constructor(method: string) {
    super(
      `WP5a-mempool: ${method} is not implemented by the ClickHouse store yet.`
    );
  }
}

/** The node-agnostic facts of a transaction some node holds in its mempool. */
export interface MempoolTxFacts {
  hash: TxKey;
  internalId: bigint;
  /** Every output, as copied onto `utxo` rows. */
  outputs: readonly UtxoOutput[];
  /** The outpoint spent by each input, in input order. */
  spends: readonly Outpoint[];
  /** Spent outputs whose facts are known (others are unresolved). */
  spent: ReadonlyMap<Outpoint, UtxoOutput>;
}

/** One node's mempool entry. */
export interface LiveMempoolEntry extends MempoolEntry {
  internalId: bigint;
  /**
   * Spends whose UTXO −1 row has NOT been written for this node: the spent
   * output was unknown when the entry was added. Each is persisted as a live
   * `pending_spend` row (node, outpoint, spender) until it is resolved
   * (written when its creator becomes accepted by the node, or when the entry
   * is confirmed and the output is known) or dropped (the entry leaves the
   * mempool unconfirmed).
   */
  unresolved: Set<Outpoint>;
}

/** A planned archive for one node, with the entry it removes. */
export interface NodeArchive extends PlannedArchive {
  entry: LiveMempoolEntry;
  /** Captured at plan time: the shared facts are released when applied. */
  facts: MempoolTxFacts;
}

/** An outstanding spend whose −1 row is written now. */
export interface SpendResolution {
  spender: TxKey;
  outpoint: Outpoint;
  inputIndex: number;
  output: UtxoOutput;
}

/** A new mempool entry for one node. */
export interface NodeAddition {
  facts: MempoolTxFacts;
  validatedAt: Date;
}

/**
 * Everything one commit changes in one node's mempool: at most one addition,
 * archives (direct + cascade), resolutions of outstanding spends, and
 * `immediate`: history-only rows for a validation that never enters the
 * mempool (the transaction is already confirmed for the node, or conflicts
 * with a confirmed one; Postgres's sweep end state).
 */
export interface NodeMempoolChange {
  node: number;
  addition?: NodeAddition;
  archives: NodeArchive[];
  resolutions: SpendResolution[];
  immediate?: {
    facts: MempoolTxFacts;
    validatedAt: Date;
    replacedAt: Date | null;
  };
}

export const isEmptyChange = (change: NodeMempoolChange) =>
  change.addition === undefined &&
  change.immediate === undefined &&
  change.archives.length === 0 &&
  change.resolutions.length === 0;

/** A mempool tx held back because a spent output is unknown. */
export interface OrphanEntry {
  receivedAt: Date;
  missingParents: readonly TxKey[];
  /** node → validated_at, merged from every save/validation while parked */
  validations: Map<number, Date>;
  done: Deferred<void>;
  release: () => void;
}

const pushUnique = <K, V>(map: Map<K, V[]>, key: K, value: V) => {
  const list = map.get(key);
  if (list === undefined) {
    map.set(key, [value]);
  } else if (!list.includes(value)) {
    list.push(value);
  }
};

const removeFrom = <K, V>(map: Map<K, V[]>, key: K, value: V) => {
  const list = map.get(key);
  if (list === undefined) return;
  const rest = list.filter((item) => item !== value);
  if (rest.length === 0) {
    map.delete(key);
  } else {
    map.set(key, rest);
  }
};

/** One node's mempool with incrementally maintained indexes. */
export class NodeMempoolState implements NodeMempool {
  readonly txs = new Map<TxKey, LiveMempoolEntry>();

  readonly spenders = new Map<Outpoint, TxKey[]>();

  readonly children = new Map<TxKey, TxKey[]>();

  /** creator tx → mempool txs with an outstanding spend of one of its outputs */
  readonly waitingOnCreator = new Map<TxKey, TxKey[]>();

  get index(): MempoolGraphIndex {
    return { children: this.children, spenders: this.spenders };
  }

  add(tx: TxKey, entry: LiveMempoolEntry) {
    this.txs.set(tx, entry);
    entry.spends.forEach((spent) => {
      pushUnique(this.spenders, spent, tx);
      pushUnique(this.children, outpointTxHash(spent), tx);
    });
    entry.unresolved.forEach((spent) => {
      pushUnique(this.waitingOnCreator, outpointTxHash(spent), tx);
    });
  }

  remove(tx: TxKey) {
    const entry = this.txs.get(tx);
    if (entry === undefined) return undefined;
    this.txs.delete(tx);
    entry.spends.forEach((spent) => {
      removeFrom(this.spenders, spent, tx);
      removeFrom(this.children, outpointTxHash(spent), tx);
    });
    entry.unresolved.forEach((spent) => {
      removeFrom(this.waitingOnCreator, outpointTxHash(spent), tx);
    });
    return entry;
  }

  resolve(tx: TxKey, spent: Outpoint) {
    const entry = this.txs.get(tx);
    if (entry === undefined || !entry.unresolved.delete(spent)) return;
    const creator = outpointTxHash(spent);
    if (![...entry.unresolved].some((o) => outpointTxHash(o) === creator)) {
      removeFrom(this.waitingOnCreator, creator, tx);
    }
  }
}

/**
 * The per-node mempools plus the shared transaction facts and the orphan
 * pool. All mutation goes through `apply`, called by the operation that
 * planned the change, synchronously after planning (so a later operation of
 * the node, which waits for this one's rows, plans against it).
 */
export class MempoolState {
  /** Set by `enableMempoolTracking` (after the bulk horizon). */
  tracking = false;

  private readonly mempools = new Map<number, NodeMempoolState>();

  private readonly facts = new Map<
    TxKey,
    { facts: MempoolTxFacts; refs: number }
  >();

  /** Orphan pool: tx hash → entry (mempool commits are never incomplete). */
  readonly orphans = new Map<TxKey, OrphanEntry>();

  /**
   * Live operations that changed (or will change) a node's mempool; a later
   * operation that plans against the node's mempool depends on them.
   */
  private readonly modifiers = new Map<number, Set<StoreOperation>>();

  /** Nodes whose in-memory mempool may be ahead of the store (a modifier failed). */
  readonly stale = new Set<number>();

  node(nodeInternalId: number): NodeMempoolState {
    const existing = this.mempools.get(nodeInternalId);
    if (existing !== undefined) return existing;
    const created = new NodeMempoolState();
    this.mempools.set(nodeInternalId, created);
    return created;
  }

  nodeMempool(nodeInternalId: number): NodeMempool {
    return this.node(nodeInternalId);
  }

  get nodeIds(): number[] {
    return [...this.mempools.keys()].sort((a, b) => a - b);
  }

  isEmpty(nodeInternalId: number) {
    return (this.mempools.get(nodeInternalId)?.txs.size ?? 0) === 0;
  }

  factsOf(tx: TxKey): MempoolTxFacts | undefined {
    return this.facts.get(tx)?.facts;
  }

  /** Replace a node's mempool (rebuild at init or after a failure). */
  reset(nodeInternalId: number, entries: [MempoolTxFacts, LiveMempoolEntry][]) {
    const old = this.mempools.get(nodeInternalId);
    old?.txs.forEach((_, tx) => {
      this.release(tx);
    });
    const fresh = new NodeMempoolState();
    this.mempools.set(nodeInternalId, fresh);
    entries.forEach(([facts, entry]) => {
      this.retain(facts);
      fresh.add(facts.hash, entry);
    });
    this.stale.delete(nodeInternalId);
  }

  /* -------------------------------------------- operation bookkeeping */

  /** Register `operation` as a (possible) modifier of the nodes' mempools. */
  addModifier(operation: StoreOperation, nodes: readonly number[]) {
    nodes.forEach((node) => {
      const set = this.modifiers.get(node) ?? new Set<StoreOperation>();
      set.add(operation);
      this.modifiers.set(node, set);
    });
  }

  /** A modifier failed: its nodes' in-memory mempools may be ahead of the store. */
  markStale(operation: StoreOperation) {
    this.modifiers.forEach((set, node) => {
      if (set.has(operation)) this.stale.add(node);
    });
  }

  removeModifier(operation: StoreOperation) {
    this.modifiers.forEach((set, node) => {
      set.delete(operation);
      if (set.size === 0) {
        this.modifiers.delete(node);
      }
    });
  }

  /**
   * Live modifiers of `node` registered before `operation` (a later one
   * waits for this operation's rows, so depending on it would be a cycle).
   */
  modifiersOf(node: number, operation: StoreOperation): StoreOperation[] {
    return [...(this.modifiers.get(node) ?? [])].filter(
      (other) =>
        other !== operation && !other.finished && other.id < operation.id
    );
  }

  /** True if the node's mempool is non-empty or a live operation may change it. */
  mayHaveMempool(node: number) {
    return !this.isEmpty(node) || (this.modifiers.get(node)?.size ?? 0) > 0;
  }

  /* ------------------------------------------------------- planning */

  /**
   * Block-path plan for one node accepting blocks: confirmations, conflicts
   * and cascade (`planAcceptedBlockCleanup`), then the outstanding spends
   * that can be written now: of entries that stay in the mempool, those
   * whose creator is among `becomingAccepted` (it becomes accepted by the
   * node in this commit, so its +1 row is written now too), and of confirmed
   * entries, those whose spent output is known (`knownOutput`).
   */
  planBlockAcceptance(
    nodeInternalId: number,
    inclusions: readonly AcceptedInclusion[],
    creatorOutputs: (tx: TxKey) => readonly UtxoOutput[] | undefined,
    knownOutput: (spent: Outpoint) => UtxoOutput | undefined
  ): NodeMempoolChange {
    const mempool = this.node(nodeInternalId);
    const change: NodeMempoolChange = {
      archives: [],
      node: nodeInternalId,
      resolutions: [],
    };
    if (mempool.txs.size === 0) {
      return change;
    }
    const plan = planAcceptedBlockCleanup(mempool, inclusions, mempool.index);
    change.archives = this.withEntries(mempool, plan);
    const archived = new Set(change.archives.map((archive) => archive.tx));
    const confirmed = new Set(
      change.archives
        .filter((archive) => archive.cause === 'confirmed')
        .map((archive) => archive.tx)
    );
    const seen = new Set<string>();
    const push = (spender: TxKey, spent: Outpoint, output: UtxoOutput) => {
      const key = `${spender}|${spent}`;
      if (seen.has(key)) return;
      seen.add(key);
      const facts = this.factsOf(spender);
      change.resolutions.push({
        inputIndex: facts?.spends.indexOf(spent) ?? 0,
        outpoint: spent,
        output,
        spender,
      });
    };
    mempool.waitingOnCreator.forEach((spenders, creator) => {
      const outputs = creatorOutputs(creator);
      if (outputs === undefined) return;
      spenders.forEach((spender) => {
        if (archived.has(spender) && !confirmed.has(spender)) return;
        mempool.txs.get(spender)?.unresolved.forEach((spent) => {
          if (outpointTxHash(spent) !== creator) return;
          const index = Number(spent.slice(spent.lastIndexOf(':') + 1));
          const output = outputs[index];
          if (output !== undefined) push(spender, spent, output);
        });
      });
    });
    confirmed.forEach((tx) => {
      mempool.txs.get(tx)?.unresolved.forEach((spent) => {
        const output = knownOutput(spent);
        if (output !== undefined) push(tx, spent, output);
      });
    });
    return change;
  }

  /**
   * Plan a node's validation of `facts` at `validatedAt`
   * (`trigger_node_transaction_insert`): `undefined` if the node already has
   * it in its mempool (ON CONFLICT DO NOTHING); `confirmedFor` is set if the
   * node already accepts the transaction in a block (history row with
   * replaced_at NULL, never in the mempool), `conflictReplacedAt` if a block
   * the node accepts spends one of its outpoints (history row with the
   * sweep's MIN(accepted_at), plus cascade). Otherwise: the addition, the
   * replacement archives (`planMempoolReplacement`, planned against the
   * mempool with the new entry, as the AFTER INSERT trigger sees it), and the
   * outstanding spends of other entries whose creator is this transaction.
   */
  planValidation(
    nodeInternalId: number,
    facts: MempoolTxFacts,
    validatedAt: Date,
    confirmed: {
      confirmedFor: boolean;
      conflictReplacedAt: Date | null | undefined;
    }
  ): NodeMempoolChange | undefined {
    const mempool = this.node(nodeInternalId);
    if (mempool.txs.has(facts.hash)) {
      return undefined;
    }
    const change: NodeMempoolChange = {
      archives: [],
      node: nodeInternalId,
      resolutions: [],
    };
    if (confirmed.confirmedFor || confirmed.conflictReplacedAt !== undefined) {
      const replacedAt = confirmed.confirmedFor
        ? null
        : confirmed.conflictReplacedAt ?? null;
      change.immediate = { facts, replacedAt, validatedAt };
      change.archives = this.withEntries(
        mempool,
        planDescendantCascade(
          mempool,
          [
            {
              cause: confirmed.confirmedFor ? 'confirmed' : 'conflict',
              replacedAt,
              tx: facts.hash,
            },
          ],
          mempool.index
        )
      );
      return change;
    }
    const entry = this.entryFor(facts, validatedAt);
    change.addition = { facts, validatedAt };
    mempool.add(facts.hash, entry);
    try {
      const plan = planMempoolReplacement(
        mempool,
        { spends: facts.spends, tx: facts.hash, validatedAt },
        mempool.index
      );
      change.archives = this.withEntries(mempool, plan);
    } finally {
      mempool.remove(facts.hash);
    }
    const archived = new Set(change.archives.map((archive) => archive.tx));
    (mempool.waitingOnCreator.get(facts.hash) ?? []).forEach((spender) => {
      if (archived.has(spender)) return;
      const waiting = mempool.txs.get(spender);
      waiting?.unresolved.forEach((spent) => {
        if (outpointTxHash(spent) !== facts.hash) return;
        const index = Number(spent.slice(spent.lastIndexOf(':') + 1));
        const output = facts.outputs[index];
        if (output === undefined) return;
        change.resolutions.push({
          inputIndex: this.factsOf(spender)?.spends.indexOf(spent) ?? 0,
          outpoint: spent,
          output,
          spender,
        });
      });
    });
    return change;
  }

  /** Expiry of one entry (`archiveMempoolTransaction`): the entry + cascade. */
  planExpiry(
    nodeInternalId: number,
    tx: TxKey,
    replacedAt: Date
  ): NodeMempoolChange {
    const mempool = this.node(nodeInternalId);
    return {
      archives: this.withEntries(
        mempool,
        planMempoolExpiry(mempool, { replacedAt, tx }, mempool.index)
      ),
      node: nodeInternalId,
      resolutions: [],
    };
  }

  entryFor(facts: MempoolTxFacts, validatedAt: Date): LiveMempoolEntry {
    return {
      internalId: facts.internalId,
      spends: facts.spends,
      unresolved: new Set(
        facts.spends.filter((spent) => !facts.spent.has(spent))
      ),
      validatedAt,
    };
  }

  /* ------------------------------------------------------- applying */

  /** Apply a planned change to the node's in-memory mempool. */
  apply(change: NodeMempoolChange) {
    const mempool = this.node(change.node);
    change.resolutions.forEach((resolution) => {
      mempool.resolve(resolution.spender, resolution.outpoint);
    });
    change.archives.forEach((archive) => {
      if (mempool.remove(archive.tx) !== undefined) {
        this.release(archive.tx);
      }
    });
    if (change.addition !== undefined) {
      const { facts, validatedAt } = change.addition;
      this.retain(facts);
      mempool.add(facts.hash, this.entryFor(facts, validatedAt));
    }
  }

  private withEntries(
    mempool: NodeMempoolState,
    plan: readonly PlannedArchive[]
  ): NodeArchive[] {
    return plan.flatMap((archive) => {
      const entry = mempool.txs.get(archive.tx);
      const facts = this.factsOf(archive.tx);
      return entry === undefined || facts === undefined
        ? []
        : [{ ...archive, entry, facts }];
    });
  }

  private retain(facts: MempoolTxFacts) {
    const known = this.facts.get(facts.hash);
    if (known === undefined) {
      this.facts.set(facts.hash, { facts, refs: 1 });
    } else {
      known.refs += 1;
    }
  }

  private release(tx: TxKey) {
    const known = this.facts.get(tx);
    if (known === undefined) return;
    known.refs -= 1;
    if (known.refs <= 0) {
      this.facts.delete(tx);
    }
  }
}
