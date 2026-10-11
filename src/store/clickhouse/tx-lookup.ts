/* eslint-disable functional/no-let, @typescript-eslint/naming-convention, @typescript-eslint/no-loop-func, complexity, no-bitwise, functional/no-loop-statement, @typescript-eslint/parameter-properties, @typescript-eslint/no-magic-numbers, max-classes-per-file, @typescript-eslint/member-ordering */
// cspell:ignore clickhouse coinbases unhex txlookup
/**
 * The "is this transaction already stored, and its id" lookup of the block
 * and mempool paths (docs/clickhouse-port/spend-index.md, "Transaction-hash
 * lookup"). The chipnet sync issued 324k `FROM transaction WHERE hash IN`
 * queries for 2.8M transactions (most of them one hash: one query per block,
 * per event-loop turn), 999 s of ClickHouse time and 319 GiB read.
 *
 * Two parts:
 * - `TransactionLookup`: one shared queue. Requests of every block and
 *   mempool save are coalesced into sorted chunks of at most `chunkSize`
 *   hashes, at most `concurrency` queries at once; a request on an idle
 *   lookup starts at once (the tip), later ones gather while queries run (a
 *   sync).
 * - `NewTransactionProof`: the transactions of a block that cannot be
 *   stored yet, so need no lookup: during an initial sync of a database
 *   whose stored blocks are one chain (one block per height, never a re-org,
 *   never a mempool transaction, one node), a block above the stored tip
 *   that extends it in this process (directly, or through blocks that did) is
 *   new, and so are its transactions: a non-coinbase transaction appears once
 *   in a valid chain (BIP30, no double spend), and a coinbase that commits to
 *   its block's height (BIP34) differs from every coinbase of another height.
 *   Any second block at a height, re-org, mempool operation or second node
 *   ends the proof for the rest of the process.
 */
import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../../types/chaingraph.js';

import type { ClickHouseClient } from './client.js';
import { validCommitSql } from './utxo.js';

export interface StoredTransaction {
  internalId: bigint;
  seq: bigint;
}

interface Waiting {
  hashes: readonly string[];
  resolve: (found: Map<string, StoredTransaction>) => void;
  reject: (error: unknown) => void;
}

const compareText = (a: string, b: string) => (a < b ? -1 : Number(a > b));

/** Stored (valid-commit) transactions by hash, coalesced across callers. */
export class TransactionLookup {
  /** Queries issued and hashes looked up (tests and measurements). */
  readonly stats = { hashes: 0, queries: 0 };

  private readonly queue: Waiting[] = [];

  private queued = 0;

  private inFlight = 0;

  private timer: ReturnType<typeof setTimeout> | undefined;

  constructor(
    private readonly client: Pick<ClickHouseClient, 'query'>,
    private readonly fence: () => bigint[],
    private readonly options: {
      chunkSize: number;
      concurrency: number;
      /** How long a request waits for others while a query runs (default 5 ms). */
      gatherMs?: number;
    }
  ) {}

  /** Stored rows of `hashes` (absent: not stored); the first valid row per hash. */
  async lookup(
    hashes: readonly string[]
  ): Promise<Map<string, StoredTransaction>> {
    if (hashes.length === 0) return new Map();
    return new Promise((resolve, reject) => {
      this.queue.push({ hashes, reject, resolve });
      this.queued += hashes.length;
      this.pump();
    });
  }

  private pump() {
    while (this.inFlight < this.options.concurrency && this.queue.length > 0) {
      const gather = this.inFlight > 0 && this.queued < this.options.chunkSize;
      if (gather) {
        this.timer ??= setTimeout(() => {
          this.timer = undefined;
          this.run(this.take());
          this.pump();
        }, this.options.gatherMs ?? 5);
        return;
      }
      this.run(this.take());
    }
  }

  /** Take whole requests up to `chunkSize` hashes (at least one request). */
  private take(): Waiting[] {
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    const taken: Waiting[] = [];
    let count = 0;
    while (this.queue.length > 0) {
      const next = this.queue[0]!;
      if (
        taken.length > 0 &&
        count + next.hashes.length > this.options.chunkSize
      )
        break;
      this.queue.shift();
      taken.push(next);
      count += next.hashes.length;
    }
    this.queued -= count;
    return taken;
  }

  private run(requests: Waiting[]) {
    if (requests.length === 0) return;
    this.inFlight += 1;
    const hashes = [
      ...new Set(requests.flatMap((request) => request.hashes)),
    ].sort(compareText);
    this.query(hashes)
      .then(
        (found) => {
          requests.forEach((request) => {
            const mine = new Map<string, StoredTransaction>();
            request.hashes.forEach((hash) => {
              const row = found.get(hash);
              if (row !== undefined) mine.set(hash, row);
            });
            request.resolve(mine);
          });
        },
        (error: unknown) => {
          requests.forEach((request) => {
            request.reject(error);
          });
        }
      )
      .finally(() => {
        this.inFlight -= 1;
        this.pump();
      });
  }

  /** One sorted chunk query (a request larger than `chunkSize` is split). */
  private async query(
    hashes: readonly string[]
  ): Promise<Map<string, StoredTransaction>> {
    const found = new Map<string, StoredTransaction>();
    for (
      let start = 0;
      start < hashes.length;
      start += this.options.chunkSize
    ) {
      const chunk = hashes.slice(start, start + this.options.chunkSize);
      this.stats.queries += 1;
      this.stats.hashes += chunk.length;
      // eslint-disable-next-line no-await-in-loop
      const rows = await this.client.query<{
        hash_hex: string;
        internal_id: string;
        commit_seq: string;
      }>(
        `SELECT lower(hex(hash)) AS hash_hex, internal_id, commit_seq FROM transaction
         WHERE hash IN (SELECT toFixedString(unhex(h), 32) FROM (SELECT arrayJoin({hashes:Array(String)}) AS h))
           AND ${validCommitSql()}
         ORDER BY commit_seq`,
        { fence: this.fence(), hashes: chunk }
      );
      rows.forEach((row) => {
        if (!found.has(row.hash_hex)) {
          found.set(row.hash_hex, {
            internalId: BigInt(row.internal_id),
            seq: BigInt(row.commit_seq),
          });
        }
      });
    }
    return found;
  }
}

/** The stored chain at the start of an epoch, as `NewTransactionProof` needs it. */
export interface StoredChainFacts {
  /** Highest stored block height (-1: no block). */
  tipHeight: number;
  /** Its hash (all zeros when there is no block). */
  tipHash: string;
  /** Every stored height holds exactly one block. */
  oneBlockPerHeight: boolean;
  /** No committed reorg, mempool or expiry commit ever. */
  noReorgOrMempool: boolean;
  /** Registered nodes. */
  nodeCount: number;
}

/** Read `StoredChainFacts` (a few small aggregate queries). */
export const readStoredChainFacts = async (
  client: Pick<ClickHouseClient, 'query'>,
  fence: bigint[],
  nodeCount: number
): Promise<StoredChainFacts> => {
  const [[blocks], [commits]] = await Promise.all([
    client.query<{
      tip: string;
      tip_hash: string;
      blocks: string;
      heights: string;
    }>(
      `SELECT toString(if(count() = 0, -1, toInt64(max(height)))) AS tip,
         if(count() = 0, repeat('00', 32), lower(hex(argMax(hash, height)))) AS tip_hash,
         toString(count()) AS blocks, toString(uniqExact(height)) AS heights
       FROM block WHERE ${validCommitSql()}`,
      { fence }
    ),
    client.query<{ n: string }>(
      `SELECT toString(count()) AS n FROM commit_log FINAL
       WHERE state = 'committed' AND kind IN ('reorg', 'mempool_batch', 'expiry')`
    ),
  ]);
  return {
    noReorgOrMempool: commits?.n === '0',
    nodeCount,
    oneBlockPerHeight: blocks?.blocks === blocks?.heights,
    tipHash: blocks?.tip_hash ?? '00'.repeat(32),
    tipHeight: Number(blocks?.tip ?? '-1'),
  };
};

/** Bitcoin Core re-enables BIP30 here: earlier coinbases may commit to later heights. */
const bip34UniqueBelowHeight = 1_983_702;

/** The script number a coinbase's unlocking bytecode starts with (BIP34), if minimally pushed. */
export const coinbaseHeight = (
  unlockingBytecode: string
): number | undefined => {
  const length = Number.parseInt(unlockingBytecode.slice(0, 2), 16);
  if (!(length >= 1 && length <= 4)) return undefined;
  const hex = unlockingBytecode.slice(2, 2 + length * 2);
  if (hex.length !== length * 2) return undefined;
  const bytes = hex.match(/../gu)!.map((byte) => Number.parseInt(byte, 16));
  // positive, minimally encoded
  const last = bytes[bytes.length - 1]!;
  if (last & 0x80) return undefined;
  if (bytes.length > 1 && last === 0 && (bytes[bytes.length - 2]! & 0x80) === 0)
    return undefined;
  return bytes.reduceRight((value, byte) => value * 256 + byte, 0);
};

/** Whether `transaction` (of a block at `height`) can only be stored by that block. */
export const uniqueToItsBlock = (
  transaction: ChaingraphTransaction,
  height: number
) =>
  !transaction.isCoinbase ||
  (height < bip34UniqueBelowHeight &&
    coinbaseHeight(transaction.inputs[0]?.unlockingBytecode ?? '') === height);

/**
 * Which blocks of this process are provably new (see the module comment).
 * `note` is called for every block in call order (when its save is
 * appended); it returns whether the block's transactions need no lookup
 * (those that are `uniqueToItsBlock`).
 */
export class NewTransactionProof {
  private disabledReason: string | undefined;

  private readonly hashAtHeight = new Map<number, string>();

  private readonly proven = new Set<string>();

  /** Blocks proven new and transactions that skipped the lookup. */
  readonly stats = { blocks: 0, skipped: 0 };

  constructor(private readonly facts: StoredChainFacts | undefined) {
    if (facts === undefined) this.disabledReason = 'no facts';
    else if (!facts.oneBlockPerHeight)
      this.disabledReason = 'a stored height has two blocks';
    else if (!facts.noReorgOrMempool)
      this.disabledReason = 'a re-org or mempool commit is stored';
    else if (facts.nodeCount > 1) this.disabledReason = 'more than one node';
  }

  get enabled() {
    return this.disabledReason === undefined;
  }

  /** End the proof for this process (a re-org, a mempool operation, a second node). */
  disable(reason: string) {
    this.disabledReason ??= reason;
  }

  /** Record `block` (call order); whether it is proven new. */
  note(block: ChaingraphBlock): boolean {
    const known = this.hashAtHeight.get(block.height);
    if (known !== undefined && known !== block.hash) {
      this.disable(`two blocks at height ${block.height}`);
    }
    this.hashAtHeight.set(block.height, block.hash);
    if (this.disabledReason !== undefined || this.facts === undefined)
      return false;
    if (this.proven.has(block.hash)) return true;
    const extendsTip =
      block.height === this.facts.tipHeight + 1 &&
      block.previousBlockHash === this.facts.tipHash;
    const proven =
      block.height > this.facts.tipHeight &&
      (extendsTip || this.proven.has(block.previousBlockHash));
    if (proven) {
      this.proven.add(block.hash);
      this.stats.blocks += 1;
    }
    return proven;
  }
}
