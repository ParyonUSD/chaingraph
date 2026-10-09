/* eslint-disable max-classes-per-file, @typescript-eslint/no-magic-numbers, max-params, @typescript-eslint/parameter-properties, @typescript-eslint/init-declarations, functional/no-let */
// cspell:ignore clickhouse dedup unhex
/**
 * The per-node UTXO set (plan §2.3, WP5a-core): the transition rules, the
 * row encoders for `utxo` / `utxo_by_script`, the server-side delta SQL, and
 * the in-memory output registry used to resolve spent outputs.
 *
 * Rules (docs/clickhouse-port/wp5a-core.md §3):
 * - `tx-accepted(n, t)` is "t has at least one live acceptance container for
 *   n" (an accepted block of n containing t, or n's mempool). Only the
 *   0 → ≥1 and ≥1 → 0 transitions of that count emit UTXO rows.
 * - On 0 → ≥1: `+1` per output of t, `−1` per outpoint t spends. On ≥1 → 0:
 *   the inverse. Every row of one outpoint is a byte-identical copy of the
 *   output facts (only `sign` differs) and `created_height = 0`, so
 *   `any()` in the views is exact.
 * - `version` is the row's own `commit_seq` (on every per-node collapsing
 *   table): VersionedCollapsingMergeTree then only collapses a +1/−1 pair
 *   written by ONE commit. A −1 that copied an older +1's version could be
 *   merged away together with that +1 before its own commit is committed,
 *   or after it was aborted, and the gate cannot bring deleted rows back
 *   (wp5a-core.md §2).
 * - So `sum(sign)` for (n, o) = acc(n, creator) − Σ acc(n, spender), which
 *   is 0 or 1 whenever "spender accepted ⇒ creator accepted" holds for the
 *   visible commits (guaranteed by commit dependencies, §4 of the doc).
 */
import type { ChaingraphOutput } from '../../types/chaingraph.js';

import { RowBinaryWriter } from './row-binary.js';
import { nonfungibleTokenCapabilityEnum8 } from './row-encoders.js';

/**
 * The facts of one output, as copied onto every `utxo` row for it.
 */
export interface UtxoOutput {
  transactionHash: string;
  outputIndex: number;
  transactionInternalId: bigint;
  valueSatoshis: bigint;
  lockingBytecode: string;
  tokenCategory?: string;
  fungibleTokenAmount?: bigint;
  nonfungibleTokenCapability?: ChaingraphOutput['nonfungibleTokenCapability'];
  nonfungibleTokenCommitment?: string;
}

export interface UtxoRow {
  nodeInternalId: number;
  sign: -1 | 1;
  output: UtxoOutput;
}

export type OutpointKey = string;

export const outpointKey = (
  transactionHash: string,
  outputIndex: number
): OutpointKey => `${transactionHash}:${outputIndex}`;

export const utxoFromChaingraphOutput = (
  transactionHash: string,
  outputIndex: number,
  transactionInternalId: bigint,
  output: ChaingraphOutput
): UtxoOutput => ({
  fungibleTokenAmount: output.fungibleTokenAmount,
  lockingBytecode: output.lockingBytecode,
  nonfungibleTokenCapability: output.nonfungibleTokenCapability,
  nonfungibleTokenCommitment: output.nonfungibleTokenCommitment,
  outputIndex,
  tokenCategory: output.tokenCategory,
  transactionHash,
  transactionInternalId,
  valueSatoshis: output.valueSatoshis,
});

/**
 * The UTXO transition of one (node, tx) pair when its acceptance changes from
 * `acceptedBefore` to `acceptedAfter`: `1` (became accepted), `-1` (stopped
 * being accepted) or `0`.
 */
export const acceptanceTransition = (
  acceptedBefore: boolean,
  acceptedAfter: boolean
): -1 | 0 | 1 => {
  if (acceptedBefore === acceptedAfter) {
    return 0;
  }
  return acceptedAfter ? 1 : -1;
};

/**
 * The rows for one transition of one transaction for one node: `delta` per
 * output, `-delta` per spent output (coinbase inputs spend nothing; pass only
 * real spent outputs).
 */
export const utxoRowsForTransition = ({
  nodeInternalId,
  delta,
  outputs,
  spentOutputs,
}: {
  nodeInternalId: number;
  delta: -1 | 0 | 1;
  outputs: readonly UtxoOutput[];
  spentOutputs: readonly UtxoOutput[];
}): UtxoRow[] => {
  if (delta === 0) {
    return [];
  }
  const inverse = delta === 1 ? -1 : 1;
  return [
    ...outputs.map(
      (output): UtxoRow => ({ nodeInternalId, output, sign: delta })
    ),
    ...spentOutputs.map(
      (output): UtxoRow => ({ nodeInternalId, output, sign: inverse })
    ),
  ];
};

/**
 * The pure acceptance state machine (plan §2.3 "exactly once per
 * transition"): per (node, tx) the number of live acceptance containers
 * (accepted blocks containing the tx, plus the mempool). The store derives the
 * same counts from `tx_acceptance` (and the mempool state); this class is the
 * reference model the unit tests check the sign rules against.
 */
export class AcceptanceCounter {
  private readonly containers = new Map<string, Set<string>>();

  /** Add (`+1`) or remove (`-1`) container `container` of (node, tx); returns the UTXO transition. */
  apply(
    nodeInternalId: number,
    transactionHash: string,
    container: string,
    sign: -1 | 1
  ): -1 | 0 | 1 {
    const key = `${nodeInternalId}|${transactionHash}`;
    const current = this.containers.get(key) ?? new Set<string>();
    const before = current.size > 0;
    if (sign === 1) {
      current.add(container);
    } else {
      current.delete(container);
    }
    this.containers.set(key, current);
    return acceptanceTransition(before, current.size > 0);
  }

  isAccepted(nodeInternalId: number, transactionHash: string) {
    return (
      (this.containers.get(`${nodeInternalId}|${transactionHash}`)?.size ?? 0) >
      0
    );
  }
}

/**
 * `[name, type]` of the inserted (non-MATERIALIZED) columns, DDL order.
 */
export const utxoColumns = [
  'node_internal_id',
  'token_category',
  'transaction_hash',
  'output_index',
  'transaction_internal_id',
  'created_height',
  'value_satoshis',
  'locking_bytecode',
  'fungible_token_amount',
  'nonfungible_token_capability',
  'nonfungible_token_commitment',
  'sign',
  'version',
  'commit_seq',
] as const;

export const utxoByScriptColumns = [
  'node_internal_id',
  'transaction_hash',
  'output_index',
  'transaction_internal_id',
  'created_height',
  'value_satoshis',
  'locking_bytecode',
  'token_category',
  'fungible_token_amount',
  'nonfungible_token_capability',
  'nonfungible_token_commitment',
  'sign',
  'version',
  'commit_seq',
] as const;

/** `utxo.created_height` is always 0 (wp5a-core.md decision (i)). */
export const utxoCreatedHeight = 0;

const hashBytes = 32;

const writeCategory = (writer: RowBinaryWriter, category?: string) =>
  category === undefined
    ? writer.zeros(hashBytes)
    : writer.fixedString32(category);

const writeTokenTail = (writer: RowBinaryWriter, output: UtxoOutput) =>
  writer
    .nullable(output.fungibleTokenAmount, (w, amount) => w.int64(amount))
    .nullable(output.nonfungibleTokenCapability, (w, capability) =>
      w.enum8(nonfungibleTokenCapabilityEnum8[capability])
    )
    .nullable(output.nonfungibleTokenCommitment, (w, commitment) =>
      w.hexBytes(commitment)
    );

/**
 * Encode rows for both `utxo` and `utxo_by_script` (every row goes to both,
 * in the same commit).
 */
export const encodeUtxoRows = (
  rows: readonly UtxoRow[],
  commitSeq: bigint
): { utxo: Buffer; utxoByScript: Buffer; rowCount: number } => {
  const utxo = new RowBinaryWriter(rows.length * 200);
  const byScript = new RowBinaryWriter(rows.length * 200);
  rows.forEach(({ nodeInternalId, output, sign }) => {
    utxo.uint32(nodeInternalId);
    writeCategory(utxo, output.tokenCategory)
      .fixedString32(output.transactionHash)
      .uint32(output.outputIndex)
      .uint64(output.transactionInternalId)
      .uint32(utxoCreatedHeight)
      .int64(output.valueSatoshis)
      .hexBytes(output.lockingBytecode);
    writeTokenTail(utxo, output)
      .int8Sign(sign)
      .uint64(commitSeq)
      .uint64(commitSeq)
      .endRow();

    byScript
      .uint32(nodeInternalId)
      .fixedString32(output.transactionHash)
      .uint32(output.outputIndex)
      .uint64(output.transactionInternalId)
      .uint32(utxoCreatedHeight)
      .int64(output.valueSatoshis)
      .hexBytes(output.lockingBytecode);
    writeCategory(byScript, output.tokenCategory);
    writeTokenTail(byScript, output)
      .int8Sign(sign)
      .uint64(commitSeq)
      .uint64(commitSeq)
      .endRow();
  });
  return {
    rowCount: rows.length,
    utxo: utxo.finish(),
    utxoByScript: byScript.finish(),
  };
};

/**
 * The visibility gate's "valid commit" test for writer-side reads of base
 * tables: not void (aborted) and not fenced. `{fence:Array(UInt64)}` is the
 * dense per-epoch fence array the writer loaded at init (only the lease
 * holder writes fences). Rows of this writer's open commits ARE included:
 * every reader of them takes a commit dependency (wp5a-core.md §4).
 */
export const validCommitSql = (column = 'commit_seq') =>
  `(${column} NOT IN (SELECT commit_seq FROM commit_void)
    AND (bitShiftRight(${column}, 40) > length({fence:Array(UInt64)})
         OR ${column} <= arrayElement({fence:Array(UInt64)}, bitShiftRight(${column}, 40))))`;

/**
 * As `validCommitSql`, and also not one of this writer's open commits
 * (`{open:Array(UInt64)}`): the latest committed state.
 */
export const committedSql = (column = 'commit_seq') =>
  `(${validCommitSql(column)} AND NOT has({open:Array(UInt64)}, ${column}))`;

const outputColumnsFrom = (alias: string) =>
  `${alias}.token_category, ${alias}.transaction_hash, ${alias}.output_index, ${alias}.transaction_internal_id,
   toUInt32(${utxoCreatedHeight}), ${alias}.value_satoshis, ${alias}.locking_bytecode, ${alias}.fungible_token_amount,
   ${alias}.nonfungible_token_capability, ${alias}.nonfungible_token_commitment`;

/**
 * `INSERT … SELECT` of UTXO rows for node `{node:UInt32}` under commit
 * `{seq:UInt64}`, for the transactions in the CTE `deltas` with columns
 * `transaction_hash` and `d` (d = +1 became accepted, −1 stopped being
 * accepted): `d` per output of
 * each transaction, `−d` per output it spends (joined from `output`, so the
 * rows are byte-identical to the ones the creating transition wrote).
 * `deltasSql` must be a `SELECT transaction_hash, d` (it may use `{node}`,
 * `{seq}`, `{fence}` and its own parameters).
 */
export const utxoDeltaInsertSql = (
  table: 'utxo_by_script' | 'utxo',
  deltasSql: string
) => {
  const columns = table === 'utxo' ? utxoColumns : utxoByScriptColumns;
  const select = (alias: string, signExpression: string) => {
    const facts = outputColumnsFrom(alias);
    return table === 'utxo'
      ? `{node:UInt32}, ${facts}, ${signExpression}, {seq:UInt64}, {seq:UInt64}`
      : `{node:UInt32}, ${alias}.transaction_hash, ${alias}.output_index, ${alias}.transaction_internal_id,
         toUInt32(${utxoCreatedHeight}), ${alias}.value_satoshis, ${alias}.locking_bytecode, ${alias}.token_category,
         ${alias}.fungible_token_amount, ${alias}.nonfungible_token_capability, ${alias}.nonfungible_token_commitment,
         ${signExpression}, {seq:UInt64}, {seq:UInt64}`;
  };
  return `INSERT INTO ${table} (${columns.join(', ')})
WITH
  deltas AS (${deltasSql}),
  spends AS (
    SELECT i.outpoint_transaction_hash AS spent_hash, i.outpoint_index AS spent_index, d.d AS d
    FROM input AS i INNER JOIN deltas AS d ON i.transaction_hash = d.transaction_hash
    WHERE i.transaction_hash IN (SELECT transaction_hash FROM deltas)
      AND ${validCommitSql('i.commit_seq')}
  )
SELECT ${select('o', 'toInt8(d.d)')}
FROM output AS o INNER JOIN deltas AS d ON o.transaction_hash = d.transaction_hash
WHERE o.transaction_hash IN (SELECT transaction_hash FROM deltas)
  AND ${validCommitSql('o.commit_seq')}
UNION ALL
SELECT ${select('o', 'toInt8(-s.d)')}
FROM output AS o INNER JOIN spends AS s
  ON o.transaction_hash = s.spent_hash AND o.output_index = s.spent_index
WHERE o.transaction_hash IN (SELECT spent_hash FROM spends)
  AND ${validCommitSql('o.commit_seq')}`;
};

/**
 * Half-open height batches `[from, to)` covering `[minHeight, maxHeight]`
 * (the horizon build issues one statement per batch per node).
 */
export const heightBatches = (
  minHeight: number,
  maxHeight: number,
  batchSize: number
): [from: number, to: number][] => {
  if (batchSize < 1) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new RangeError(`Invalid batch size ${batchSize}.`);
  }
  if (maxHeight < minHeight) {
    return [];
  }
  const batches: [number, number][] = [];
  // eslint-disable-next-line functional/no-loop-statement
  for (let from = minHeight; from <= maxHeight; from += batchSize) {
    batches.push([from, Math.min(from + batchSize, maxHeight + 1)]);
  }
  return batches;
};

/**
 * The deltas of the horizon build (plan §3.8) for one node and one height
 * batch: every (node, tx) whose acceptance rows were written by bulk-mode
 * commits (`commit_seq >= {bulkStart}`) gets `acc(now) − acc(before bulk)`.
 * A tx is assigned to the batch holding the lowest height among its bulk-mode
 * rows, so each (node, tx) is emitted exactly once across batches.
 * Parameters: `{node}`, `{seq}` (the build commit, excluded), `{bulkStart}`,
 * `{fromHeight}`, `{toHeight}`, `{fence}`.
 */
export const horizonDeltasSql = `
SELECT transaction_hash, toInt8(max(s_all > 0)) - toInt8(max(s_before > 0)) AS d
FROM (
  SELECT transaction_hash, block_internal_id,
    sum(sign) AS s_all,
    sumIf(sign, commit_seq < {bulkStart:UInt64}) AS s_before,
    minIf(height, commit_seq >= {bulkStart:UInt64}) AS bulk_height,
    countIf(commit_seq >= {bulkStart:UInt64}) AS bulk_rows
  FROM tx_acceptance
  WHERE node_internal_id = {node:UInt32}
    AND transaction_hash IN (
      SELECT transaction_hash FROM block_transaction
      WHERE block_internal_id IN (
        SELECT block_internal_id FROM node_block
        WHERE node_internal_id = {node:UInt32}
          AND commit_seq >= {bulkStart:UInt64} AND commit_seq != {seq:UInt64}
          AND height >= {fromHeight:UInt32} AND height < {toHeight:UInt32}
          AND ${validCommitSql()})
        AND ${validCommitSql()})
    AND commit_seq != {seq:UInt64}
    AND ${validCommitSql()}
  GROUP BY transaction_hash, block_internal_id
)
GROUP BY transaction_hash
HAVING d != 0
  AND minIf(bulk_height, bulk_rows > 0) >= {fromHeight:UInt32}
  AND minIf(bulk_height, bulk_rows > 0) < {toHeight:UInt32}`;

/**
 * An output the registry knows: its facts (the internal id may still be in
 * assignment, `internalId` resolves it) and, while the writing commit is
 * open, its owner.
 */
export interface RegisteredOutput<Owner> {
  output: Omit<UtxoOutput, 'transactionInternalId'>;
  internalId: Promise<bigint>;
  owner: Owner | undefined;
}

interface Waiter<Owner> {
  resolve: (entry: RegisteredOutput<Owner>) => void;
}

/**
 * Recent outputs (plan §3.5 "in-memory output cache, including in-flight
 * saves"): outputs of open commits are pinned until their commit ends;
 * outputs of committed commits stay in a bounded insertion-order cache.
 * Waiters (pending spends) are resolved as soon as any save registers the
 * outpoint.
 */
export class OutputRegistry<Owner> {
  private readonly pinned = new Map<OutpointKey, RegisteredOutput<Owner>[]>();

  private readonly recent = new Map<OutpointKey, RegisteredOutput<Owner>>();

  private readonly waiters = new Map<OutpointKey, Waiter<Owner>[]>();

  private readonly keysByOwner = new Map<Owner, OutpointKey[]>();

  constructor(private readonly recentCapacity = 500_000) {}

  get pinnedCount() {
    return this.pinned.size;
  }

  get recentCount() {
    return this.recent.size;
  }

  get waitingCount() {
    return [...this.waiters.values()].reduce(
      (total, list) => total + list.length,
      0
    );
  }

  /** Pin the outputs of `transactions` for `owner` and wake their waiters. */
  register(
    owner: Owner,
    transactions: readonly {
      hash: string;
      outputs: readonly ChaingraphOutput[];
      internalId: Promise<bigint>;
    }[]
  ) {
    const ownerKeys = this.keysByOwner.get(owner) ?? [];
    this.keysByOwner.set(owner, ownerKeys);
    transactions.forEach((transaction) => {
      transaction.outputs.forEach((output, outputIndex) => {
        const key = outpointKey(transaction.hash, outputIndex);
        ownerKeys.push(key);
        const entry: RegisteredOutput<Owner> = {
          internalId: transaction.internalId,
          output: {
            fungibleTokenAmount: output.fungibleTokenAmount,
            lockingBytecode: output.lockingBytecode,
            nonfungibleTokenCapability: output.nonfungibleTokenCapability,
            nonfungibleTokenCommitment: output.nonfungibleTokenCommitment,
            outputIndex,
            tokenCategory: output.tokenCategory,
            transactionHash: transaction.hash,
            valueSatoshis: output.valueSatoshis,
          },
          owner,
        };
        const list = this.pinned.get(key);
        if (list === undefined) {
          this.pinned.set(key, [entry]);
        } else {
          list.push(entry);
        }
        this.wake(key, entry);
      });
    });
  }

  /**
   * Unpin `owner`'s outputs; if it committed, keep them in the recent cache
   * (without an owner: they are durable).
   */
  release(owner: Owner, committed: boolean) {
    const keys = this.keysByOwner.get(owner) ?? [];
    this.keysByOwner.delete(owner);
    keys.forEach((key) => {
      const list = this.pinned.get(key) ?? [];
      const mine = list.filter((entry) => entry.owner === owner);
      if (mine.length === 0) {
        return;
      }
      const rest = list.filter((entry) => entry.owner !== owner);
      if (rest.length === 0) {
        this.pinned.delete(key);
      } else {
        this.pinned.set(key, rest);
      }
      if (committed) {
        this.remember(key, { ...mine[0]!, owner: undefined });
      }
    });
  }

  /** Cache durable outputs (e.g. read from the store). */
  remember(key: OutpointKey, entry: RegisteredOutput<Owner>) {
    this.recent.delete(key);
    this.recent.set(key, entry);
    // eslint-disable-next-line functional/no-loop-statement
    while (this.recent.size > this.recentCapacity) {
      const oldest = this.recent.keys().next().value as OutpointKey;
      this.recent.delete(oldest);
    }
  }

  lookup(key: OutpointKey): RegisteredOutput<Owner> | undefined {
    return this.pinned.get(key)?.[0] ?? this.recent.get(key);
  }

  /**
   * Resolve when some save registers `key`. Returns the promise and a
   * cancel function (call it on timeout).
   */
  waitFor(key: OutpointKey): {
    promise: Promise<RegisteredOutput<Owner>>;
    cancel: () => void;
  } {
    let waiter: Waiter<Owner> | undefined;
    const promise = new Promise<RegisteredOutput<Owner>>((resolve) => {
      waiter = { resolve };
    });
    const list = this.waiters.get(key) ?? [];
    list.push(waiter!);
    this.waiters.set(key, list);
    return {
      cancel: () => {
        const current = (this.waiters.get(key) ?? []).filter(
          (item) => item !== waiter
        );
        if (current.length === 0) {
          this.waiters.delete(key);
        } else {
          this.waiters.set(key, current);
        }
      },
      promise,
    };
  }

  private wake(key: OutpointKey, entry: RegisteredOutput<Owner>) {
    const list = this.waiters.get(key);
    if (list === undefined) {
      return;
    }
    this.waiters.delete(key);
    list.forEach((waiter) => {
      waiter.resolve(entry);
    });
  }
}
