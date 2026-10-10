/* eslint-disable @typescript-eslint/naming-convention, camelcase, functional/no-loop-statement, no-await-in-loop, functional/no-throw-statement, functional/no-try-statement, max-params */
// cspell:ignore clickhouse unhex seqs dedup
/**
 * Stand-in `input` rows and their resolution
 * (docs/clickhouse-port/mempool-fill-fix.md).
 *
 * `input` carries the attributes of the output it spends. A mempool
 * transaction saved while a spent output is unknown (an orphan released by
 * the grace period or by a full pool, wp5a-mempool.md §4) gets, for that
 * input, a row with a stand-in spent output (value 0, empty bytecode, no
 * token). `input` is an immutable MergeTree, so the real row is written
 * later as a second row; readers must see exactly one of the two:
 *
 * - The stand-in rows of one unknown parent transaction T of commit C (the
 *   child's mempool commit) are written under their own seq P, a
 *   `fill_pending` commit, never under C. `input_stand_in` lists P's inputs
 *   and its owner C.
 * - The commit R that first writes T's outputs (a block or a mempool save of
 *   T) also writes the real `input` rows of P's inputs and a row
 *   `input_stand_in_resolution (P, R)`. If T was stored first (restart, or a
 *   save that saw P before its owner settled), the owner C itself or a
 *   repair commit at startup does it.
 * - The snapshot (`visibility.ts` `snapshotSql` step 5) hides P while C is
 *   not visible and once R is visible. A reader therefore sees the stand-in
 *   exactly while C is visible and R is not, and the real row exactly when R
 *   is visible: never both, never neither, never the stand-in after R.
 *
 * Who resolves P (in memory, `StandInRegistry`): a group is `open` from the
 * synchronous decision in C's save (which also re-checks the output
 * registry, so a T registered before it is used directly) until C has
 * awaited its dependencies; then C either resolves it itself (a save of T
 * marked it `parentSeen` while it was open) or makes it `live`. A save of T
 * claims `live` groups (and depends on C if C is still open). Every step that
 * reads or changes a group's state is synchronous, so exactly one commit
 * resolves each P.
 */
import type { ChaingraphTransaction } from '../../types/chaingraph.js';

import type { ClickHouseClient } from './client.js';
import type { CommitLog, OpenCommit } from './commit-log.js';
import type { StoreOperation } from './node-state.js';
import { RowBinaryWriter } from './row-binary.js';
import type { ResolvedInput, SpentOutput } from './row-encoders.js';
import {
  encodeResolvedInputRows,
  rowBinaryTableColumns,
} from './row-encoders.js';
import type { UtxoOutput } from './utxo.js';
import { outpointKey, validCommitSql } from './utxo.js';

/** The stand-in spent output: what Postgres's join shows as no output. */
export const standInSpentOutput: SpentOutput = {
  lockingBytecode: '',
  valueSatoshis: 0n,
};

export const standInColumns = {
  input_stand_in: [
    'stand_in_seq',
    'owner_seq',
    'transaction_hash',
    'input_index',
    'outpoint_transaction_hash',
    'outpoint_index',
    'commit_seq',
  ],
  input_stand_in_resolution: ['stand_in_seq', 'commit_seq'],
} as const;

const inputColumns = rowBinaryTableColumns.input.map(([name]) => name);

/** RowBinary bytes of one `input_stand_in` row (3 × UInt64, 2 × UInt32, 2 × FixedString(32)). */
const memberRowBytes = 96;
/** RowBinary bytes of one `input_stand_in_resolution` row. */
const resolutionRowBytes = 16;

/** Row counts of each P (`openStandIns`), for its `committed` row. */
const standInRowCounts = new WeakMap<
  StandInGroup,
  { [table: string]: number }
>();

/** One input written with a stand-in spent output. */
export interface StandInMember {
  transactionHash: string;
  transactionInternalId: bigint;
  inputIndex: number;
  input: ChaingraphTransaction['inputs'][number];
}

export type StandInState = 'claimed' | 'live' | 'open';

/** The stand-in inputs of one owner commit spending one unknown parent. */
export interface StandInGroup {
  /** P, once its `fill_pending` commit is begun. */
  seq: bigint | undefined;
  /** C: the commit whose transaction the inputs belong to. */
  ownerSeq: bigint;
  /** C's operation while it is live (`undefined` once loaded from the store). */
  owner: StoreOperation | undefined;
  parentHash: string;
  members: StandInMember[];
  state: StandInState;
  /**
   * Set by a save of the parent that found the group `open`: the parent's
   * outputs (by index). The owner resolves the group itself.
   */
  parentOutputs: ReadonlyMap<number, SpentOutput> | undefined;
}

export const spentOutputOf = (output: UtxoOutput): SpentOutput => ({
  fungibleTokenAmount: output.fungibleTokenAmount,
  lockingBytecode: output.lockingBytecode,
  nonfungibleTokenCapability: output.nonfungibleTokenCapability,
  nonfungibleTokenCommitment: output.nonfungibleTokenCommitment,
  tokenCategory: output.tokenCategory,
  valueSatoshis: output.valueSatoshis,
});

const outputsOfTransaction = (transaction: ChaingraphTransaction) =>
  new Map<number, SpentOutput>(
    transaction.outputs.map((output, index) => [
      index,
      {
        fungibleTokenAmount: output.fungibleTokenAmount,
        lockingBytecode: output.lockingBytecode,
        nonfungibleTokenCapability: output.nonfungibleTokenCapability,
        nonfungibleTokenCommitment: output.nonfungibleTokenCommitment,
        tokenCategory: output.tokenCategory,
        valueSatoshis: output.valueSatoshis,
      },
    ])
  );

/** The writer's unresolved stand-in groups, by parent transaction. */
export class StandInRegistry {
  private readonly byParent = new Map<string, Set<StandInGroup>>();

  get size() {
    return [...this.byParent.values()].reduce(
      (total, groups) => total + groups.size,
      0
    );
  }

  /** Every unresolved group (tests, diagnostics). */
  all(): StandInGroup[] {
    return [...this.byParent.values()].flatMap((groups) => [...groups]);
  }

  add(group: StandInGroup) {
    const groups = this.byParent.get(group.parentHash) ?? new Set();
    groups.add(group);
    this.byParent.set(group.parentHash, groups);
  }

  remove(group: StandInGroup) {
    const groups = this.byParent.get(group.parentHash);
    if (groups === undefined) return;
    groups.delete(group);
    if (groups.size === 0) this.byParent.delete(group.parentHash);
  }

  /** Forget every group (a fault-injection hook of the specs). */
  clear() {
    this.byParent.clear();
  }

  /**
   * Synchronous: the groups a commit writing `transactions` (their outputs)
   * resolves. `live` groups are claimed and returned with the parent's
   * outputs; `open` groups are marked `parentSeen` (their owner resolves
   * them); `claimed` groups are left alone.
   */
  claim(transactions: readonly ChaingraphTransaction[]): {
    group: StandInGroup;
    outputs: ReadonlyMap<number, SpentOutput>;
  }[] {
    const claimed: {
      group: StandInGroup;
      outputs: ReadonlyMap<number, SpentOutput>;
    }[] = [];
    if (this.byParent.size === 0) return claimed;
    for (const transaction of transactions) {
      const groups = this.byParent.get(transaction.hash);
      if (groups !== undefined) {
        const outputs = outputsOfTransaction(transaction);
        groups.forEach((group) => {
          if (group.state === 'open') {
            group.parentOutputs = outputs;
          } else if (group.state === 'live') {
            group.state = 'claimed';
            claimed.push({ group, outputs });
          }
        });
      }
    }
    return claimed;
  }
}

/**
 * The real `input` rows of `group` from its parent's outputs, or `undefined`
 * if one of its outpoints is not among them (an invalid spend).
 */
export const resolvedRowsOf = (
  group: StandInGroup,
  outputs: ReadonlyMap<number, SpentOutput>
): ResolvedInput[] | undefined => {
  const rows: ResolvedInput[] = [];
  for (const member of group.members) {
    const spent = outputs.get(member.input.outpointIndex);
    if (spent === undefined) return undefined;
    rows.push({ ...member, spent });
  }
  return rows;
};

export interface StandInContext {
  client: ClickHouseClient;
  commitLog: CommitLog;
  standIns: StandInRegistry;
}

const insert = async (
  context: Pick<StandInContext, 'client'>,
  commit: OpenCommit,
  table: string,
  columns: readonly string[],
  encoded: { data: Uint8Array; rowCount: number },
  chunk: number | string,
  rowCounts: { [table: string]: number }
) => {
  if (encoded.rowCount === 0) return;
  await context.client.insertRowBinary(table, columns, encoded.data, {
    deduplicationToken: commit.token(table, chunk),
  });
  rowCounts[table] = (rowCounts[table] ?? 0) + encoded.rowCount;
};

const encodeMemberRows = (group: StandInGroup, commitSeq: bigint) => {
  const writer = new RowBinaryWriter(group.members.length * memberRowBytes);
  group.members.forEach((member) => {
    writer
      .uint64(group.seq!)
      .uint64(group.ownerSeq)
      .fixedString32(member.transactionHash)
      .uint32(member.inputIndex)
      .fixedString32(member.input.outpointTransactionHash)
      .uint32(member.input.outpointIndex)
      .uint64(commitSeq)
      .endRow();
  });
  return { data: writer.finish(), rowCount: writer.rowCount };
};

const encodeResolutionRows = (seqs: readonly bigint[], commitSeq: bigint) => {
  const writer = new RowBinaryWriter(seqs.length * resolutionRowBytes);
  seqs.forEach((seq) => {
    writer.uint64(seq).uint64(commitSeq).endRow();
  });
  return { data: writer.finish(), rowCount: writer.rowCount };
};

/**
 * Synchronous (call it in the same tick as the decision that `pending`
 * inputs are unknown): one `open` group per unknown parent of
 * `transaction`, registered.
 */
export const registerStandIns = (
  registry: StandInRegistry,
  transaction: ChaingraphTransaction,
  transactionInternalId: bigint,
  pending: readonly { inputIndex: number }[],
  owner: { seq: bigint; operation: StoreOperation }
): StandInGroup[] => {
  const byParent = new Map<string, StandInGroup>();
  pending.forEach(({ inputIndex }) => {
    const input = transaction.inputs[inputIndex]!;
    const parentHash = input.outpointTransactionHash;
    const group = byParent.get(parentHash) ?? {
      members: [],
      owner: owner.operation,
      ownerSeq: owner.seq,
      parentHash,
      parentOutputs: undefined,
      seq: undefined,
      state: 'open',
    };
    if (!byParent.has(parentHash)) {
      byParent.set(parentHash, group);
      registry.add(group);
    }
    group.members.push({
      input,
      inputIndex,
      transactionHash: transaction.hash,
      transactionInternalId,
    });
  });
  return [...byParent.values()];
};

/**
 * Begin one `fill_pending` commit P per group and write its stand-in `input`
 * rows and `input_stand_in` rows under P. P is committed by
 * `commitStandIns`, before its owner.
 */
export const openStandIns = async (
  context: StandInContext,
  groups: readonly StandInGroup[]
) => {
  for (const group of groups) {
    const commit = await context.commitLog.beginCommit({
      kind: 'fill_pending',
      nodeScope: [],
    });
    group.seq = commit.seq;
    const rowCounts: { [table: string]: number } = {};
    await Promise.all([
      insert(
        context,
        commit,
        'input',
        inputColumns,
        encodeResolvedInputRows(
          group.members.map((member) => ({
            ...member,
            spent: standInSpentOutput,
          })),
          commit.seq
        ),
        0,
        rowCounts
      ),
      insert(
        context,
        commit,
        'input_stand_in',
        standInColumns.input_stand_in,
        encodeMemberRows(group, commit.seq),
        0,
        rowCounts
      ),
    ]);
    standInRowCounts.set(group, rowCounts);
  }
};

/**
 * Synchronous, in the owner's save once it awaited its dependencies: groups
 * a save of their parent saw while `open` are returned (the owner resolves
 * them under its own seq, `writeResolutions`); the others become `live`.
 */
export const settleStandIns = (groups: readonly StandInGroup[]) =>
  groups.filter((group) => {
    if (group.parentOutputs !== undefined) {
      group.state = 'claimed';
      return true;
    }
    group.state = 'live';
    return false;
  });

/**
 * Write, under `commit`, the real `input` rows of `resolved` and one
 * `input_stand_in_resolution` row per group (dedup chunk `s<chunk>`).
 * Returns the groups whose rows were written (a group whose outpoint is
 * not among its parent's outputs is skipped).
 */
export const writeResolutions = async (
  context: Pick<StandInContext, 'client'>,
  commit: OpenCommit,
  resolved: readonly {
    group: StandInGroup;
    outputs: ReadonlyMap<number, SpentOutput>;
  }[],
  rowCounts: { [table: string]: number },
  chunk = '0'
): Promise<StandInGroup[]> => {
  const rows: ResolvedInput[] = [];
  const written: StandInGroup[] = [];
  resolved.forEach(({ group, outputs }) => {
    const groupRows = resolvedRowsOf(group, outputs);
    if (groupRows === undefined || group.seq === undefined) return;
    rows.push(...groupRows);
    written.push(group);
  });
  if (written.length === 0) return written;
  await Promise.all([
    insert(
      context,
      commit,
      'input',
      inputColumns,
      encodeResolvedInputRows(rows, commit.seq),
      `s${chunk}`,
      rowCounts
    ),
    insert(
      context,
      commit,
      'input_stand_in_resolution',
      standInColumns.input_stand_in_resolution,
      encodeResolutionRows(
        written.map((group) => group.seq!),
        commit.seq
      ),
      `s${chunk}`,
      rowCounts
    ),
  ]);
  return written;
};

/** Commit every group's P (before the owner commits). */
export const commitStandIns = async (
  context: StandInContext,
  groups: readonly StandInGroup[]
) => {
  for (const group of groups) {
    if (group.seq !== undefined) {
      await context.commitLog.markCommitted(
        group.seq,
        standInRowCounts.get(group) ?? {}
      );
    }
  }
};

/** The owner failed: abort every open P and forget the groups. */
export const abortStandIns = async (
  context: StandInContext,
  groups: readonly StandInGroup[],
  reason: string
) => {
  groups.forEach((group) => {
    context.standIns.remove(group);
  });
  for (const group of groups) {
    if (group.seq !== undefined) {
      await context.commitLog
        .abortIfOpen(group.seq, reason)
        .catch(() => undefined);
    }
  }
};

/**
 * A commit that claimed `groups` (`StandInRegistry.claim`): once it
 * commits, they are resolved (forgotten); if it fails, they are `live`
 * again (its resolution rows are void).
 */
export const followClaims = (
  registry: StandInRegistry,
  operation: StoreOperation,
  groups: readonly StandInGroup[]
) => {
  if (groups.length === 0) return;
  operation.committed.then(
    () => {
      groups.forEach((group) => {
        registry.remove(group);
      });
    },
    () => {
      groups.forEach((group) => {
        if (group.state === 'claimed') group.state = 'live';
      });
    }
  );
};

/**
 * The unresolved stand-in groups of the store (startup, no open commits):
 * P and its owner valid, no valid resolution. Owners are committed (recovery
 * aborted the rest), so the groups are `live`.
 */
export const loadStandIns = async (
  context: Pick<StandInContext, 'client'>,
  fence: bigint[]
): Promise<StandInGroup[]> => {
  const members = await context.client.query<{
    seq: string;
    owner: string;
    hash: string;
    input_index: number;
    outpoint_hash: string;
    outpoint_index: number;
  }>(
    `SELECT toString(stand_in_seq) AS seq, toString(owner_seq) AS owner, lower(hex(transaction_hash)) AS hash,
       input_index, lower(hex(outpoint_transaction_hash)) AS outpoint_hash, outpoint_index
     FROM input_stand_in
     WHERE ${validCommitSql('commit_seq')} AND ${validCommitSql('owner_seq')}
       AND stand_in_seq NOT IN (SELECT stand_in_seq FROM input_stand_in_resolution WHERE ${validCommitSql(
         'commit_seq'
       )})
     ORDER BY stand_in_seq, hash, input_index`,
    { fence }
  );
  if (members.length === 0) return [];
  const rows = await context.client.query<{
    hash: string;
    input_index: number;
    internal_id: string;
    sequence_number: number;
    unlocking_bytecode: string;
  }>(
    `SELECT lower(hex(transaction_hash)) AS hash, input_index, toString(transaction_internal_id) AS internal_id,
       sequence_number, lower(hex(unlocking_bytecode)) AS unlocking_bytecode
     FROM input
     WHERE transaction_hash IN (SELECT toFixedString(unhex(h), 32) FROM (SELECT arrayJoin({hashes:Array(String)}) AS h))
       AND has({seqs:Array(UInt64)}, commit_seq)`,
    {
      hashes: [...new Set(members.map((member) => member.hash))],
      seqs: [...new Set(members.map((member) => member.seq))].map(BigInt),
    }
  );
  const inputOf = new Map(
    rows.map((row) => [`${row.hash}:${Number(row.input_index)}`, row])
  );
  const groups = new Map<string, StandInGroup>();
  members.forEach((member) => {
    const row = inputOf.get(`${member.hash}:${Number(member.input_index)}`);
    if (row === undefined) return;
    const group = groups.get(member.seq) ?? {
      members: [],
      owner: undefined,
      ownerSeq: BigInt(member.owner),
      parentHash: member.outpoint_hash,
      parentOutputs: undefined,
      seq: BigInt(member.seq),
      state: 'live' as const,
    };
    groups.set(member.seq, group);
    group.members.push({
      input: {
        outpointIndex: Number(member.outpoint_index),
        outpointTransactionHash: member.outpoint_hash,
        sequenceNumber: Number(row.sequence_number),
        unlockingBytecode: row.unlocking_bytecode,
      },
      inputIndex: Number(member.input_index),
      transactionHash: member.hash,
      transactionInternalId: BigInt(row.internal_id),
    });
  });
  return [...groups.values()];
};

/**
 * Startup repair: resolve every loaded group whose parent outputs are
 * stored (`stored`: outpoint key → output) in ONE `fill_pending` commit (the
 * parent's commit is long committed, e.g. by an earlier epoch that stopped
 * before it could resolve them). Returns the groups left unresolved.
 */
export const repairStandIns = async (
  context: StandInContext,
  groups: readonly StandInGroup[],
  stored: ReadonlyMap<string, UtxoOutput>
): Promise<{ repaired: StandInGroup[]; left: StandInGroup[] }> => {
  const resolvable: {
    group: StandInGroup;
    outputs: ReadonlyMap<number, SpentOutput>;
  }[] = [];
  const left: StandInGroup[] = [];
  groups.forEach((group) => {
    const outputs = new Map<number, SpentOutput>();
    group.members.forEach((member) => {
      const output = stored.get(
        outpointKey(
          member.input.outpointTransactionHash,
          member.input.outpointIndex
        )
      );
      if (output !== undefined) {
        outputs.set(member.input.outpointIndex, spentOutputOf(output));
      }
    });
    if (resolvedRowsOf(group, outputs) === undefined) {
      left.push(group);
    } else {
      resolvable.push({ group, outputs });
    }
  });
  if (resolvable.length === 0) return { left, repaired: [] };
  const commit = await context.commitLog.beginCommit({
    kind: 'fill_pending',
    nodeScope: [],
  });
  try {
    const rowCounts: { [table: string]: number } = {};
    const repaired = await writeResolutions(
      context,
      commit,
      resolvable,
      rowCounts
    );
    await context.commitLog.markCommitted(commit.seq, rowCounts);
    return { left, repaired };
  } catch (error) {
    await context.commitLog
      .abortIfOpen(commit.seq, `stand-in repair failed: ${String(error)}`)
      .catch(() => undefined);
    throw error;
  }
};
