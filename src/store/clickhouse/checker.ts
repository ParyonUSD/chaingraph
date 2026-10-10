/* eslint-disable max-lines, camelcase, @typescript-eslint/naming-convention, complexity, max-params */
// cspell:ignore clickhouse unhex seqs milli noncoinbase dedup
/**
 * `StoreChecker` over the ClickHouse store (WP5b).
 *
 * Every read method takes ONE `readSnapshot` and then reads only the pinned
 * gated views of `ddl/050_views.sql` with that snapshot's parameters:
 * node-scoped facts through `<view>_at(node, visible, fence, void)`,
 * node-agnostic facts through `<view>_at(visible0, tail, fence, void)`
 * (`pinnedView`). All views of one call therefore see
 * the same set of commits, so no assertion can observe a torn save; rows of
 * uncommitted, aborted (void) or fenced commits are never seen.
 *
 * Node-agnostic methods take the snapshot of node 0. `validatingNodes` is the
 * one method that reads several nodes: it takes one snapshot per node and
 * reads each node's mempool through that node's own pinned view.
 *
 * Base tables are read or written only by `schemaReport` (system tables) and
 * the two fault-injection methods; see
 * docs/clickhouse-port/wp5b-checker-and-harness.md for what those do.
 *
 * Semantics mirror `src/store/postgres/checker.ts`: lower-case big-endian
 * hex, the same orderings, UTC `Date`s (read as epoch milliseconds, so no
 * time zone is involved), `null` for missing timestamps.
 */
import type { Input, Output, TransactionCommon } from '@bitauth/libauth';
import {
  bigIntToCompactSize,
  binToHex,
  encodeTransaction,
  flattenBinArray,
  hexToBin,
  numberToBinInt32LE,
  numberToBinUint32LE,
} from '@bitauth/libauth';

import type {
  AcceptedBlock,
  BlockHistoryEntry,
  BlockSelector,
  BlockValueAggregates,
  CheckerInput,
  CheckerOutpoint,
  CheckerOutput,
  MempoolEntry,
  SchemaReport,
  StoreChecker,
  TransactionHistoryEntry,
} from '../checker.js';

import type { ClickHouseClient, QueryParams } from './client.js';
import { quoteIdentifier } from './client.js';
import {
  gateSql,
  nodeAgnosticId,
  nodeViewParams,
  pinnedView,
  readSnapshot,
  snapshotParams,
} from './visibility.js';

const hashBytes = 32;
const zeroCategoryHex = '00'.repeat(hashBytes);
const byHex = (a: string, b: string) => (a < b ? -1 : Number(a > b));
const hashParam = (name: string) =>
  `toFixedString(unhex({${name}:String}), 32)`;
const hashListParam = (name: string) =>
  `arrayMap(h -> toFixedString(unhex(h), 32), {${name}:Array(String)})`;
const hexOf = (column: string) => `lower(hex(${column}))`;
/** Epoch milliseconds of a (Nullable) DateTime64 column. */
const msOf = (column: string) => `toUnixTimestamp64Milli(${column})`;
const dateOf = (ms: string | null) =>
  ms === null ? null : new Date(Number(ms));

/** One transaction as stored, ready for `encodeTransaction`. */
interface StoredTransaction {
  hash: string;
  version: number;
  locktime: number;
}

interface StoredInputRow {
  transaction_hash_hex: string;
  input_index: number;
  outpoint_transaction_hash: string;
  outpoint_index: number;
  sequence_number: number;
  unlocking_bytecode: string;
}

interface StoredOutputRow {
  transaction_hash_hex: string;
  output_index: number;
  value_satoshis: string;
  locking_bytecode: string;
  token_category: string;
  fungible_token_amount: string | null;
  nonfungible_token_capability:
    | CheckerOutput['nonfungibleTokenCapability']
    | null;
  nonfungible_token_commitment: string | null;
}

const toLibauthOutput = (row: StoredOutputRow): Output => {
  const base = {
    lockingBytecode: hexToBin(row.locking_bytecode),
    valueSatoshis: BigInt(row.value_satoshis),
  };
  if (row.token_category === zeroCategoryHex) {
    return base;
  }
  return {
    ...base,
    token: {
      amount: BigInt(row.fungible_token_amount ?? '0'),
      category: hexToBin(row.token_category),
      ...(row.nonfungible_token_commitment === null
        ? {}
        : {
            nft: {
              capability: row.nonfungible_token_capability ?? 'none',
              commitment: hexToBin(row.nonfungible_token_commitment),
            },
          }),
    },
  };
};

const toLibauthInput = (row: StoredInputRow): Input => ({
  outpointIndex: Number(row.outpoint_index),
  outpointTransactionHash: hexToBin(row.outpoint_transaction_hash),
  sequenceNumber: Number(row.sequence_number),
  unlockingBytecode: hexToBin(row.unlocking_bytecode),
});

const groupBy = <Row extends { transaction_hash_hex: string }>(rows: Row[]) =>
  rows.reduce((groups, row) => {
    const group = groups.get(row.transaction_hash_hex);
    if (group === undefined) {
      groups.set(row.transaction_hash_hex, [row]);
    } else {
      group.push(row);
    }
    return groups;
  }, new Map<string, Row[]>());

export class ClickHouseChecker implements StoreChecker {
  private readonly db: string;

  /** `false`: the store runs with `CHAINGRAPH_CLICKHOUSE_UTXO=off`. */
  private readonly utxoTables: boolean;

  constructor(
    // eslint-disable-next-line @typescript-eslint/parameter-properties
    private readonly client: ClickHouseClient,
    database: string,
    options: { utxo?: 'off' | 'on' } = {}
  ) {
    this.db = quoteIdentifier(database);
    this.utxoTables = options.utxo !== 'off';
  }

  /* ---------------------------------------------------------------- nodes */

  readonly nodeInternalId = async (node: string) => {
    const [row] = await this.client.query<{ internal_id: number }>(
      `SELECT internal_id FROM ${this.db}.node_v WHERE name = {node:String} ORDER BY internal_id LIMIT 1`,
      { node }
    );
    return row === undefined ? undefined : Number(row.internal_id);
  };

  readonly nodeNamesOrdered = async () =>
    (
      await this.client.query<{ internal_id: number; name: string }>(
        `SELECT name, internal_id FROM ${this.db}.node_v ORDER BY name`
      )
    ).map((row) => ({ internalId: Number(row.internal_id), name: row.name }));

  /* ----------------------------------------------- node-agnostic base facts */

  readonly transactionExists = async (hash: string) =>
    (await this.transactionRowCount(hash)) > 0;

  readonly transactionRowCount = async (hash: string) => {
    const params = await this.agnosticSnapshot();
    const [row] = await this.client.query<{ c: string }>(
      `SELECT count() AS c FROM ${this.view('transaction_at')}
       WHERE hash = ${hashParam('hash')}`,
      { ...params, hash }
    );
    return Number(row?.c ?? 0);
  };

  readonly encodedTransactionHex = async (hash: string) => {
    const params = await this.agnosticSnapshot();
    const [encoded] = await this.encodeTransactions(
      params,
      `SELECT ${hashParam('hash')}`,
      { hash }
    );
    return encoded === undefined ? undefined : binToHex(encoded);
  };

  readonly encodedBlockHeaderHex = async (by: BlockSelector) => {
    const params = await this.agnosticSnapshot();
    const block = await this.selectBlock(params, by);
    return block === undefined ? undefined : binToHex(this.encodeHeader(block));
  };

  readonly encodedBlockHex = async (by: BlockSelector) => {
    const params = await this.agnosticSnapshot();
    const block = await this.selectBlock(params, by);
    if (block === undefined) {
      return undefined;
    }
    const blockParams = { ...params, block: block.internal_id };
    const [count] = await this.client.query<{ c: string }>(
      `SELECT count() AS c FROM ${this.view('block_transaction_at')}
       WHERE block_internal_id = {block:UInt64}`,
      blockParams
    );
    const transactions = await this.encodeTransactions(
      blockParams,
      `SELECT transaction_hash FROM ${this.view('block_transaction_at')}
       WHERE block_internal_id = {block:UInt64}`,
      {},
      `SELECT transaction_hash, min(transaction_index) AS position
       FROM ${this.view('block_transaction_at')}
       WHERE block_internal_id = {block:UInt64}
       GROUP BY transaction_hash`
    );
    return binToHex(
      flattenBinArray([
        this.encodeHeader(block),
        bigIntToCompactSize(BigInt(count?.c ?? 0)),
        ...transactions,
      ])
    );
  };

  readonly allBlockHashes = async () => {
    const params = await this.agnosticSnapshot();
    return (
      await this.client.query<{ hash: string }>(
        `SELECT ${hexOf('hash')} AS hash FROM ${this.view(
          'block_at'
        )} ORDER BY hash`,
        params
      )
    ).map((row) => row.hash);
  };

  readonly blockTransactionCount = async (blockHash: string) => {
    const params = await this.agnosticSnapshot();
    const [row] = await this.client.query<{ c: string }>(
      `SELECT count() AS c FROM ${this.view('block_transaction_at')}
       WHERE block_internal_id IN (SELECT internal_id FROM ${this.view(
         'block_at'
       )} WHERE hash = ${hashParam('blockHash')})`,
      { ...params, blockHash }
    );
    return Number(row?.c ?? 0);
  };

  readonly blockTransactionAt = async (blockHash: string, index: number) => {
    const params = await this.agnosticSnapshot();
    const [row] = await this.client.query<{ hash_hex: string }>(
      `SELECT ${hexOf('block_transaction.transaction_hash')} AS hash_hex
       FROM ${this.view('block_transaction_at')} AS block_transaction
       WHERE block_transaction.block_internal_id IN (SELECT internal_id FROM ${this.view(
         'block_at'
       )} WHERE hash = ${hashParam('blockHash')})
         AND block_transaction.transaction_index = {index:UInt32}
         AND block_transaction.transaction_hash IN (SELECT hash FROM ${this.view(
           'transaction_at'
         )})
       ORDER BY hash_hex
       LIMIT 1`,
      { ...params, blockHash, index }
    );
    return row?.hash_hex;
  };

  /**
   * As the Postgres functions: over the distinct transactions linked to the
   * block; input value = value of the spent outputs (stored on `input`;
   * coinbase inputs are 0); fee = input − output of non-coinbase
   * transactions; generated = output − input.
   */
  readonly blockValueAggregates = async (by: BlockSelector) => {
    const params = await this.agnosticSnapshot();
    const block = await this.selectBlock(params, by);
    if (block === undefined) {
      return undefined;
    }
    const linked = `(SELECT transaction_hash FROM ${this.view(
      'block_transaction_at'
    )} WHERE block_internal_id = {block:UInt64})`;
    const [row] = await this.client.query<{
      input: string;
      noncoinbase_output: string;
      output: string;
    }>(
      `SELECT
         (SELECT toString(sum(output_value_satoshis)) FROM ${this.view(
           'transaction_at'
         )} WHERE hash IN ${linked}) AS output,
         (SELECT toString(sumIf(output_value_satoshis, NOT is_coinbase)) FROM ${this.view(
           'transaction_at'
         )} WHERE hash IN ${linked}) AS noncoinbase_output,
         (SELECT toString(sum(value_satoshis)) FROM ${this.view(
           'input_at'
         )} WHERE transaction_hash IN ${linked}
            AND transaction_hash IN (SELECT hash FROM ${this.view(
              'transaction_at'
            )} WHERE NOT is_coinbase)) AS input`,
      { ...params, block: block.internal_id }
    );
    const output = BigInt(row?.output ?? '0');
    const input = BigInt(row?.input ?? '0');
    const fee = input - BigInt(row?.noncoinbase_output ?? '0');
    return {
      fee,
      generated: output - input,
      input,
      output,
    } as BlockValueAggregates;
  };

  readonly outputsOfTx = async (hash: string) => {
    const params = await this.agnosticSnapshot();
    const rows = await this.client.query<StoredOutputRow>(
      `${this.outputSelect()} WHERE transaction_hash = ${hashParam('hash')}
       ORDER BY output_index`,
      { ...params, hash }
    );
    return rows.map<CheckerOutput>((row) => ({
      lockingBytecode: row.locking_bytecode,
      outputIndex: Number(row.output_index),
      valueSatoshis: BigInt(row.value_satoshis),
      ...(row.token_category === zeroCategoryHex
        ? {}
        : { tokenCategory: row.token_category }),
      ...(row.fungible_token_amount === null
        ? {}
        : { fungibleTokenAmount: BigInt(row.fungible_token_amount) }),
      ...(row.nonfungible_token_capability === null
        ? {}
        : { nonfungibleTokenCapability: row.nonfungible_token_capability }),
      ...(row.nonfungible_token_commitment === null
        ? {}
        : { nonfungibleTokenCommitment: row.nonfungible_token_commitment }),
    }));
  };

  readonly inputsOfTx = async (hash: string) => {
    const params = await this.agnosticSnapshot();
    const rows = await this.client.query<StoredInputRow>(
      `${this.inputSelect()} WHERE transaction_hash = ${hashParam('hash')}
       ORDER BY input_index`,
      { ...params, hash }
    );
    return rows.map<CheckerInput>((row) => ({
      inputIndex: Number(row.input_index),
      outpointIndex: Number(row.outpoint_index),
      outpointTransactionHash: row.outpoint_transaction_hash,
      sequenceNumber: Number(row.sequence_number),
      unlockingBytecode: row.unlocking_bytecode,
    }));
  };

  readonly inputsSpending = async (outpointHash: string, index: number) => {
    const params = await this.agnosticSnapshot();
    const rows = await this.client.query<{
      input_index: number;
      tx_hash: string;
    }>(
      `SELECT ${hexOf('transaction_hash')} AS tx_hash, input_index
       FROM ${this.view('input_at')}
       WHERE outpoint_transaction_hash = ${hashParam('outpointHash')}
         AND outpoint_index = {index:UInt32}
       ORDER BY tx_hash, input_index`,
      { ...params, index, outpointHash }
    );
    return rows.map((row) => ({
      inputIndex: Number(row.input_index),
      txHash: row.tx_hash,
    }));
  };

  /* ------------------------------------------------- per-node acceptance */

  readonly acceptedBlocks = async (
    node: string,
    filter: { height?: number } = {}
  ) => {
    const params = await this.nodeSnapshot(node);
    if (params === undefined) {
      return [];
    }
    const rows = await this.client.query<{
      accepted_ms: string | null;
      hash: string;
      height: number;
    }>(
      `SELECT ${hexOf('block_hash')} AS hash, height, ${msOf(
        'accepted_at'
      )} AS accepted_ms
       FROM ${this.view('node_block_at')}
       WHERE {height:Nullable(UInt32)} IS NULL OR height = {height:Nullable(UInt32)}
       ORDER BY height, hash`,
      { ...params, height: filter.height ?? null }
    );
    return rows.map<AcceptedBlock>((row) => ({
      acceptedAt: dateOf(row.accepted_ms),
      hash: row.hash,
      height: Number(row.height),
    }));
  };

  readonly acceptedBlockCount = async (node: string, hashes: string[]) => {
    const params = await this.nodeSnapshot(node);
    if (params === undefined) {
      return 0;
    }
    const [row] = await this.client.query<{ c: string }>(
      `SELECT count() AS c FROM ${this.view('node_block_at')}
       WHERE has(${hashListParam('hashes')}, block_hash)`,
      { ...params, hashes }
    );
    return Number(row?.c ?? 0);
  };

  readonly mempool = async (node: string) => {
    const params = await this.nodeSnapshot(node);
    if (params === undefined) {
      return [];
    }
    const rows = await this.client.query<{
      hash: string;
      validated_ms: string | null;
    }>(
      `SELECT ${hexOf('transaction_hash')} AS hash, ${msOf(
        'validated_at'
      )} AS validated_ms
       FROM ${this.view('node_transaction_at')}
       ORDER BY hash`,
      params
    );
    return rows.map<MempoolEntry>((row) => ({
      hash: row.hash,
      validatedAt: dateOf(row.validated_ms),
    }));
  };

  readonly mempoolMembership = async (node: string, hashes: string[]) => {
    const params = await this.nodeSnapshot(node);
    if (params === undefined) {
      return new Set<string>();
    }
    const rows = await this.client.query<{ hash: string }>(
      `SELECT ${hexOf('transaction_hash')} AS hash
       FROM ${this.view('node_transaction_at')}
       WHERE has(${hashListParam('hashes')}, transaction_hash)
       ORDER BY hash`,
      { ...params, hashes }
    );
    return new Set(rows.map((row) => row.hash));
  };

  /** One pinned snapshot per node; each node's mempool read through its own view. */
  readonly validatingNodes = async (hash: string) => {
    const nodes = await this.nodeNamesOrdered();
    const validating = await nodes.reduce<Promise<string[]>>(
      async (previous, { internalId, name }) => {
        const names = await previous;
        const snapshot = await readSnapshot(this.client, internalId);
        const [row] = await this.client.query<{ c: string }>(
          `SELECT count() AS c FROM ${this.view('node_transaction_at')}
           WHERE transaction_hash = ${hashParam('hash')}`,
          { ...snapshotParams(snapshot), hash }
        );
        return Number(row?.c ?? 0) > 0 ? [...names, name] : names;
      },
      Promise.resolve([])
    );
    return validating;
  };

  readonly transactionHistory = async (node: string, hashes?: string[]) => {
    const params = await this.nodeSnapshot(node);
    if (params === undefined) {
      return [];
    }
    const rows = await this.client.query<{
      hash: string;
      replaced_ms: string | null;
      validated_ms: string | null;
    }>(
      `SELECT ${hexOf('tx.hash')} AS hash,
              ${msOf('history.validated_at')} AS validated_ms,
              ${msOf('history.replaced_at')} AS replaced_ms
       FROM ${this.view('node_transaction_history_at')} AS history
       INNER JOIN ${this.view(
         'transaction_at'
       )} AS tx ON tx.internal_id = history.transaction_internal_id
       WHERE {filter:UInt8} = 0 OR has(${hashListParam('hashes')}, tx.hash)
       ORDER BY history.validated_at, history.replaced_at, hash`,
      { ...params, filter: hashes === undefined ? 0 : 1, hashes: hashes ?? [] }
    );
    return rows.map<TransactionHistoryEntry>((row) => ({
      hash: row.hash,
      replacedAt: dateOf(row.replaced_ms),
      validatedAt: dateOf(row.validated_ms),
    }));
  };

  readonly blockHistory = async (node: string) => {
    const params = await this.nodeSnapshot(node);
    if (params === undefined) {
      return [];
    }
    const rows = await this.client.query<{
      accepted_ms: string | null;
      hash: string;
      removed_ms: string;
    }>(
      `SELECT ${hexOf('block.hash')} AS hash,
              ${msOf('history.accepted_at')} AS accepted_ms,
              ${msOf('history.removed_at')} AS removed_ms
       FROM ${this.view('node_block_history_at')} AS history
       INNER JOIN ${this.view(
         'block_at'
       )} AS block ON block.internal_id = history.block_internal_id
       ORDER BY history.removed_at, hash`,
      params
    );
    return rows.map<BlockHistoryEntry>((row) => ({
      acceptedAt: dateOf(row.accepted_ms),
      hash: row.hash,
      removedAt: new Date(Number(row.removed_ms)),
    }));
  };

  /** ACC: in the node's mempool or in a block it accepts (`tx_acceptance`). */
  readonly txAccepted = async (node: string, hash: string) => {
    const params = await this.nodeSnapshot(node);
    if (params === undefined) {
      return false;
    }
    const [row] = await this.client.query<{ c: string }>(
      `SELECT count() AS c FROM ${this.view('tx_acceptance_at')}
       WHERE transaction_hash = ${hashParam('hash')}`,
      { ...params, hash }
    );
    return Number(row?.c ?? 0) > 0;
  };

  /**
   * The node's UTXO set: `utxo_by_script_at` when scoped by locking bytecode
   * (its key is the 25-byte prefix), otherwise `utxo_at` (keyed by category).
   * With utxo off (no stored UTXO tables) it is computed at query time, as
   * in Chaingraph v1: outputs of transactions accepted by the node, minus
   * outpoints spent by an input of a transaction accepted by the node.
   */
  readonly unspent = async (
    node: string,
    scope: { category?: string; lockingBytecode?: string }
  ) => {
    const params = await this.nodeSnapshot(node);
    if (params === undefined) {
      return [];
    }
    if (!this.utxoTables) {
      return this.unspentAtQueryTime(params, scope);
    }
    const byScript = scope.lockingBytecode !== undefined;
    const conditions = [
      ...(byScript
        ? [
            'locking_bytecode_prefix = substring(unhex({lockingBytecode:String}), 1, 25)',
            'locking_bytecode = unhex({lockingBytecode:String})',
          ]
        : []),
      ...(scope.category === undefined
        ? []
        : [`token_category = ${hashParam('category')}`]),
    ];
    const rows = await this.client.query<{
      output_index: number;
      transaction_hash_hex: string;
    }>(
      `SELECT ${hexOf('transaction_hash')} AS transaction_hash_hex, output_index
       FROM ${this.view(byScript ? 'utxo_by_script_at' : 'utxo_at')}
       ${conditions.length === 0 ? '' : `WHERE ${conditions.join(' AND ')}`}
       ORDER BY transaction_hash_hex, output_index`,
      {
        ...params,
        category: scope.category ?? '',
        lockingBytecode: scope.lockingBytecode ?? '',
      }
    );
    return rows.map<CheckerOutpoint>((row) => ({
      outputIndex: Number(row.output_index),
      transactionHash: row.transaction_hash_hex,
    }));
  };

  /* ------------------------------------------------- per-node invariants */

  readonly confirmedButInMempool = async (node: string) => {
    const params = await this.nodeSnapshot(node);
    if (params === undefined) {
      return [];
    }
    return (
      await this.client.query<{ hash: string }>(
        `SELECT DISTINCT ${hexOf('transaction_hash')} AS hash
         FROM ${this.view('node_transaction_at')}
         WHERE transaction_hash IN (SELECT transaction_hash FROM ${this.view(
           'tx_acceptance_at'
         )} WHERE block_internal_id != 0)
         ORDER BY hash`,
        params
      )
    ).map((row) => row.hash);
  };

  /**
   * Mempool transactions spending an output of a transaction this node
   * archived as replaced, and (transitively) their mempool descendants. The
   * closure is computed here over one snapshot's rows.
   */
  readonly orphanMempoolDescendants = async (node: string) => {
    const params = await this.nodeSnapshot(node);
    if (params === undefined) {
      return [];
    }
    const [replaced, edges] = await Promise.all([
      this.client.query<{ hash: string }>(
        `SELECT DISTINCT ${hexOf('tx.hash')} AS hash
         FROM ${this.view('node_transaction_history_at')} AS history
         INNER JOIN ${this.view(
           'transaction_at'
         )} AS tx ON tx.internal_id = history.transaction_internal_id
         WHERE history.replaced_at IS NOT NULL`,
        params
      ),
      this.client.query<{ child: string; parent: string }>(
        `SELECT DISTINCT ${hexOf('transaction_hash')} AS child,
                ${hexOf('outpoint_transaction_hash')} AS parent
         FROM ${this.view('input_at')}
         WHERE transaction_hash IN (SELECT transaction_hash FROM ${this.view(
           'node_transaction_at'
         )})`,
        params
      ),
    ]);
    const replacedHashes = new Set(replaced.map((row) => row.hash));
    const childrenOf = edges.reduce((children, { child, parent }) => {
      children.set(parent, [...(children.get(parent) ?? []), child]);
      return children;
    }, new Map<string, string[]>());
    const seeds = edges
      .filter(({ parent }) => replacedHashes.has(parent))
      .map(({ child }) => child);
    const orphans = new Set<string>();
    const visit = (hashes: readonly string[]): void => {
      const fresh = hashes.filter((hash) => !orphans.has(hash));
      fresh.forEach((hash) => orphans.add(hash));
      if (fresh.length > 0) {
        visit(fresh.flatMap((hash) => childrenOf.get(hash) ?? []));
      }
    };
    visit(seeds);
    return [...orphans].sort(byHex);
  };

  /* -------------------------------------------------------- fault injection */

  /**
   * Lose one block-transaction link, as Postgres's DELETE does: a synchronous
   * `ALTER TABLE block_transaction DELETE` mutation of that one row (any
   * commit). Voiding or downgrading the block's commit cannot express this:
   * a void hides every row of the commit (the block itself, every link, and
   * every node's acceptance of it), and `commit_log` keeps the highest-ranked
   * state, so a later `incomplete` row never overrides `committed`. The
   * agent's incomplete-block scan then sees a block whose visible links do
   * not add up to `transaction_count`, exactly as on Postgres.
   */
  readonly dropBlockTransactionLink = async (
    blockHash: string,
    index: number
  ) => {
    const blocks = await this.client.query<{ internal_id: string }>(
      `SELECT internal_id FROM ${this.db}.block WHERE hash = ${hashParam(
        'blockHash'
      )}`,
      { blockHash }
    );
    await blocks.reduce<Promise<void>>(
      async (previous, { internal_id }) =>
        previous.then(async () =>
          this.client.command(
            `ALTER TABLE ${this.db}.block_transaction
             DELETE WHERE block_internal_id = {block:UInt64}
                      AND transaction_index = {index:UInt32}`,
            { block: internal_id, index },
            { mutations_sync: '2' }
          )
        ),
      Promise.resolve()
    );
  };

  /**
   * Forget that `node` validated `hash`: append a −1 row cancelling the
   * node's live +1 row in `node_transaction` and in `tx_acceptance` (block 0
   * = mempool). Only the writer-lease holder may allocate commit seqs (a new
   * epoch would fence the running agent), so the −1 rows are written under
   * the commit_seq of the +1 row they cancel: that commit is committed and
   * published, so the cancellation is visible at once and, as a
   * VersionedCollapsingMergeTree pair (same version), collapses on merge.
   * Dedup tokens `seq:table:forget-…` never collide with the agent's
   * `seq:table:N`. The two inserts are not one commit: a reader between them
   * can see the node's mempool and acceptance disagree (fault injection only).
   * `utxo` is not touched (the e2e use only checks mempool membership).
   */
  readonly forgetNodeValidation = async (node: string, hash: string) => {
    const nodeId = await this.nodeInternalId(node);
    if (nodeId === undefined) {
      return;
    }
    const snapshot = await readSnapshot(this.client, nodeId);
    /** The node's live +1 row for `hash` (highest version), if any. */
    const liveRow = async (table: 'node_transaction' | 'tx_acceptance') => {
      const isAcceptance = table === 'tx_acceptance';
      const timeColumn = isAcceptance ? 'accepted_at' : 'validated_at';
      const [row] = await this.client.query<{
        commit_seq: string;
        height: number;
        time_ms: string | null;
        transaction_internal_id: string;
        version: string;
      }>(
        `SELECT commit_seq, height, time_ms, transaction_internal_id, version
         FROM (
           SELECT commit_seq, sign, version, transaction_internal_id,
                  ${isAcceptance ? 'height' : '0'} AS height,
                  ${msOf(timeColumn)} AS time_ms,
                  sum(sign) OVER () AS live
           FROM ${this.db}.${table}
           WHERE node_internal_id = {node:UInt32}
             AND transaction_hash = ${hashParam('hash')}
             ${isAcceptance ? 'AND block_internal_id = 0' : ''}
             AND ${gateSql.visibleAt('commit_seq', `${this.db}.commit_void`)}
         )
         WHERE live > 0 AND sign = 1
         ORDER BY version DESC
         LIMIT 1`,
        { ...nodeViewParams(snapshot), hash }
      );
      return row;
    };
    const cancel = async (table: 'node_transaction' | 'tx_acceptance') => {
      const row = await liveRow(table);
      if (row === undefined) {
        return;
      }
      const columns =
        table === 'tx_acceptance'
          ? `(transaction_hash, node_internal_id, block_internal_id, transaction_internal_id, height, accepted_at, sign, version, commit_seq)
             SELECT ${hashParam(
               'hash'
             )}, {node:UInt32}, 0, {tx:UInt64}, {height:UInt32},`
          : `(node_internal_id, transaction_internal_id, transaction_hash, validated_at, sign, version, commit_seq)
             SELECT {node:UInt32}, {tx:UInt64}, ${hashParam('hash')},`;
      await this.client.insertSelect(
        `INSERT INTO ${this.db}.${table} ${columns}
                if(isNull({timeMs:Nullable(Int64)}), NULL,
                   fromUnixTimestamp64Milli(assumeNotNull({timeMs:Nullable(Int64)}), 'UTC')),
                -1, {version:UInt64}, {seq:UInt64}`,
        {
          hash,
          height: row.height,
          node: nodeId,
          seq: row.commit_seq,
          timeMs: row.time_ms,
          tx: row.transaction_internal_id,
          version: row.version,
        },
        {
          deduplicationToken: `${row.commit_seq}:${table}:forget-${nodeId}-${row.version}-${hash}`,
        }
      );
    };
    await cancel('node_transaction');
    await cancel('tx_acceptance');
  };

  /**
   * `indexes`: every table (`table:<name>`), projection
   * (`projection:<table>.<name>`) and data-skipping index
   * (`index:<table>.<name>`) of the database, sorted. `triggers`: the
   * database's materialized views (ClickHouse's insert triggers); the store
   * uses none, so this must be empty.
   */
  readonly schemaReport = async (): Promise<SchemaReport> => {
    const database = this.db.slice(1, -1);
    const rows = await this.client.query<{ entry: string }>(
      `SELECT entry FROM (
         SELECT concat('table:', name) AS entry FROM system.tables
         WHERE database = {database:String} AND engine NOT IN ('View', 'MaterializedView')
         UNION ALL
         SELECT concat('projection:', table, '.', name) FROM system.projections
         WHERE database = {database:String}
         UNION ALL
         SELECT concat('index:', table, '.', name) FROM system.data_skipping_indices
         WHERE database = {database:String}
       ) ORDER BY entry`,
      { database }
    );
    const triggers = await this.client.query<{ name: string }>(
      `SELECT name FROM system.tables
       WHERE database = {database:String} AND engine = 'MaterializedView'
       ORDER BY name`,
      { database }
    );
    return {
      indexes: rows.map((row) => row.entry),
      triggers: triggers.reduce<SchemaReport['triggers']>(
        (all, row) => ({ ...all, [row.name]: 'enabled' }),
        {}
      ),
    };
  };

  /* -------------------------------------------------------------- helpers */

  /** `db.view(node, visible, fence, void)` or `db.view(visible0, tail, fence, void)`. */
  private view(name: string) {
    return pinnedView(name, this.db);
  }

  /**
   * `unspent` without the stored UTXO tables, through the pinned views of
   * one node snapshot: output_at ⋈ tx_acceptance_at, anti-joined with the
   * outpoints of input_at rows of accepted transactions.
   */
  private async unspentAtQueryTime(
    params: QueryParams,
    scope: { category?: string; lockingBytecode?: string }
  ) {
    const acceptedSql = `SELECT transaction_hash FROM ${this.view(
      'tx_acceptance_at'
    )}`;
    const conditions = [
      `transaction_hash IN (${acceptedSql})`,
      `(transaction_hash, output_index) NOT IN (
         SELECT outpoint_transaction_hash, outpoint_index FROM ${this.view(
           'input_at'
         )}
         WHERE transaction_hash IN (${acceptedSql}))`,
      ...(scope.lockingBytecode === undefined
        ? []
        : ['locking_bytecode = unhex({lockingBytecode:String})']),
      ...(scope.category === undefined
        ? []
        : [`token_category = ${hashParam('category')}`]),
    ];
    const rows = await this.client.query<{
      output_index: number;
      transaction_hash_hex: string;
    }>(
      `SELECT DISTINCT ${hexOf(
        'transaction_hash'
      )} AS transaction_hash_hex, output_index
       FROM ${this.view('output_at')}
       WHERE ${conditions.join(' AND ')}
       ORDER BY transaction_hash_hex, output_index`,
      {
        ...params,
        category: scope.category ?? '',
        lockingBytecode: scope.lockingBytecode ?? '',
      }
    );
    return rows.map<CheckerOutpoint>((row) => ({
      outputIndex: Number(row.output_index),
      transactionHash: row.transaction_hash_hex,
    }));
  }

  private async agnosticSnapshot(): Promise<QueryParams> {
    return snapshotParams(await readSnapshot(this.client, nodeAgnosticId));
  }

  /** The pinned parameters for `node`, or `undefined` for an unknown node. */
  private async nodeSnapshot(node: string): Promise<QueryParams | undefined> {
    const nodeId = await this.nodeInternalId(node);
    if (nodeId === undefined) {
      return undefined;
    }
    return snapshotParams(await readSnapshot(this.client, nodeId));
  }

  private async selectBlock(params: QueryParams, by: BlockSelector) {
    if (by.hash === undefined && by.height === undefined) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error('BlockSelector requires a hash or a height.');
    }
    const [row] = await this.client.query<{
      bits: number;
      internal_id: string;
      merkle_root: string;
      nonce: number;
      previous_block_hash: string;
      timestamp: number;
      version: number;
    }>(
      `SELECT internal_id, version, ${hexOf(
        'previous_block_hash'
      )} AS previous_block_hash,
              ${hexOf('merkle_root')} AS merkle_root, timestamp, bits, nonce
       FROM ${this.view('block_at')}
       WHERE ${
         by.hash === undefined
           ? 'height = {height:UInt32}'
           : `hash = ${hashParam('hash')}`
       }
       ORDER BY hash
       LIMIT 1`,
      { ...params, hash: by.hash ?? '', height: by.height ?? 0 }
    );
    return row;
  }

  // eslint-disable-next-line class-methods-use-this
  private encodeHeader(block: {
    bits: number;
    merkle_root: string;
    nonce: number;
    previous_block_hash: string;
    timestamp: number;
    version: number;
  }) {
    return flattenBinArray([
      numberToBinInt32LE(Number(block.version)),
      hexToBin(block.previous_block_hash).reverse(),
      hexToBin(block.merkle_root).reverse(),
      numberToBinUint32LE(Number(block.timestamp)),
      numberToBinUint32LE(Number(block.bits)),
      numberToBinUint32LE(Number(block.nonce)),
    ]);
  }

  private outputSelect() {
    return `SELECT ${hexOf(
      'transaction_hash'
    )} AS transaction_hash_hex, output_index,
              toString(value_satoshis) AS value_satoshis,
              ${hexOf('locking_bytecode')} AS locking_bytecode,
              ${hexOf('token_category')} AS token_category,
              if(isNull(fungible_token_amount), NULL, toString(fungible_token_amount)) AS fungible_token_amount,
              nonfungible_token_capability,
              if(isNull(nonfungible_token_commitment), NULL, ${hexOf(
                'assumeNotNull(nonfungible_token_commitment)'
              )}) AS nonfungible_token_commitment
       FROM ${this.view('output_at')}`;
  }

  private inputSelect() {
    return `SELECT ${hexOf(
      'transaction_hash'
    )} AS transaction_hash_hex, input_index,
              ${hexOf(
                'outpoint_transaction_hash'
              )} AS outpoint_transaction_hash,
              outpoint_index, sequence_number,
              ${hexOf('unlocking_bytecode')} AS unlocking_bytecode
       FROM ${this.view('input_at')}`;
  }

  /**
   * P2P encodings of the transactions whose hash is in `hashesSql`, in
   * `orderSql` order (`transaction_hash, position`) or hash order. Every
   * stored transaction row is encoded (duplicates included, as Postgres).
   */
  private async encodeTransactions(
    params: QueryParams,
    hashesSql: string,
    extraParams: QueryParams,
    orderSql?: string
  ): Promise<Uint8Array[]> {
    const allParams = { ...params, ...extraParams };
    const [transactions, inputs, outputs] = await Promise.all([
      this.client.query<StoredTransaction & { position: string }>(
        `SELECT ${hexOf(
          'tx.hash'
        )} AS hash, tx.version AS version, tx.locktime AS locktime,
                ${
                  orderSql === undefined ? '0' : 'ordering.position'
                } AS position
         FROM ${this.view('transaction_at')} AS tx
         ${
           orderSql === undefined
             ? ''
             : `INNER JOIN (${orderSql}) AS ordering ON ordering.transaction_hash = tx.hash`
         }
         WHERE tx.hash IN (${hashesSql})
         ORDER BY position, hash`,
        allParams
      ),
      this.client.query<StoredInputRow>(
        `${this.inputSelect()} WHERE transaction_hash IN (${hashesSql})
         ORDER BY transaction_hash_hex, input_index`,
        allParams
      ),
      this.client.query<StoredOutputRow>(
        `${this.outputSelect()} WHERE transaction_hash IN (${hashesSql})
         ORDER BY transaction_hash_hex, output_index`,
        allParams
      ),
    ]);
    const inputsByTx = groupBy(inputs);
    const outputsByTx = groupBy(outputs);
    return transactions.map((transaction) => {
      const libauthTransaction: TransactionCommon = {
        inputs: (inputsByTx.get(transaction.hash) ?? []).map(toLibauthInput),
        locktime: Number(transaction.locktime),
        outputs: (outputsByTx.get(transaction.hash) ?? []).map(toLibauthOutput),
        version: Number(transaction.version),
      };
      return encodeTransaction(libauthTransaction);
    });
  }
}

/** The ClickHouse checker for `client`'s database. */
export const createClickHouseChecker = (
  client: ClickHouseClient,
  database = client.database,
  options: { utxo?: 'off' | 'on' } = {}
): StoreChecker => new ClickHouseChecker(client, database, options);
