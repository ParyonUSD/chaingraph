/* eslint-disable camelcase, @typescript-eslint/naming-convention, complexity, no-underscore-dangle */
// cspell:ignore clickhouse
/**
 * An independent evaluator of the v2 filters (specs only): the store's raw
 * rows, read by direct SQL on the convenience views (`*_v`), and every
 * filter operator evaluated in JavaScript with SQL's three-valued logic (as
 * Postgres, and so Chaingraph v1, evaluates it). The API's results must
 * equal it.
 */
import type { ClickHouseClient } from '../store/clickhouse/client.js';

import type {
  BigIntComparison,
  BlockFilter,
  BlockWindow,
  BooleanComparison,
  CapabilityComparison,
  HexComparison,
  InputFilter,
  IntComparison,
  OutputFilter,
  TransactionFilter,
} from './sql.js';

export interface OracleOutput {
  transaction_hash: string;
  output_index: number;
  transaction_internal_id: bigint;
  value_satoshis: bigint;
  locking_bytecode: string;
  token_category: string | null;
  fungible_token_amount: bigint | null;
  nonfungible_token_capability: string | null;
  nonfungible_token_commitment: string | null;
}

export interface OracleInput {
  transaction_hash: string;
  input_index: number;
  transaction_internal_id: bigint;
  outpoint_transaction_hash: string;
  outpoint_index: bigint;
  sequence_number: bigint;
}

export interface OracleTransaction {
  hash: string;
  internal_id: bigint;
  version: bigint;
  locktime: bigint;
  size_bytes: bigint;
  is_coinbase: boolean;
  input_count: bigint;
  output_count: bigint;
  output_value_satoshis: bigint;
}

export interface OracleBlock {
  internal_id: bigint;
  height: bigint;
  hash: string;
  timestamp: bigint;
  size_bytes: bigint;
  transaction_count: bigint;
}

/** The raw rows and one node's acceptance (its blocks with heights, its mempool). */
export interface Oracle {
  outputs: OracleOutput[];
  inputs: OracleInput[];
  transactions: Map<string, OracleTransaction>;
  blocks: Map<bigint, OracleBlock>;
  /** transaction hash → block internal ids the node accepts it in (0 = mempool). */
  acceptance: Map<string, bigint[]>;
  outputsByTransaction: Map<string, OracleOutput[]>;
  inputsByTransaction: Map<string, OracleInput[]>;
  outputByOutpoint: Map<string, OracleOutput>;
  inputsByOutpoint: Map<string, OracleInput[]>;
}

const big = (value: unknown) =>
  value === null || value === undefined ? null : BigInt(String(value));

/** Load the oracle of `node` by direct SQL (convenience views). */
export const loadOracle = async (
  client: ClickHouseClient,
  node: number
): Promise<Oracle> => {
  const zero = `toFixedString(unhex(repeat('00', 32)), 32)`;
  const outputs = (
    await client.query<{ [key: string]: number | string | null }>(
      `SELECT lower(hex(transaction_hash)) AS transaction_hash, output_index, toString(transaction_internal_id) AS transaction_internal_id,
        toString(value_satoshis) AS value_satoshis, lower(hex(locking_bytecode)) AS locking_bytecode,
        if(token_category = ${zero}, NULL, lower(hex(token_category))) AS token_category,
        toString(fungible_token_amount) AS fungible_token_amount,
        toString(nonfungible_token_capability) AS nonfungible_token_capability,
        lower(hex(nonfungible_token_commitment)) AS nonfungible_token_commitment
       FROM output_v`
    )
  ).map(
    (row): OracleOutput => ({
      fungible_token_amount: big(row.fungible_token_amount),
      locking_bytecode: String(row.locking_bytecode),
      nonfungible_token_capability:
        row.nonfungible_token_capability === null
          ? null
          : String(row.nonfungible_token_capability),
      nonfungible_token_commitment:
        row.nonfungible_token_commitment === null
          ? null
          : String(row.nonfungible_token_commitment),
      output_index: Number(row.output_index),
      token_category:
        row.token_category === null ? null : String(row.token_category),
      transaction_hash: String(row.transaction_hash),
      transaction_internal_id: BigInt(String(row.transaction_internal_id)),
      value_satoshis: BigInt(String(row.value_satoshis)),
    })
  );
  const inputs = (
    await client.query<{ [key: string]: number | string }>(
      `SELECT lower(hex(transaction_hash)) AS transaction_hash, input_index, toString(transaction_internal_id) AS transaction_internal_id,
        lower(hex(outpoint_transaction_hash)) AS outpoint_transaction_hash, toString(outpoint_index) AS outpoint_index,
        toString(sequence_number) AS sequence_number FROM input_v`
    )
  ).map(
    (row): OracleInput => ({
      input_index: Number(row.input_index),
      outpoint_index: BigInt(String(row.outpoint_index)),
      outpoint_transaction_hash: String(row.outpoint_transaction_hash),
      sequence_number: BigInt(String(row.sequence_number)),
      transaction_hash: String(row.transaction_hash),
      transaction_internal_id: BigInt(String(row.transaction_internal_id)),
    })
  );
  const transactions = new Map(
    (
      await client.query<{ [key: string]: boolean | number | string }>(
        `SELECT lower(hex(hash)) AS hash, toString(internal_id) AS internal_id, toString(version) AS version,
          toString(locktime) AS locktime, toString(size_bytes) AS size_bytes, is_coinbase,
          toString(input_count) AS input_count, toString(output_count) AS output_count,
          toString(output_value_satoshis) AS output_value_satoshis FROM transaction_v`
      )
    ).map((row): [string, OracleTransaction] => [
      String(row.hash),
      {
        hash: String(row.hash),
        input_count: BigInt(String(row.input_count)),
        internal_id: BigInt(String(row.internal_id)),
        is_coinbase: row.is_coinbase === true || row.is_coinbase === 1,
        locktime: BigInt(String(row.locktime)),
        output_count: BigInt(String(row.output_count)),
        output_value_satoshis: BigInt(String(row.output_value_satoshis)),
        size_bytes: BigInt(String(row.size_bytes)),
        version: BigInt(String(row.version)),
      },
    ])
  );
  const blocks = new Map(
    (
      await client.query<{ [key: string]: number | string }>(
        `SELECT toString(internal_id) AS internal_id, toString(height) AS height, lower(hex(hash)) AS hash,
          toString(timestamp) AS timestamp, toString(size_bytes) AS size_bytes,
          toString(transaction_count) AS transaction_count FROM block_v`
      )
    ).map((row): [bigint, OracleBlock] => [
      BigInt(String(row.internal_id)),
      {
        hash: String(row.hash),
        height: BigInt(String(row.height)),
        internal_id: BigInt(String(row.internal_id)),
        size_bytes: BigInt(String(row.size_bytes)),
        timestamp: BigInt(String(row.timestamp)),
        transaction_count: BigInt(String(row.transaction_count)),
      },
    ])
  );
  const acceptance = new Map<string, bigint[]>();
  (
    await client.query<{ hash: string; block: string }>(
      `SELECT lower(hex(transaction_hash)) AS hash, toString(block_internal_id) AS block FROM tx_acceptance_v(node = ${node})`
    )
  ).forEach((row) => {
    acceptance.set(row.hash, [
      ...(acceptance.get(row.hash) ?? []),
      BigInt(row.block),
    ]);
  });
  const group = <T>(rows: T[], key: (row: T) => string) => {
    const map = new Map<string, T[]>();
    rows.forEach((row) => {
      map.set(key(row), [...(map.get(key(row)) ?? []), row]);
    });
    return map;
  };
  return {
    acceptance,
    blocks,
    inputs,
    inputsByOutpoint: group(
      inputs,
      (input) => `${input.outpoint_transaction_hash}:${input.outpoint_index}`
    ),
    inputsByTransaction: group(inputs, (input) => input.transaction_hash),
    outputByOutpoint: new Map(
      outputs.map((output) => [
        `${output.transaction_hash}:${output.output_index}`,
        output,
      ])
    ),
    outputs,
    outputsByTransaction: group(outputs, (output) => output.transaction_hash),
    transactions,
  };
};

/* ------------------------------------------------------------------ */
/* three-valued logic                                                   */
/* ------------------------------------------------------------------ */

type Truth = boolean | null;

const and = (values: readonly Truth[]): Truth =>
  values.includes(false) ? false : values.includes(null) ? null : true;
const or = (values: readonly Truth[]): Truth =>
  values.includes(true) ? true : values.includes(null) ? null : false;
const not = (value: Truth): Truth => (value === null ? null : !value);

const given = <T>(value: T | null | undefined): value is T =>
  value !== undefined && value !== null;

const compareValues = <T extends bigint | string>(
  value: T | null,
  comparison: { [key: string]: unknown },
  convert: (raw: unknown) => T
): Truth[] => {
  const parts: Truth[] = [];
  const binary = (key: string, test: (a: T, b: T) => boolean) => {
    const raw = comparison[key];
    if (!given(raw)) return;
    parts.push(value === null ? null : test(value, convert(raw)));
  };
  binary('_eq', (a, b) => a === b);
  binary('_neq', (a, b) => a !== b);
  binary('_gt', (a, b) => a > b);
  binary('_gte', (a, b) => a >= b);
  binary('_lt', (a, b) => a < b);
  binary('_lte', (a, b) => a <= b);
  const list = (key: string, negate: boolean) => {
    const raw = comparison[key] as unknown[] | null | undefined;
    if (!given(raw)) return;
    if (raw.length === 0 && negate) return;
    const found =
      value === null
        ? null
        : raw.map(convert).some((candidate) => candidate === value);
    parts.push(negate ? not(found) : found);
  };
  list('_in', false);
  list('_nin', true);
  if (given(comparison._is_null)) {
    parts.push((value === null) === comparison._is_null);
  }
  return parts;
};

const numeric = (
  value: bigint | number | null,
  comparison: BigIntComparison | IntComparison | null | undefined
) =>
  given(comparison)
    ? compareValues(
        value === null ? null : BigInt(value),
        comparison as { [key: string]: unknown },
        (raw) => BigInt(raw as bigint)
      )
    : [];

const hex = (
  value: string | null,
  comparison: HexComparison | null | undefined
) => {
  if (!given(comparison)) return [];
  const parts = compareValues(
    value,
    comparison as { [key: string]: unknown },
    (raw) => String(raw).toLowerCase()
  );
  const prefix = (prefixValue: string) =>
    value === null ? null : value.startsWith(prefixValue.toLowerCase());
  if (given(comparison._prefix)) parts.push(prefix(comparison._prefix));
  if (given(comparison._prefix_in)) {
    parts.push(or(comparison._prefix_in.map(prefix)));
  }
  return parts;
};

const capability = (
  value: string | null,
  comparison: CapabilityComparison | null | undefined
) =>
  given(comparison)
    ? compareValues(value, comparison as { [key: string]: unknown }, (raw) =>
        String(raw)
      )
    : [];

const boolean = (
  value: boolean,
  comparison: BooleanComparison | null | undefined
): Truth[] => [
  ...(given(comparison?._eq) ? [value === comparison._eq] : []),
  ...(given(comparison?._neq) ? [value !== comparison._neq] : []),
];

/* ------------------------------------------------------------------ */
/* filters                                                              */
/* ------------------------------------------------------------------ */

export const accepted = (oracle: Oracle, hash: string) =>
  oracle.acceptance.has(hash);

export const evalOutput = (
  oracle: Oracle,
  output: OracleOutput,
  filter: OutputFilter | null | undefined
): Truth => {
  if (!given(filter)) return true;
  const parts: Truth[] = [
    ...hex(output.transaction_hash, filter.transaction_hash),
    ...numeric(output.transaction_internal_id, filter.transaction_internal_id),
    ...numeric(output.output_index, filter.output_index),
    ...numeric(output.value_satoshis, filter.value_satoshis),
    ...numeric(output.fungible_token_amount, filter.fungible_token_amount),
    ...hex(output.locking_bytecode, filter.locking_bytecode),
    ...hex(output.token_category, filter.token_category),
    ...capability(
      output.nonfungible_token_capability,
      filter.nonfungible_token_capability
    ),
    ...hex(
      output.nonfungible_token_commitment,
      filter.nonfungible_token_commitment
    ),
  ];
  if (given(filter.transaction)) {
    const transaction = oracle.transactions.get(output.transaction_hash);
    parts.push(
      transaction !== undefined &&
        // eslint-disable-next-line @typescript-eslint/no-use-before-define
        evalTransaction(oracle, transaction, filter.transaction) === true
    );
  }
  if (given(filter.spent_by)) {
    parts.push(
      (
        oracle.inputsByOutpoint.get(
          `${output.transaction_hash}:${output.output_index}`
        ) ?? []
      ).some(
        (input) =>
          accepted(oracle, input.transaction_hash) &&
          // eslint-disable-next-line @typescript-eslint/no-use-before-define
          evalInput(oracle, input, filter.spent_by) === true
      )
    );
  }
  if (given(filter._and)) {
    parts.push(...filter._and.map((f) => evalOutput(oracle, output, f)));
  }
  if (given(filter._or)) {
    parts.push(or(filter._or.map((f) => evalOutput(oracle, output, f))));
  }
  if (given(filter._not)) {
    parts.push(not(evalOutput(oracle, output, filter._not)));
  }
  return and(parts);
};

export const evalInput = (
  oracle: Oracle,
  input: OracleInput,
  filter: InputFilter | null | undefined
): Truth => {
  if (!given(filter)) return true;
  const parts: Truth[] = [
    ...hex(input.transaction_hash, filter.transaction_hash),
    ...numeric(input.transaction_internal_id, filter.transaction_internal_id),
    ...numeric(input.input_index, filter.input_index),
    ...hex(input.outpoint_transaction_hash, filter.outpoint_transaction_hash),
    ...numeric(input.outpoint_index, filter.outpoint_index),
    ...numeric(input.sequence_number, filter.sequence_number),
  ];
  if (given(filter.outpoint)) {
    const spent = oracle.outputByOutpoint.get(
      `${input.outpoint_transaction_hash}:${input.outpoint_index}`
    );
    parts.push(
      spent !== undefined && evalOutput(oracle, spent, filter.outpoint) === true
    );
  }
  if (given(filter.transaction)) {
    const transaction = oracle.transactions.get(input.transaction_hash);
    parts.push(
      transaction !== undefined &&
        // eslint-disable-next-line @typescript-eslint/no-use-before-define
        evalTransaction(oracle, transaction, filter.transaction) === true
    );
  }
  if (given(filter._and)) {
    parts.push(...filter._and.map((f) => evalInput(oracle, input, f)));
  }
  if (given(filter._or)) {
    parts.push(or(filter._or.map((f) => evalInput(oracle, input, f))));
  }
  if (given(filter._not)) {
    parts.push(not(evalInput(oracle, input, filter._not)));
  }
  return and(parts);
};

export const evalBlock = (
  oracle: Oracle,
  block: OracleBlock,
  filter: BlockFilter | null | undefined
): Truth => {
  if (!given(filter)) return true;
  const parts: Truth[] = [
    ...hex(block.hash, filter.hash),
    ...numeric(block.internal_id, filter.internal_id),
    ...numeric(block.height, filter.height),
    ...numeric(block.timestamp, filter.timestamp),
    ...numeric(block.size_bytes, filter.size_bytes),
    ...numeric(block.transaction_count, filter.transaction_count),
  ];
  if (given(filter._and)) {
    parts.push(...filter._and.map((f) => evalBlock(oracle, block, f)));
  }
  if (given(filter._or)) {
    parts.push(or(filter._or.map((f) => evalBlock(oracle, block, f))));
  }
  if (given(filter._not))
    parts.push(not(evalBlock(oracle, block, filter._not)));
  return and(parts);
};

const inWindow = (oracle: Oracle, hash: string, window: BlockWindow) =>
  (oracle.acceptance.get(hash) ?? []).some((blockId) => {
    if (blockId === 0n) return window.include_mempool !== false;
    const { height } = oracle.blocks.get(blockId)!;
    return (
      (!given(window.from_height) || height >= BigInt(window.from_height)) &&
      (!given(window.to_height) || height < BigInt(window.to_height))
    );
  });

export const evalTransaction = (
  oracle: Oracle,
  transaction: OracleTransaction,
  filter: TransactionFilter | null | undefined
): Truth => {
  if (!given(filter)) return true;
  const parts: Truth[] = [
    ...numeric(transaction.internal_id, filter.internal_id),
    ...hex(transaction.hash, filter.hash),
    ...numeric(transaction.version, filter.version),
    ...numeric(transaction.locktime, filter.locktime),
    ...numeric(transaction.size_bytes, filter.size_bytes),
    ...boolean(transaction.is_coinbase, filter.is_coinbase),
    ...numeric(transaction.input_count, filter.input_count),
    ...numeric(transaction.output_count, filter.output_count),
    ...numeric(transaction.output_value_satoshis, filter.output_value_satoshis),
  ];
  if (given(filter.has_output)) {
    parts.push(
      (oracle.outputsByTransaction.get(transaction.hash) ?? []).some(
        (output) => evalOutput(oracle, output, filter.has_output) === true
      )
    );
  }
  if (given(filter.has_input)) {
    parts.push(
      (oracle.inputsByTransaction.get(transaction.hash) ?? []).some(
        (input) => evalInput(oracle, input, filter.has_input) === true
      )
    );
  }
  if (given(filter.block_window)) {
    parts.push(inWindow(oracle, transaction.hash, filter.block_window));
  }
  if (given(filter.in_block)) {
    parts.push(
      (oracle.acceptance.get(transaction.hash) ?? []).some(
        (blockId) =>
          blockId !== 0n &&
          evalBlock(oracle, oracle.blocks.get(blockId)!, filter.in_block) ===
            true
      )
    );
  }
  if (given(filter.in_mempool)) {
    parts.push(
      (oracle.acceptance.get(transaction.hash) ?? []).includes(0n) ===
        filter.in_mempool
    );
  }
  if (given(filter._and)) {
    parts.push(
      ...filter._and.map((f) => evalTransaction(oracle, transaction, f))
    );
  }
  if (given(filter._or)) {
    parts.push(
      or(filter._or.map((f) => evalTransaction(oracle, transaction, f)))
    );
  }
  if (given(filter._not)) {
    parts.push(not(evalTransaction(oracle, transaction, filter._not)));
  }
  return and(parts);
};
