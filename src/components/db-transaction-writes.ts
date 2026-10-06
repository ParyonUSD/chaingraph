/* eslint-disable functional/no-loop-statement, no-await-in-loop, no-continue, complexity */
import type pg from 'pg';

import type { ChaingraphTransaction } from '../types/chaingraph.js';

import { boundedValueRows, type QueryParameter } from './db-membership.js';

/** Stream inserts; never duplicate a whole block as SQL or flatten all its inputs/outputs. */
export const insertTransactions = async (
  client: pg.PoolClient,
  transactions: ChaingraphTransaction[],
  initialAcceptedNodeIds?: number[]
) => {
  /*
   * These IDs describe acceptance to be published by the same transaction;
   * finish_membership_changes must still recompute spends and other node support.
   */
  const acceptedNodeIds =
    initialAcceptedNodeIds === undefined
      ? undefined
      : [...new Set(initialAcceptedNodeIds)].sort((a, b) => a - b);
  const opReturn = 106;
  const newlySaved = new Map<string, string>();
  const transactionRows = function* transactionRows(): Generator<
    QueryParameter[]
  > {
    for (const transaction of transactions) {
      yield [
        Buffer.from(transaction.hash, 'hex'),
        transaction.version,
        transaction.locktime,
        transaction.sizeBytes,
        transaction.isCoinbase,
      ];
    }
  };
  for (const chunk of boundedValueRows(transactionRows())) {
    const result = await client.query<{ hash: Buffer; internalId: string }>(
      `INSERT INTO transaction (hash, version, locktime, size_bytes, is_coinbase)
       VALUES ${chunk.values} ON CONFLICT ON CONSTRAINT transaction_hash_key DO NOTHING
       RETURNING hash, internal_id AS "internalId";`,
      chunk.parameters
    );
    result.rows.forEach((row) =>
      newlySaved.set(row.hash.toString('hex'), row.internalId)
    );
  }
  const outputRows = function* outputRows(): Generator<QueryParameter[]> {
    for (const transaction of transactions) {
      if (!newlySaved.has(transaction.hash)) continue;
      for (const [index, output] of transaction.outputs.entries()) {
        const lockingBytecode = Buffer.from(output.lockingBytecode, 'hex');
        const row: QueryParameter[] = [
          Buffer.from(transaction.hash, 'hex'),
          index,
          output.valueSatoshis.toString(),
          lockingBytecode,
          output.tokenCategory === undefined
            ? null
            : Buffer.from(output.tokenCategory, 'hex'),
          output.fungibleTokenAmount?.toString() ?? null,
          output.nonfungibleTokenCapability ?? null,
          output.nonfungibleTokenCommitment === undefined
            ? null
            : Buffer.from(output.nonfungibleTokenCommitment, 'hex'),
        ];
        if (acceptedNodeIds !== undefined) {
          row.push(
            acceptedNodeIds,
            lockingBytecode[0] === opReturn ? [] : acceptedNodeIds
          );
        }
        yield row;
      }
    }
  };
  for (const chunk of boundedValueRows(outputRows())) {
    await client.query(
      `INSERT INTO output (transaction_hash, output_index, value_satoshis,
      locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability,
      nonfungible_token_commitment${
        acceptedNodeIds === undefined
          ? ''
          : ', accepted_node_ids, unspent_node_ids'
      }) VALUES ${chunk.values};`,
      chunk.parameters
    );
  }
  const inputRows = function* inputRows(): Generator<QueryParameter[]> {
    for (const transaction of transactions) {
      const internalId = newlySaved.get(transaction.hash);
      if (internalId === undefined) continue;
      for (const [index, input] of transaction.inputs.entries()) {
        yield [
          internalId,
          index,
          input.outpointIndex,
          input.sequenceNumber,
          Buffer.from(input.outpointTransactionHash, 'hex'),
          Buffer.from(input.unlockingBytecode, 'hex'),
        ];
      }
    }
  };
  for (const chunk of boundedValueRows(inputRows())) {
    await client.query(
      `INSERT INTO input (transaction_internal_id, input_index, outpoint_index,
      sequence_number, outpoint_transaction_hash, unlocking_bytecode) VALUES ${chunk.values};`,
      chunk.parameters
    );
  }
  return newlySaved;
};
