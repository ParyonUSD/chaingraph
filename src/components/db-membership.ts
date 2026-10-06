/* Sequential protocol and streaming generators intentionally mutate only local state. */
/* eslint-disable functional/no-loop-statement, functional/no-let, functional/no-try-statement, functional/no-throw-statement, no-await-in-loop, complexity, max-params, @typescript-eslint/no-loop-func, require-atomic-updates */
import type pg from 'pg';

export type OutputMembershipMode = 'baseline' | 'deferred' | 'incremental';

/** Retry the complete publication, including normalized writes, with a fresh client. */
export const runMembershipTransaction = async <Result>(
  connectionPool: Pick<pg.Pool, 'connect'>,
  mode: OutputMembershipMode,
  requestedNodes:
    | number[]
    | 'all'
    | ((client: pg.PoolClient) => Promise<number[]>),
  write: (client: pg.PoolClient, nodeIds: number[]) => Promise<Result>
): Promise<Result> => {
  const maximumAttempts = 4;
  for (let attempt = 0; attempt < maximumAttempts; attempt += 1) {
    const client = await connectionPool.connect();
    let discard = false;
    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED;');
      let discovered =
        typeof requestedNodes === 'function'
          ? await requestedNodes(client)
          : requestedNodes;
      if (mode === 'incremental' && discovered === 'all') {
        /*
         * Fence registration while taking the node universe snapshot. Ordinary
         * per-node publications remain compatible with this table lock.
         */
        await client.query('LOCK TABLE node IN SHARE MODE;');
        discovered = (
          await client.query<{ internalId: number }>(
            'SELECT internal_id AS "internalId" FROM node ORDER BY internal_id;'
          )
        ).rows.map((node) => node.internalId);
      }
      const nodeIds =
        discovered === 'all'
          ? []
          : [...new Set(discovered)].sort((a, b) => a - b);
      if (mode === 'incremental') {
        for (const nodeId of nodeIds) {
          await client.query(
            'SELECT output_membership.lock_node($1::integer);',
            [nodeId]
          );
        }
        await client.query(
          'SELECT output_membership.begin_membership_changes();'
        );
      } else if (mode === 'deferred') {
        await client.query(
          "SET LOCAL output_membership.collect_changes = 'deferred';"
        );
      }
      const result = await write(client, nodeIds);
      if (mode === 'incremental') {
        await client.query(
          'SELECT output_membership.finish_membership_changes($1::integer[]);',
          [nodeIds]
        );
      }
      await client.query('COMMIT;');
      return result;
    } catch (error) {
      try {
        await client.query('ROLLBACK;');
      } catch {
        discard = true;
      }
      const { code } = error as { code?: string };
      if (
        attempt + 1 === maximumAttempts ||
        (code !== '40001' && code !== '40P01')
      ) {
        throw error;
      }
    } finally {
      client.release(discard);
    }
    // Release the pool connection before bounded exponential backoff.
    const baseDelayMs = 10;
    const backoffMultiplier = 2;
    await new Promise<void>((resolve) => {
      setTimeout(resolve, baseDelayMs * backoffMultiplier ** attempt);
    });
  }
  throw new Error('Membership transaction exhausted retries.');
};

export type QueryParameter = Buffer | boolean | number | string | null;
const bytesPerUnit = 1_048_576;
export const sqlChunkTargetBytes = bytesPerUnit + bytesPerUnit;

/** Build only the next bounded VALUES list. A single large bytea stays a bound parameter. */
export const boundedValueRows = function* boundedValueRows(
  rows: Iterable<QueryParameter[]>,
  parameterOffset = 0,
  parameterTypes: ('bigint' | 'bytea')[] = [],
  targetBytes = sqlChunkTargetBytes
): Generator<{ values: string; parameters: QueryParameter[] }> {
  const maximumParameters = 60_000;
  const hexCharactersPerByte = 2;
  const estimatedPlaceholderBytes = 16;
  let parameters: QueryParameter[] = [];
  let tuples: string[] = [];
  let bytes = 0;
  for (const row of rows) {
    const rowBytes = row.reduce<number>(
      (total, value) =>
        total +
        (Buffer.isBuffer(value)
          ? value.length * hexCharactersPerByte
          : Buffer.byteLength(String(value ?? ''))),
      row.length * estimatedPlaceholderBytes
    );
    if (
      tuples.length > 0 &&
      (bytes + rowBytes > targetBytes ||
        parameters.length + row.length + parameterOffset > maximumParameters)
    ) {
      yield { parameters, values: tuples.join(',') };
      parameters = [];
      tuples = [];
      bytes = 0;
    }
    tuples.push(
      `(${row
        .map((_, index) => {
          const type = parameterTypes[index];
          return `$${parameterOffset + parameters.length + index + 1}${
            type === undefined ? '' : `::${type}`
          }`;
        })
        .join(',')})`
    );
    parameters.push(...row);
    bytes += rowBytes;
  }
  if (tuples.length > 0) yield { parameters, values: tuples.join(',') };
};
