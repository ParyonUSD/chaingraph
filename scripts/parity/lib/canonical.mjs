// cspell:ignore bytewise clickhouse nullness Milli unhex prefilter denormalised selftest orged unnest
/**
 * Canonical row definitions of the parity harness (docs/clickhouse-port/parity-harness.md §2).
 *
 * Every table yields, on BOTH engines, one text column `s` per row: the fields
 * below joined with `|`, built from the same facts with the same formatting:
 *
 * - internal ids never appear (they differ between stores); rows are joined
 *   on hashes / indices and nodes are matched by name;
 * - bytea / FixedString / String as lowercase hex (`''` for empty bytes);
 * - integers as decimal; block/transaction `version` as signed int32;
 * - booleans `1`/`0`; NULL as `NULL`; "no token" is `NULL` on both sides
 *   (Postgres NULL, ClickHouse 32 zero bytes);
 * - timestamps: `--timestamps exact` appends ISO-8601 UTC with milliseconds
 *   (`2026-10-09T12:34:56.789Z`); otherwise only their NULL-ness (`null`/`set`)
 *   is in `s` and the values are compared separately with a tolerance
 *   (columns listed in `ts`).
 *
 * A table definition returns, per engine and chunk, `{ sql, ts }`: a SELECT
 * producing `s` (and one millisecond column per `ts` entry, `t0`, `t1`, …).
 */

const zeroHashHex = '0'.repeat(64);

/** Postgres expression builders. */
export const pgExpr = {
  bool: (column) => `CASE WHEN ${column} THEN '1' ELSE '0' END`,
  capability: (column) => `coalesce(${column}::text, 'NULL')`,
  concat: (fields) => `concat_ws('|', ${fields.join(', ')})`,
  hex: (column) => `encode(${column}, 'hex')`,
  int: (column) => `${column}::text`,
  int32: (column) =>
    `((((${column}) % 4294967296 + 4294967296 + 2147483648) % 4294967296) - 2147483648)::text`,
  iso: (column) =>
    `coalesce(to_char(date_trunc('milliseconds', ${column}), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'), 'NULL')`,
  milliseconds: (column) =>
    `floor(extract(epoch FROM ${column}) * 1000)::bigint`,
  nullableHex: (column) => `coalesce(encode(${column}, 'hex'), 'NULL')`,
  nullableInt: (column) => `coalesce(${column}::text, 'NULL')`,
  nullness: (column) =>
    `CASE WHEN ${column} IS NULL THEN 'null' ELSE 'set' END`,
  string: (literal) => `'${literal}'`,
  tokenCategory: (column) => `coalesce(encode(${column}, 'hex'), 'NULL')`,
};

/** ClickHouse expression builders (same output text as pgExpr). */
export const chExpr = {
  bool: (column) => `if(${column}, '1', '0')`,
  capability: (column) => `ifNull(toString(${column}), 'NULL')`,
  concat: (fields) => `concat(${fields.join(", '|', ")})`,
  hex: (column) => `lower(hex(${column}))`,
  int: (column) => `toString(${column})`,
  int32: (column) => `toString(toInt32(${column}))`,
  iso: (column) =>
    `ifNull(concat(replaceOne(toString(${column}), ' ', 'T'), 'Z'), 'NULL')`,
  milliseconds: (column) => `toUnixTimestamp64Milli(${column})`,
  nullableHex: (column) => `ifNull(lower(hex(${column})), 'NULL')`,
  nullableInt: (column) => `ifNull(toString(${column}), 'NULL')`,
  nullness: (column) => `if(isNull(${column}), 'null', 'set')`,
  string: (literal) => `'${literal}'`,
  tokenCategory: (column) =>
    `if(${column} = unhex('${zeroHashHex}'), 'NULL', lower(hex(${column})))`,
};

/** Output facts (output, utxo, F1g): same field list on both engines. */
const outputFields = (
  expr,
  alias,
  hashColumn = `${alias}.transaction_hash`
) => [
  expr.hex(hashColumn),
  expr.int(`${alias}.output_index`),
  expr.int(`${alias}.value_satoshis`),
  expr.hex(`${alias}.locking_bytecode`),
  expr.tokenCategory(`${alias}.token_category`),
  expr.nullableInt(`${alias}.fungible_token_amount`),
  expr.capability(`${alias}.nonfungible_token_capability`),
  expr.nullableHex(`${alias}.nonfungible_token_commitment`),
];

/**
 * Build `SELECT s, t0, t1 … FROM <from>`: in `exact` mode the timestamps are
 * appended to `s` as ISO strings; in `tolerance` mode they are returned as
 * millisecond columns for the separate pass; in `exclude` mode they are dropped.
 */
const select = (expr, ctx, fields, timestampColumns, from) => {
  const exact = ctx.timestamps === 'exact';
  const all = exact
    ? [...fields, ...timestampColumns.map((column) => expr.iso(column))]
    : fields;
  const extra =
    ctx.timestamps === 'tolerance'
      ? timestampColumns
          .map((column, index) => `, ${expr.milliseconds(column)} AS t${index}`)
          .join('')
      : '';
  return `SELECT ${expr.concat(all)} AS s${extra} ${from}`;
};

// ---------------------------------------------------------------- bounds

/** `column >= lo AND column < hi` for a hash-prefix chunk (hi absent on the last chunk). */
const pgHashRange = (column, chunk) =>
  `${column} >= decode('${chunk.lo}', 'hex')` +
  (chunk.hi === undefined
    ? ''
    : ` AND ${column} < decode('${chunk.hi}', 'hex')`);
const chHashRange = (column, chunk) =>
  `${column} >= unhex('${chunk.lo}')` +
  (chunk.hi === undefined ? '' : ` AND ${column} < unhex('${chunk.hi}')`);

/** ClickHouse pinned-view arguments (readClickHouseSnapshot; contract in ddl/050_views.sql). */
const gateArgs = (ctx) =>
  `fence = [${ctx.ch.fence.join(', ')}], void = [${ctx.ch.void.join(', ')}]`;
const agnostic = (ctx) =>
  `(visible0 = ${ctx.ch.visible0}, tail = [${ctx.ch.tail.join(
    ', '
  )}], ${gateArgs(ctx)})`;
export const nodeArgs = (node, ctx) =>
  `(node = ${node.chId}, visible = ${node.visible}, ${gateArgs(ctx)})`;

export const agnosticArgs = agnostic;

const sqlString = (value) => `'${String(value).replace(/'/g, "''")}'`;

// ---------------------------------------------------------------- transaction scope

/**
 * The transactions whose base rows a tx-level chunk covers (both engines):
 * - hash chunk: tx hash in [lo, hi) and contained in at least one stored block
 *   (of any node or none) with height <= H (no height limit when H is unset);
 *   with --include-mempool, also txs in a compared node's mempool;
 * - window chunk: txs contained in a stored block with height in [lo, hi].
 */
const pgScope = (ctx, chunk) => {
  if (chunk.kind === 'window') {
    return `scope AS (SELECT DISTINCT t.internal_id, t.hash FROM block b
      JOIN block_transaction bt ON bt.block_internal_id = b.internal_id
      JOIN transaction t ON t.internal_id = bt.transaction_internal_id
      WHERE b.height BETWEEN ${chunk.lo} AND ${chunk.hi})`;
  }
  const heightLimit =
    ctx.atHeight === undefined ? '' : ` AND b.height <= ${ctx.atHeight}`;
  const mempool = ctx.includeMempool
    ? ` OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = t.internal_id
          AND nt.node_internal_id IN (${ctx.nodes
            .map((node) => node.pgId)
            .join(', ')}))`
    : '';
  return `scope AS (SELECT t.internal_id, t.hash FROM transaction t
    WHERE ${pgHashRange('t.hash', chunk)}
      AND (EXISTS (SELECT 1 FROM block_transaction bt JOIN block b ON b.internal_id = bt.block_internal_id
                   WHERE bt.transaction_internal_id = t.internal_id${heightLimit})${mempool}))`;
};

const chScope = (ctx, chunk) => {
  if (chunk.kind === 'window') {
    return `scope AS (SELECT DISTINCT transaction_hash AS hash FROM block_transaction_at${agnostic(
      ctx
    )}
      WHERE block_internal_id IN (SELECT internal_id FROM block_at${agnostic(
        ctx
      )}
                                  WHERE height BETWEEN ${chunk.lo} AND ${
      chunk.hi
    }))`;
  }
  const heightLimit =
    ctx.atHeight === undefined
      ? ''
      : ` AND block_internal_id IN (SELECT internal_id FROM block_at${agnostic(
          ctx
        )} WHERE height <= ${ctx.atHeight})`;
  const mempool = ctx.includeMempool
    ? ctx.nodes
        .map(
          (
            node
          ) => ` UNION DISTINCT SELECT transaction_hash AS hash FROM node_transaction_at${nodeArgs(node, ctx)}
          WHERE ${chHashRange('transaction_hash', chunk)}`
        )
        .join('')
    : '';
  return `scope AS (SELECT DISTINCT transaction_hash AS hash FROM block_transaction_at${agnostic(
    ctx
  )}
    WHERE ${chHashRange('transaction_hash', chunk)}${heightLimit}${mempool})`;
};

/** Hash-range prefilter on the ClickHouse side (primary-key pruning); none for windows. */
const chRangeAnd = (column, chunk) =>
  chunk.kind === 'hash' ? `${chHashRange(column, chunk)} AND ` : '';

/** Height filter of a block-level chunk. */
const heightBetween = (column, chunk) =>
  `${column} BETWEEN ${chunk.lo} AND ${chunk.hi}`;

// ---------------------------------------------------------------- tables

/**
 * Table definitions. `level`:
 * - `block`: node-agnostic, height chunks (`b:lo-hi`) or windows;
 * - `tx`: node-agnostic, tx-hash-prefix chunks (`h:xx`) or windows;
 * - `node-block`: per node, height chunks or windows;
 * - `node-tx`: per node, tx-hash-prefix chunks or windows, plus `mempool` with --include-mempool;
 * - `node-mempool`: per node, one `mempool` chunk, only with --include-mempool;
 * - `node-history`: per node, one `all` chunk;
 * - `utxo`: per node, utxo-hash-prefix chunks (`u:x`), current state only.
 */
export const tables = {
  block: {
    level: 'block',
    pg: (ctx, chunk) => ({
      sql: select(
        pgExpr,
        ctx,
        [
          pgExpr.hex('b.hash'),
          pgExpr.int('b.height'),
          pgExpr.int32('b.version'),
          pgExpr.int('b.timestamp'),
          pgExpr.hex('b.previous_block_hash'),
          pgExpr.hex('b.merkle_root'),
          pgExpr.int('b.bits'),
          pgExpr.int('b.nonce'),
          pgExpr.int('b.size_bytes'),
        ],
        [],
        `FROM block b WHERE ${heightBetween('b.height', chunk)}`
      ),
    }),
    ch: (ctx, chunk) => ({
      sql: select(
        chExpr,
        ctx,
        [
          chExpr.hex('b.hash'),
          chExpr.int('b.height'),
          chExpr.int32('b.version'),
          chExpr.int('b.timestamp'),
          chExpr.hex('b.previous_block_hash'),
          chExpr.hex('b.merkle_root'),
          chExpr.int('b.bits'),
          chExpr.int('b.nonce'),
          chExpr.int('b.size_bytes'),
        ],
        [],
        `FROM block_at${agnostic(ctx)} AS b WHERE ${heightBetween(
          'b.height',
          chunk
        )}`
      ),
    }),
  },

  block_transaction: {
    level: 'block',
    pg: (ctx, chunk) => ({
      sql: select(
        pgExpr,
        ctx,
        [
          pgExpr.hex('b.hash'),
          pgExpr.int('bt.transaction_index'),
          pgExpr.hex('t.hash'),
        ],
        [],
        `FROM block b JOIN block_transaction bt ON bt.block_internal_id = b.internal_id
         JOIN transaction t ON t.internal_id = bt.transaction_internal_id
         WHERE ${heightBetween('b.height', chunk)}`
      ),
    }),
    ch: (ctx, chunk) => ({
      sql: select(
        chExpr,
        ctx,
        [
          chExpr.hex('b.hash'),
          chExpr.int('bt.transaction_index'),
          chExpr.hex('bt.transaction_hash'),
        ],
        [],
        `FROM block_transaction_at${agnostic(ctx)} AS bt
         INNER JOIN (SELECT internal_id, hash FROM block_at${agnostic(
           ctx
         )} WHERE ${heightBetween('height', chunk)}) AS b
           ON b.internal_id = bt.block_internal_id
         WHERE bt.block_internal_id IN (SELECT internal_id FROM block_at${agnostic(
           ctx
         )} WHERE ${heightBetween('height', chunk)})`
      ),
    }),
  },

  transaction: {
    level: 'tx',
    pg: (ctx, chunk) => ({
      sql: `WITH ${pgScope(ctx, chunk)} ${select(
        pgExpr,
        ctx,
        [
          pgExpr.hex('t.hash'),
          pgExpr.int32('t.version'),
          pgExpr.int('t.locktime'),
          pgExpr.int('t.size_bytes'),
          pgExpr.bool('t.is_coinbase'),
        ],
        [],
        'FROM scope JOIN transaction t ON t.internal_id = scope.internal_id'
      )}`,
    }),
    ch: (ctx, chunk) => ({
      sql: `WITH ${chScope(ctx, chunk)} ${select(
        chExpr,
        ctx,
        [
          chExpr.hex('t.hash'),
          chExpr.int32('t.version'),
          chExpr.int('t.locktime'),
          chExpr.int('t.size_bytes'),
          chExpr.bool('t.is_coinbase'),
        ],
        [],
        `FROM transaction_at${agnostic(ctx)} AS t WHERE ${chRangeAnd(
          't.hash',
          chunk
        )}t.hash IN (SELECT hash FROM scope)`
      )}`,
    }),
  },

  output: {
    level: 'tx',
    pg: (ctx, chunk) => ({
      sql: `WITH ${pgScope(ctx, chunk)} ${select(
        pgExpr,
        ctx,
        outputFields(pgExpr, 'o'),
        [],
        'FROM scope JOIN output o ON o.transaction_hash = scope.hash'
      )}`,
    }),
    ch: (ctx, chunk) => ({
      sql: `WITH ${chScope(ctx, chunk)} ${select(
        chExpr,
        ctx,
        outputFields(chExpr, 'o'),
        [],
        `FROM output_at${agnostic(ctx)} AS o
         WHERE ${chRangeAnd(
           'o.transaction_hash',
           chunk
         )}o.transaction_hash IN (SELECT hash FROM scope)`
      )}`,
    }),
  },

  input: {
    level: 'tx',
    pg: (ctx, chunk) => ({
      sql: `WITH ${pgScope(ctx, chunk)} ${select(
        pgExpr,
        ctx,
        [
          pgExpr.hex('scope.hash'),
          pgExpr.int('i.input_index'),
          pgExpr.hex('i.outpoint_transaction_hash'),
          pgExpr.int('i.outpoint_index'),
          pgExpr.int('i.sequence_number'),
          pgExpr.hex('i.unlocking_bytecode'),
        ],
        [],
        'FROM scope JOIN input i ON i.transaction_internal_id = scope.internal_id'
      )}`,
    }),
    ch: (ctx, chunk) => ({
      sql: `WITH ${chScope(ctx, chunk)} ${select(
        chExpr,
        ctx,
        [
          chExpr.hex('i.transaction_hash'),
          chExpr.int('i.input_index'),
          chExpr.hex('i.outpoint_transaction_hash'),
          chExpr.int('i.outpoint_index'),
          chExpr.int('i.sequence_number'),
          chExpr.hex('i.unlocking_bytecode'),
        ],
        [],
        `FROM input_at${agnostic(ctx)} AS i
         WHERE ${chRangeAnd(
           'i.transaction_hash',
           chunk
         )}i.transaction_hash IN (SELECT hash FROM scope)`
      )}`,
    }),
  },

  /** Opt-in: ClickHouse's denormalised spent-output columns on input vs a Postgres join (coinbase and unresolved outpoints excluded). */
  input_spent: {
    level: 'tx',
    optIn: true,
    pg: (ctx, chunk) => ({
      sql: `WITH ${pgScope(ctx, chunk)} ${select(
        pgExpr,
        ctx,
        [
          pgExpr.hex('scope.hash'),
          pgExpr.int('i.input_index'),
          ...outputFields(pgExpr, 'o').slice(2),
        ],
        [],
        `FROM scope JOIN input i ON i.transaction_internal_id = scope.internal_id
         JOIN output o ON o.transaction_hash = i.outpoint_transaction_hash AND o.output_index = i.outpoint_index`
      )}`,
    }),
    ch: (ctx, chunk) => ({
      sql: `WITH ${chScope(ctx, chunk)} ${select(
        chExpr,
        ctx,
        [
          chExpr.hex('i.transaction_hash'),
          chExpr.int('i.input_index'),
          ...outputFields(chExpr, 'i').slice(2),
        ],
        [],
        `FROM input_at${agnostic(ctx)} AS i
         WHERE ${chRangeAnd(
           'i.transaction_hash',
           chunk
         )}i.transaction_hash IN (SELECT hash FROM scope)
           AND i.outpoint_transaction_hash != unhex('${zeroHashHex}')`
      )}`,
    }),
  },

  node_block: {
    level: 'node-block',
    ts: ['accepted_at'],
    pg: (ctx, chunk, node) => ({
      sql: select(
        pgExpr,
        ctx,
        [
          pgExpr.hex('b.hash'),
          pgExpr.int('b.height'),
          pgExpr.nullness('nb.accepted_at'),
        ],
        ['nb.accepted_at'],
        `FROM node_block nb JOIN block b ON b.internal_id = nb.block_internal_id
         WHERE nb.node_internal_id = ${node.pgId} AND ${heightBetween(
          'b.height',
          chunk
        )}`
      ),
    }),
    ch: (ctx, chunk, node) => ({
      sql: select(
        chExpr,
        ctx,
        [
          chExpr.hex('nb.block_hash'),
          chExpr.int('nb.height'),
          chExpr.nullness('nb.accepted_at'),
        ],
        ['nb.accepted_at'],
        `FROM node_block_at${nodeArgs(node, ctx)} AS nb WHERE ${heightBetween(
          'nb.height',
          chunk
        )}`
      ),
    }),
  },

  /** tx-accepted(n, t): the ACC predicate on Postgres (block part; mempool part in the `mempool` chunk). */
  tx_acceptance: {
    level: 'node-tx',
    pg: (ctx, chunk, node) => {
      if (chunk.kind === 'mempool') {
        return {
          sql: select(
            pgExpr,
            ctx,
            [
              pgExpr.hex('t.hash'),
              pgExpr.string('mempool'),
              pgExpr.string('-'),
            ],
            [],
            `FROM node_transaction nt JOIN transaction t ON t.internal_id = nt.transaction_internal_id
             WHERE nt.node_internal_id = ${node.pgId}`
          ),
        };
      }
      const where =
        chunk.kind === 'window'
          ? heightBetween('b.height', chunk)
          : pgHashRange('t.hash', chunk) +
            (ctx.atHeight === undefined
              ? ''
              : ` AND b.height <= ${ctx.atHeight}`);
      return {
        sql: select(
          pgExpr,
          ctx,
          [pgExpr.hex('t.hash'), pgExpr.hex('b.hash'), pgExpr.int('b.height')],
          [],
          `FROM transaction t
           JOIN block_transaction bt ON bt.transaction_internal_id = t.internal_id
           JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id AND nb.node_internal_id = ${node.pgId}
           JOIN block b ON b.internal_id = bt.block_internal_id
           WHERE ${where}`
        ),
      };
    },
    ch: (ctx, chunk, node) => {
      if (chunk.kind === 'mempool') {
        return {
          sql: select(
            chExpr,
            ctx,
            [
              chExpr.hex('ta.transaction_hash'),
              chExpr.string('mempool'),
              chExpr.string('-'),
            ],
            [],
            `FROM tx_acceptance_at${nodeArgs(node, ctx)} AS ta WHERE ta.block_internal_id = 0`
          ),
        };
      }
      const where =
        chunk.kind === 'window'
          ? `ta.transaction_hash IN (SELECT transaction_hash FROM block_transaction_at${agnostic(
              ctx
            )}
               WHERE block_internal_id IN (SELECT internal_id FROM block_at${agnostic(
                 ctx
               )} WHERE ${heightBetween('height', chunk)}))
             AND ${heightBetween('ta.height', chunk)}`
          : chHashRange('ta.transaction_hash', chunk) +
            (ctx.atHeight === undefined
              ? ''
              : ` AND ta.height <= ${ctx.atHeight}`);
      return {
        sql: select(
          chExpr,
          ctx,
          [
            chExpr.hex('ta.transaction_hash'),
            chExpr.hex('b.hash'),
            chExpr.int('ta.height'),
          ],
          [],
          `FROM tx_acceptance_at${nodeArgs(node, ctx)} AS ta
           INNER JOIN (SELECT internal_id, hash FROM block_at${agnostic(
             ctx
           )}) AS b ON b.internal_id = ta.block_internal_id
           WHERE ta.block_internal_id != 0 AND ${where}`
        ),
      };
    },
  },

  node_transaction: {
    level: 'node-mempool',
    ts: ['validated_at'],
    pg: (ctx, _chunk, node) => ({
      sql: select(
        pgExpr,
        ctx,
        [pgExpr.hex('t.hash'), pgExpr.nullness('nt.validated_at')],
        ['nt.validated_at'],
        `FROM node_transaction nt JOIN transaction t ON t.internal_id = nt.transaction_internal_id
         WHERE nt.node_internal_id = ${node.pgId}`
      ),
    }),
    ch: (ctx, _chunk, node) => ({
      sql: select(
        chExpr,
        ctx,
        [chExpr.hex('nt.transaction_hash'), chExpr.nullness('nt.validated_at')],
        ['nt.validated_at'],
        `FROM node_transaction_at${nodeArgs(node, ctx)} AS nt`
      ),
    }),
  },

  node_block_history: {
    level: 'node-history',
    ts: ['accepted_at', 'removed_at'],
    pg: (ctx, _chunk, node) => ({
      sql: select(
        pgExpr,
        ctx,
        [
          pgExpr.hex('b.hash'),
          pgExpr.int('b.height'),
          pgExpr.nullness('h.accepted_at'),
        ],
        ['h.accepted_at', 'h.removed_at'],
        `FROM node_block_history h JOIN block b ON b.internal_id = h.block_internal_id
         WHERE h.node_internal_id = ${node.pgId}${
          ctx.historyHeight === undefined
            ? ''
            : ` AND b.height <= ${ctx.historyHeight}`
        }`
      ),
    }),
    ch: (ctx, _chunk, node) => ({
      sql: select(
        chExpr,
        ctx,
        [
          chExpr.hex('b.hash'),
          chExpr.int('b.height'),
          chExpr.nullness('h.accepted_at'),
        ],
        ['h.accepted_at', 'h.removed_at'],
        `FROM node_block_history_at${nodeArgs(node, ctx)} AS h
         INNER JOIN (SELECT internal_id, hash, height FROM block_at${agnostic(
           ctx
         )}) AS b ON b.internal_id = h.block_internal_id
         ${
           ctx.historyHeight === undefined
             ? ''
             : `WHERE b.height <= ${ctx.historyHeight}`
         }`
      ),
    }),
  },

  node_transaction_history: {
    level: 'node-history',
    ts: ['validated_at', 'replaced_at'],
    pg: (ctx, _chunk, node) => ({
      sql: select(
        pgExpr,
        ctx,
        [
          pgExpr.hex('t.hash'),
          pgExpr.nullness('h.validated_at'),
          pgExpr.nullness('h.replaced_at'),
        ],
        ['h.validated_at', 'h.replaced_at'],
        `FROM node_transaction_history h JOIN transaction t ON t.internal_id = h.transaction_internal_id
         WHERE h.node_internal_id = ${node.pgId}`
      ),
    }),
    ch: (ctx, _chunk, node) => ({
      sql: select(
        chExpr,
        ctx,
        [
          chExpr.hex('t.hash'),
          chExpr.nullness('h.validated_at'),
          chExpr.nullness('h.replaced_at'),
        ],
        ['h.validated_at', 'h.replaced_at'],
        `FROM node_transaction_history_at${nodeArgs(node, ctx)} AS h
         INNER JOIN (SELECT internal_id, hash FROM transaction_at${agnostic(
           ctx
         )}
                     WHERE internal_id IN (SELECT transaction_internal_id FROM node_transaction_history_at${nodeArgs(node, ctx)})) AS t
           ON t.internal_id = h.transaction_internal_id`
      ),
    }),
  },

  /** unspent(n, o): ClickHouse utxo_at vs Postgres F1g unspent_output(node name). */
  utxo: {
    level: 'utxo',
    pg: (ctx, chunk, node) => ({
      sql: select(
        pgExpr,
        ctx,
        outputFields(pgExpr, 'o'),
        [],
        `FROM unspent_output(${sqlString(node.name)}) o WHERE ${pgHashRange(
          'o.transaction_hash',
          chunk
        )}`
      ),
    }),
    ch: (ctx, chunk, node) => ({
      sql: select(
        chExpr,
        ctx,
        outputFields(chExpr, 'u'),
        [],
        `FROM utxo_at${nodeArgs(node, ctx)} AS u WHERE ${chHashRange(
          'u.transaction_hash',
          chunk
        )}`
      ),
    }),
  },
};

export const defaultTables = [
  'block',
  'block_transaction',
  'transaction',
  'output',
  'input',
  'node_block',
  'tx_acceptance',
  'node_transaction',
  'node_block_history',
  'node_transaction_history',
  'utxo',
];

// ---------------------------------------------------------------- digests

/**
 * Wrap a canonical SELECT into a digest query.
 * - `sum` (default): count, and the sums of the two big-endian 64-bit halves of
 *   md5(s) over all rows; order-independent, constant memory, additive across
 *   chunks. Reduced mod 2^64 by the caller (`digestFromSums`).
 * - `ordered`: md5 of all `s` sorted bytewise and joined with '\n' (Postgres
 *   COLLATE "C" = ClickHouse bytewise order); needs the chunk in memory.
 */
export const digestSql = {
  pg: (inner, mode) =>
    mode === 'ordered'
      ? `SELECT count(*)::text AS n, md5(coalesce(string_agg(s, E'\\n' ORDER BY s COLLATE "C"), '')) AS m FROM (${inner}) q`
      : `SELECT count(*)::text AS n,
           coalesce(sum(('x' || substr(h, 1, 16))::bit(64)::bigint::numeric), 0)::text AS a,
           coalesce(sum(('x' || substr(h, 17, 16))::bit(64)::bigint::numeric), 0)::text AS b
         FROM (SELECT md5(s) AS h FROM (${inner}) q) z`,
  ch: (inner, mode) =>
    mode === 'ordered'
      ? `SELECT toString(count()) AS n, lower(hex(MD5(arrayStringConcat(arraySort(groupArray(s)), '\\n')))) AS m FROM (${inner})`
      : `SELECT toString(count()) AS n,
           toString(sum(reinterpretAsUInt64(reverse(substring(MD5(s), 1, 8))))) AS a,
           toString(sum(reinterpretAsUInt64(reverse(substring(MD5(s), 9, 8))))) AS b
         FROM (${inner})`,
};

/** Sorted row streams for --diff and the timestamp pass. */
export const rowsSql = {
  pg: (inner, tsCount) =>
    `SELECT s${Array.from(
      { length: tsCount },
      (_, index) => `, t${index}`
    ).join('')} FROM (${inner}) q ORDER BY s COLLATE "C"${Array.from(
      { length: tsCount },
      (_, index) => `, t${index}`
    ).join('')}`,
  ch: (inner, tsCount) =>
    `SELECT s${Array.from(
      { length: tsCount },
      (_, index) => `, t${index}`
    ).join('')} FROM (${inner}) ORDER BY s${Array.from(
      { length: tsCount },
      (_, index) => `, t${index}`
    ).join('')}`,
};
