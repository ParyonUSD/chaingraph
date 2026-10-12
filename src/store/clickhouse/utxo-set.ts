/* eslint-disable functional/no-let, functional/no-loop-statement, no-await-in-loop, functional/no-try-statement, @typescript-eslint/naming-convention, camelcase, @typescript-eslint/no-magic-numbers, @typescript-eslint/parameter-properties, complexity, max-params, no-continue, @typescript-eslint/member-ordering, max-lines, @typescript-eslint/no-loop-func */
// cspell:ignore clickhouse unhex seqs dedup rebase rebased rebases analysed
/**
 * The stored per-node UTXO set, maintained off the commit path
 * (`ddl/090_utxo_set.sql`, docs/clickhouse-port/utxo-async.md).
 *
 * Definition (Chaingraph v1, per node n at a commit seq X): output o is in
 * the set iff the transaction creating o is accepted by n at X (a block n
 * accepts or n's mempool, from `tx_acceptance` rows with seq at most X) and no
 * input of a transaction accepted by n at X spends o. Every acceptance fact
 * is per node; nothing here answers for "any node".
 *
 * Storage: `utxo_set` holds, per node and generation, signed rows tagged
 * with the batch (a commit seq) that wrote them. The set at a published
 * batch end U of generation g is `GROUP BY outpoint HAVING sum(sign) > 0`
 * over the rows of (n, g) with `batch <= U`; every outpoint's sum is 0 or
 * 1 there. `utxo_total` (filled by the materialized view `utxo_total_mv`
 * from every insert into `utxo_set`) holds the same rows summed per
 * (batch, token category): count, value, fungible count and amount,
 * non-fungible count.
 *
 * The change rows of (from, to] for node n (`utxoChangeSql`): the
 * transactions whose acceptance by n changed between the two seqs (found
 * through the commits' `node_block` / `node_transaction` rows: every
 * `tx_acceptance` row is written in the same commit as one of those), the
 * outputs they create and the outputs they spend, and for each such output
 * its membership at `from` and at `to`. A row per output whose membership
 * differs, sign = in(to) - in(from). Outputs outside that set keep their
 * membership: neither their creator's acceptance nor any spender's changed.
 *
 * The maintainer (`UtxoSetMaintainer`, a background task of the lease
 * holder, never on a commit path) writes the change rows of (U, W'] for
 * W' at most visible(n) and publishes W' in `utxo_watermark`. Readers take U
 * from their snapshot (the newest published batch end at or below their
 * visible(n)) and add the change rows of (U, visible(n)] computed at query
 * time: exact at the snapshot whatever the maintainer's lag.
 *
 * Rebase: when a generation's rows grow past a bound, the set at U is
 * written as generation g + 1 (sign +1, batch U) and published; the old
 * generation's partitions are dropped after a grace period.
 */
import type {
  ClickHouseClient,
  ClickHouseSettings,
  QueryParams,
} from './client.js';
import { nextBatchEnd } from './spend-index.js';
import type { MultiNodeSnapshot, VisibilitySnapshot } from './visibility.js';
import {
  inlinePinnedView,
  nodeAgnosticId,
  pinnedGateTerms,
  readSnapshotMulti,
  snapshotForNode,
  snapshotParams,
} from './visibility.js';

/** The output columns a `utxo_set` row carries (as `output`, without commit_seq). */
export const utxoSetOutputColumns = [
  'transaction_hash',
  'output_index',
  'transaction_internal_id',
  'value_satoshis',
  'locking_bytecode',
  'token_category',
  'fungible_token_amount',
  'nonfungible_token_capability',
  'nonfungible_token_commitment',
] as const;

/** The MATERIALIZED key columns of `output` / `utxo_set` (not part of `*`). */
export const utxoSetDerivedColumns = [
  'locking_bytecode_prefix',
  'nonfungible_token_commitment_key',
] as const;

/** Every output column a reader sees (output rows of the API). */
export const utxoSetReadColumns = [
  ...utxoSetOutputColumns,
  ...utxoSetDerivedColumns,
] as const;

const zeroHash = "toFixedString(unhex(repeat('00', 32)), 32)";

/**
 * How a builder registers a named array: `body` is a one-row, one-column
 * subquery (`SELECT groupArray(…) FROM …`); the returned name is a WITH
 * scalar of the statement holding that array.
 *
 * Why scalars, not `MATERIALIZED` CTEs: ClickHouse 26.8's analyzer
 * re-analyses the whole definition of a materialized CTE definition, recursively, at every
 * reference (phase2-api.md §6.1). The change rows chain five steps that
 * reference earlier ones several times, so the first step was analysed 12
 * times: a one-commit correction cost ~200 ms before reading a row. A scalar
 * subquery is analysed and evaluated once and its references are constants
 * (one-commit correction ~30 ms on chipnet).
 */
export type ScalarRegistrar = (prefix: string, body: string) => string;

/**
 * Collect statement-level scalars (a standalone statement: the maintainer,
 * the view): `register` names them, `withClause()` emits them.
 */
export const scalarCollector = () => {
  const items: string[] = [];
  const register: ScalarRegistrar = (prefix, body) => {
    const name = `${prefix}_${items.length + 1}`;
    items.push(`(${body}) AS ${name}`);
    return name;
  };
  return {
    items,
    register,
    withClause: () => (items.length === 0 ? '' : `WITH ${items.join(',\n')}\n`),
  };
};

/**
 * Parameter placeholders of a change-row statement. `node` and `to` are
 * parameter NAMES (the node gate binds `{node:UInt32}` and
 * `{to:UInt64}`); `from` is a full placeholder or literal.
 */
export interface UtxoChangeParams {
  /** Name of the node parameter (default `node`). */
  node?: string;
  /** Name of the upper seq parameter (default `visible`: the snapshot's visible(n)). */
  to?: string;
  /** The lower seq: a placeholder such as `{utxo:UInt64}`. */
  from: string;
}

export interface UtxoChangeContext extends UtxoChangeParams {
  scalar: ScalarRegistrar;
  /**
   * The snapshot whose arrays are bound (inlined gate form); undefined: the
   * general form (DDL views).
   */
  snapshot?: Pick<VisibilitySnapshot, 'committedTail' | 'void'>;
  /** Database qualifier of the tables (`cg.` in the DDL). */
  qualifier?: string;
  /**
   * A predicate over output columns: only change rows of matching outputs
   * are needed (a filtered read). The affected outputs are narrowed to the
   * matching ones before their spenders are looked up, so a selective
   * filter's correction reads few spenders and acceptance rows. Every row of
   * an outpoint carries the same output columns, so this is exact.
   */
  where?: string;
}

const tableName = (qualifier: string, table: string) => `${qualifier}${table}`;

/** The elements of a WITH array, as a subquery set (index-usable `IN`). */
const elements = (array: string) => `SELECT arrayJoin(${array})`;

/** Elements of a WITH array of tuples, as columns `names` (a subquery). */
const tupleRows = (array: string, names: readonly string[]) =>
  `SELECT ${names
    .map((name, index) => `tupleElement(__x, ${index + 1}) AS ${name}`)
    .join(', ')} FROM (SELECT arrayJoin(${array}) AS __x)`;

/**
 * Per transaction of `hashes` (a subquery of `transaction_hash`): accepted
 * by the node at `from` (`b`) and at `to` (`a`). One read of the
 * transactions' acceptance rows by the primary key.
 */
const acceptanceBeforeAfter = (context: UtxoChangeContext, hashes: string) => {
  const node = context.node ?? 'node';
  const to = context.to ?? 'visible';
  const qualifier = context.qualifier ?? '';
  const terms = pinnedGateTerms('node', {
    params: { node, visible: to },
    qualifier,
    snapshot: context.snapshot,
  });
  return `SELECT transaction_hash, max(b) AS b, max(a) AS a FROM (
    SELECT transaction_hash, sumIf(sign, commit_seq <= ${
      context.from
    }) > 0 AS b, sum(sign) > 0 AS a
    FROM ${tableName(qualifier, 'tx_acceptance')}
    WHERE transaction_hash IN (${hashes}) AND ${terms.join(' AND ')}
    GROUP BY transaction_hash, block_internal_id)
  GROUP BY transaction_hash`;
};

/**
 * The change rows of node `{node}` over (from, to]: one row per output
 * whose membership in the node's UTXO set differs between the two seqs,
 * with the output's columns (`utxoSetReadColumns`) and `sign` (+1 entered,
 * -1 left). `to` must be at most the snapshot's visible(n) (every seq up
 * to it is terminal); output/input rows are read at the snapshot's
 * node-agnostic gate (`visible0`, `tail`), which shows every row of a
 * transaction a seq up to visible(n) accepts.
 *
 * Steps (each a WITH scalar, `ScalarRegistrar`):
 * 1. touched: transactions with an acceptance row of the node in (from,
 *    to], through the commits' `node_block` / `node_transaction` rows (every
 *    `tx_acceptance` row is written in the same commit as one of those);
 * 2. changed: those whose acceptance by the node differs at from and to;
 * 3. affected: the outputs they create and the outputs they spend;
 * 4. spenders: every stored input spending an affected output;
 * 5. involved: acceptance at from and to of the affected outputs' creators
 *    and of the spenders;
 * then per affected output: in(x) = creator accepted at x and no spender
 * accepted at x; sign = in(to) - in(from). Outputs outside the affected set
 * keep their membership: neither their creator's acceptance nor any
 * spender's changed.
 */
export const utxoChangeSql = (context: UtxoChangeContext) => {
  const node = context.node ?? 'node';
  const to = context.to ?? 'visible';
  const qualifier = context.qualifier ?? '';
  const { scalar, from } = context;
  const nodeRange = [
    `commit_seq > ${from}`,
    ...pinnedGateTerms('node', {
      params: { node, visible: to },
      qualifier,
      snapshot: context.snapshot,
    }),
  ].join(' AND ');
  const agnostic = pinnedGateTerms('agnostic', {
    qualifier,
    snapshot: context.snapshot,
  }).join(' AND ');
  const table = (name: string) => tableName(qualifier, name);
  const touched = scalar(
    'utxo_touched',
    `SELECT groupUniqArray(transaction_hash) FROM (
      SELECT transaction_hash FROM ${table('block_transaction')}
      WHERE block_internal_id IN (SELECT block_internal_id FROM ${table(
        'node_block'
      )} WHERE ${nodeRange})
        AND ${agnostic}
      UNION ALL
      SELECT transaction_hash FROM ${table(
        'node_transaction'
      )} WHERE ${nodeRange})`
  );
  const changed = scalar(
    'utxo_changed',
    `SELECT groupArray(transaction_hash) FROM (${acceptanceBeforeAfter(
      context,
      elements(touched)
    )}) WHERE b != a`
  );
  const affected = scalar(
    'utxo_affected',
    /*
     * The input branch keeps its own column names (an alias would shadow
     * transaction_hash in its WHERE). Coinbase outpoints are dropped from the
     * array, not in the WHERE: `outpoint_transaction_hash != zero` there is a
     * condition on p_outpoint's leading key column, which ClickHouse analyses
     * against every mark of that projection before choosing the base table
     * (synthetic mainnet scale: 2.9 M marks, ~0.8 s per statement;
     * planning-floor.md).
     */
    `SELECT arrayFilter(x -> x.1 != ${zeroHash}, groupUniqArray((h, i))) FROM (
      SELECT transaction_hash AS h, output_index AS i FROM ${table('output')}
      WHERE transaction_hash IN (${elements(changed)}) AND ${agnostic}
      UNION ALL
      SELECT outpoint_transaction_hash AS h, outpoint_index AS i FROM ${table(
        'input'
      )}
      WHERE transaction_hash IN (${elements(changed)}) AND ${agnostic})`
  );
  const narrowed =
    context.where === undefined || context.where === '1'
      ? affected
      : scalar(
          'utxo_matching',
          `SELECT groupUniqArray((transaction_hash, output_index)) FROM ${table(
            'output'
          )}
           WHERE (transaction_hash, output_index) IN (${tupleRows(affected, [
             'h',
             'i',
           ])})
             AND ${agnostic} AND (${context.where})`
        );
  const affectedRows = tupleRows(narrowed, ['h', 'i']);
  const spenders = scalar(
    'utxo_spenders',
    `SELECT groupUniqArray((outpoint_transaction_hash, outpoint_index, transaction_hash)) FROM ${table(
      'input'
    )}
      WHERE (outpoint_transaction_hash, outpoint_index) IN (${affectedRows})
        AND ${agnostic}`
  );
  const spenderRows = tupleRows(spenders, ['h', 'i', 'spender']);
  const involved = scalar(
    'utxo_involved',
    `SELECT groupArray((transaction_hash, b, a)) FROM (${acceptanceBeforeAfter(
      context,
      `SELECT h FROM (${affectedRows}) UNION ALL SELECT spender FROM (${spenderRows})`
    )})`
  );
  const involvedRows = tupleRows(involved, ['transaction_hash', 'b', 'a']);
  const columns = utxoSetReadColumns.map(
    (column) => `o.${column} AS ${column}`
  );
  return `SELECT ${columns.join(', ')},
    toInt8(ifNull(c.a, 0) AND NOT ifNull(s.a, 0)) - toInt8(ifNull(c.b, 0) AND NOT ifNull(s.b, 0)) AS sign
  FROM (SELECT ${utxoSetReadColumns.join(', ')} FROM ${table('output')}
        WHERE (transaction_hash, output_index) IN (${affectedRows})
          AND ${agnostic}
        LIMIT 1 BY transaction_hash, output_index) AS o
  LEFT JOIN (${involvedRows}) AS c ON c.transaction_hash = o.transaction_hash
  LEFT JOIN (SELECT sp.h AS h, sp.i AS i, max(ac.b) AS b, max(ac.a) AS a
             FROM (${spenderRows}) AS sp INNER JOIN (${involvedRows}) AS ac ON ac.transaction_hash = sp.spender
             GROUP BY h, i) AS s
    ON s.h = o.transaction_hash AND s.i = o.output_index
  WHERE sign != 0`;
};

/** Placeholders of a stored-set read. */
export interface UtxoSetReadParams {
  /** Node id placeholder (default `{node:UInt32}`). */
  node?: string;
  /** U (default `{utxo:UInt64}`). */
  watermark?: string;
  /** Generation (default `{utxo_generation:UInt32}`). */
  generation?: string;
  qualifier?: string;
}

/** The stored rows of the node's set at U (signed; group them). */
export const utxoStoredRowsSql = (
  params: UtxoSetReadParams = {},
  where = '1'
) =>
  `SELECT ${utxoSetReadColumns.join(', ')}, sign FROM ${
    params.qualifier ?? ''
  }utxo_set
  WHERE node_internal_id = ${params.node ?? '{node:UInt32}'}
    AND generation = ${params.generation ?? '{utxo_generation:UInt32}'}
    AND batch <= ${params.watermark ?? '{utxo:UInt64}'}
    AND (${where})`;

/**
 * The node's UTXO set at the snapshot (read rule): stored rows up to U plus
 * the change rows of (U, visible(n)], both filtered by `where` (a predicate
 * over the output columns; every row of an outpoint carries the same
 * values, so filtering before the collapse is exact), collapsed per
 * outpoint. `withChanges` false when U = visible(n) (nothing to add).
 */
export const utxoSetRowsSql = ({
  change,
  read = {},
  where = '1',
  withChanges,
}: {
  change: UtxoChangeContext;
  read?: UtxoSetReadParams;
  where?: string;
  withChanges: boolean;
}) => {
  const values = utxoSetReadColumns
    .filter(
      (column) => column !== 'transaction_hash' && column !== 'output_index'
    )
    .map((column) => `any(${column}) AS ${column}`);
  const parts = [
    utxoStoredRowsSql(read, where),
    ...(withChanges
      ? [
          `SELECT ${utxoSetReadColumns.join(', ')}, sign FROM (${utxoChangeSql({
            ...change,
            where,
          })}) WHERE ${where}`,
        ]
      : []),
  ];
  return `SELECT transaction_hash, output_index, ${values.join(', ')}
  FROM (${parts.join('\n  UNION ALL\n  ')})
  GROUP BY transaction_hash, output_index
  HAVING sum(sign) > 0`;
};

/** Total columns of `utxo_total` (and the per-category totals of a read). */
export const utxoTotalColumns = [
  'outputs',
  'value_satoshis',
  'fungible_outputs',
  'fungible_token_amount',
  'nonfungible_outputs',
] as const;

/**
 * Per token category (zero category = no token): the node's UTXO count,
 * value, fungible count and amount, non-fungible count at the snapshot:
 * `utxo_total` rows up to U plus the change rows of (U, visible(n)].
 * `category`: a placeholder restricting to one category.
 */
export const utxoTotalsSql = ({
  category,
  change,
  read = {},
  withChanges,
}: {
  category?: string;
  change: UtxoChangeContext;
  read?: UtxoSetReadParams;
  withChanges: boolean;
}) => {
  const categoryTerm =
    category === undefined ? '1' : `token_category = ${category}`;
  const stored = `SELECT token_category, ${utxoTotalColumns.join(', ')} FROM ${
    read.qualifier ?? ''
  }utxo_total
  WHERE node_internal_id = ${read.node ?? '{node:UInt32}'}
    AND generation = ${read.generation ?? '{utxo_generation:UInt32}'}
    AND batch <= ${read.watermark ?? '{utxo:UInt64}'}
    AND ${categoryTerm}`;
  /* built only when used: building registers the change steps as WITH scalars */
  const changes =
    () => `SELECT token_category, toInt64(s) AS outputs, s * v AS value_satoshis,
    if(isNull(f), 0, toInt64(s)) AS fungible_outputs,
    toInt128(s) * ifNull(f, 0) AS fungible_token_amount,
    if(isNull(c), 0, toInt64(s)) AS nonfungible_outputs
  FROM (SELECT token_category, sign AS s, value_satoshis AS v, fungible_token_amount AS f,
          nonfungible_token_capability AS c
        FROM (${utxoChangeSql({
          ...change,
          where: categoryTerm,
        })}) WHERE ${categoryTerm})`;
  return `SELECT token_category, sum(n) AS outputs, sum(v) AS value_satoshis, sum(fn) AS fungible_outputs,
    sum(toInt128(fa)) AS fungible_token_amount, sum(nn) AS nonfungible_outputs
  FROM (SELECT token_category, outputs AS n, value_satoshis AS v, fungible_outputs AS fn,
          fungible_token_amount AS fa, nonfungible_outputs AS nn
        FROM (${stored}${withChanges ? `\n  UNION ALL\n  ${changes()}` : ''}))
  GROUP BY token_category
  HAVING outputs != 0 OR fungible_outputs != 0 OR nonfungible_outputs != 0 OR value_satoshis != 0`;
};

/**
 * The parameterized view `utxo_set_at` (generated into
 * ddl/090_utxo_set.sql; utxo-set.spec.ts checks the committed text): the
 * node's set at a snapshot for ad-hoc readers (the parity harness, the
 * checker). Parameters: the node view's (`node`, `visible`, `fence`,
 * `void`), the node-agnostic view's (`visible0`, `tail`) and the snapshot's
 * `utxo` (U) and `utxo_generation`. Its steps are WITH scalars.
 */
export const utxoSetViewStatement = () => {
  const scalars = scalarCollector();
  const rows = utxoSetRowsSql({
    change: {
      from: '{utxo:UInt64}',
      qualifier: 'cg.',
      scalar: scalars.register,
    },
    read: { qualifier: 'cg.' },
    withChanges: true,
  });
  return `CREATE OR REPLACE VIEW cg.utxo_set_at AS
${scalars.withClause()}${rows};`;
};

/* ------------------------------------------------------------------ */
/* checks (tests, tools, the parity scripts)                            */
/* ------------------------------------------------------------------ */

/**
 * The node's unspent outputs at the snapshot computed at query time
 * (Chaingraph v1's definition, as `ClickHouseChecker.unspent` with UTXO
 * tables off): outputs of transactions the node accepts, minus outpoints
 * spent by an input of a transaction the node accepts. Output columns;
 * bind `snapshotParams(snapshot)`.
 */
export const unspentAtQueryTimeSql = (
  snapshot: Pick<VisibilitySnapshot, 'committedTail' | 'void'>,
  where?: string
) => {
  const agnostic = pinnedGateTerms('agnostic', { snapshot }).join(' AND ');
  const accepted = `SELECT transaction_hash FROM ${inlinePinnedView(
    'tx_acceptance_at',
    snapshot
  )}`;
  const outputs = `SELECT ${utxoSetReadColumns.join(', ')} FROM output
    WHERE ${agnostic} AND ${where ?? '1'}`;
  /* a scope (`where`): only the inputs spending the scope's outputs are read (input.p_outpoint) */
  const scoped =
    where === undefined
      ? ''
      : ` AND (outpoint_transaction_hash, outpoint_index) IN (SELECT transaction_hash, output_index FROM (${outputs}))`;
  return `SELECT o.* FROM (
    ${outputs} AND transaction_hash IN (${accepted})
    LIMIT 1 BY transaction_hash, output_index) AS o
  LEFT ANTI JOIN (SELECT outpoint_transaction_hash, outpoint_index FROM input
                  WHERE ${agnostic}${scoped} AND transaction_hash IN (${accepted})) AS s
    ON s.outpoint_transaction_hash = o.transaction_hash AND s.outpoint_index = o.output_index`;
};

/** What `checkUtxoSet` found: empty lists and equal totals = exact. */
export interface UtxoSetCheck {
  node: number;
  /** The mark the stored read used (the snapshot's, or the forced one). */
  watermark: bigint;
  generation: number;
  visible: bigint;
  /** Outpoints of the query-time set (the reference). */
  count: number;
  /** In the reference, not in the stored read (up to 20). */
  missing: string[];
  /** In the stored read, not in the reference (up to 20). */
  extra: string[];
  /** In both with different column values (up to 20). */
  mismatched: string[];
  /** Per-category totals (stored totals + correction) equal the reference's. */
  totalsEqual: boolean;
  /** Outpoints whose stored sum of signs up to U is not 0 or 1 (up to 20). */
  invariant: string[];
}

const rowKey =
  "concat(lower(hex(transaction_hash)), ':', toString(output_index))";
/* toString of a tuple renders NULL as 'NULL': cityHash64 over a NULL argument would be NULL and drop the row from the comparison */
const rowDigest = `cityHash64(toString(tuple(transaction_internal_id, value_satoshis, locking_bytecode, token_category,
  fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment, locking_bytecode_prefix,
  nonfungible_token_commitment_key)))`;

/**
 * The stored set of `nodeId` read by the read rule (stored rows up to U plus
 * the correction for (U, visible(n)]) and its per-category totals, against
 * the query-time definition, at one snapshot. `mark`: read as if the
 * snapshot's watermark were this published (U, generation) (a lagging
 * reader; U must be at most visible(n) and published). Undefined when the
 * node has no stored set (and no mark is forced).
 */
export const checkUtxoSet = async (
  client: Pick<ClickHouseClient, 'query'>,
  nodeId: number,
  mark?: { watermark: bigint; generation: number }
): Promise<UtxoSetCheck | undefined> => {
  const snapshot = snapshotForNode(
    await readSnapshotMulti(client, [nodeId], { utxo: true }),
    nodeId
  );
  const used = mark ?? snapshot.utxo;
  if (used === undefined) return undefined;
  const pinned: VisibilitySnapshot = {
    ...snapshot,
    utxo: {
      generation: used.generation,
      lagCommits: mark === undefined ? snapshot.utxo!.lagCommits : 1,
      lagRows: 0,
      watermark: used.watermark,
    },
  };
  const params = snapshotParams(pinned);
  const withChanges = pinned.utxo!.lagCommits > 0;
  const rows = scalarCollector();
  const stored = utxoSetRowsSql({
    change: { from: '{utxo:UInt64}', scalar: rows.register, snapshot: pinned },
    withChanges,
  });
  const reference = unspentAtQueryTimeSql(pinned);
  const diff = await client.query<{ kind: string; key: string }>(
    `${rows.withClause()}SELECT kind, key FROM (
       SELECT if(r.k = '', 'extra', if(s.k = '', 'missing', 'mismatched')) AS kind,
              if(r.k = '', s.k, r.k) AS key
       FROM (SELECT ${rowKey} AS k, ${rowDigest} AS d FROM (${reference})) AS r
       FULL OUTER JOIN (SELECT ${rowKey} AS k, ${rowDigest} AS d FROM (${stored})) AS s
         ON r.k = s.k
       WHERE r.k = '' OR s.k = '' OR r.d != s.d)
     ORDER BY kind, key LIMIT 3 BY kind
     SETTINGS join_use_nulls = 0`,
    params
  );
  const [counted] = await client.query<{ n: string }>(
    `SELECT toString(count()) AS n FROM (${reference})`,
    params
  );
  const totals = scalarCollector();
  const storedTotals = utxoTotalsSql({
    change: {
      from: '{utxo:UInt64}',
      scalar: totals.register,
      snapshot: pinned,
    },
    withChanges,
  });
  const [compared] = await client.query<{ equal: number }>(
    `${totals.withClause()}SELECT
       (SELECT groupArraySorted(100000)(tuple(token_category, toInt64(outputs), toInt64(value_satoshis),
          toInt64(fungible_outputs), toInt128(fungible_token_amount), toInt64(nonfungible_outputs)))
        FROM (${storedTotals}))
       = (SELECT groupArraySorted(100000)(tuple(token_category, toInt64(outputs), toInt64(value_satoshis),
          toInt64(fungible_outputs), toInt128(fungible_token_amount), toInt64(nonfungible_outputs)))
          FROM (SELECT token_category, count() AS outputs, sum(v) AS value_satoshis,
                  countIf(isNotNull(f)) AS fungible_outputs,
                  sum(toInt128(ifNull(f, 0))) AS fungible_token_amount,
                  countIf(isNotNull(c)) AS nonfungible_outputs
                FROM (SELECT token_category, value_satoshis AS v, fungible_token_amount AS f,
                        nonfungible_token_capability AS c FROM (${reference}))
                GROUP BY token_category)) AS equal`,
    params
  );
  const invariant = await client.query<{ key: string }>(
    `SELECT concat(${rowKey}, ' sum ', toString(sum(sign))) AS key FROM utxo_set
     WHERE node_internal_id = {node:UInt32} AND generation = {utxo_generation:UInt32} AND batch <= {utxo:UInt64}
     GROUP BY transaction_hash, output_index HAVING sum(sign) NOT IN (0, 1) LIMIT 20`,
    params
  );
  const of = (kind: string) =>
    diff.filter((row) => row.kind === kind).map((row) => row.key);
  return {
    count: Number(counted?.n ?? '0'),
    extra: of('extra'),
    generation: used.generation,
    invariant: invariant.map((row) => row.key),
    mismatched: of('mismatched'),
    missing: of('missing'),
    node: nodeId,
    totalsEqual: Number(compared?.equal ?? 0) === 1,
    visible: snapshot.visible,
    watermark: used.watermark,
  };
};

/* ------------------------------------------------------------------ */
/* the periodic spot-check (owner decision 2026-10-12)                  */
/* ------------------------------------------------------------------ */

/** One compared scope of a spot-check: the stored set's answer and the query-time one. */
export interface UtxoSpotCheckScope {
  kind: 'category' | 'prefix' | 'totals';
  /** The token category or locking-bytecode prefix (hex); empty for `totals`. */
  value: string;
  stored: string;
  reference: string;
  equal: boolean;
}

/** A spot-check of one node at one snapshot. */
export interface UtxoSetSpotCheck {
  node: number;
  watermark: bigint;
  generation: number;
  visible: bigint;
  scopes: UtxoSpotCheckScope[];
  /** Scopes not compared (a statement over its time budget). */
  skipped: { kind: UtxoSpotCheckScope['kind']; value: string; error: string }[];
}

// eslint-disable-next-line functional/no-mixed-type
export interface UtxoSpotCheckOptions {
  /** Random token categories (default 3; from the node's running totals). */
  categories?: number;
  /** Random locking-bytecode prefixes (default 3; from outputs at a random transaction hash). */
  prefixes?: number;
  /** Compare the node's whole-set totals (default true). */
  totals?: boolean;
  /** These categories / prefixes (hex) instead of random ones (tests, tools). */
  categoryValues?: readonly string[];
  prefixValues?: readonly string[];
  /** Server-side budget of each statement, s (default 120). */
  maxExecutionSeconds?: number;
  /** Random source (tests). */
  random?: () => number;
}

const scopeDigest = (rows: string) =>
  `SELECT concat(toString(count()), ' outputs, digest ', hex(groupBitXor(cityHash64(${rowKey}, ${rowDigest})))) AS answer FROM (${rows})`;

const totalsTuple = `tuple(token_category, toInt64(outputs), toInt64(value_satoshis),
  toInt64(fungible_outputs), toInt128(fungible_token_amount), toInt64(nonfungible_outputs))`;

const totalsDigest = (totals: string) =>
  `SELECT concat(toString(count()), ' categories, ', toString(sum(toInt64(outputs))), ' outputs, ',
     toString(sum(toInt64(value_satoshis))), ' sat, digest ', hex(cityHash64(groupArraySorted(1000000)(${totalsTuple})))) AS answer
   FROM (${totals})`;

const referenceTotals = (reference: string) =>
  `SELECT token_category, count() AS outputs, sum(v) AS value_satoshis,
     countIf(isNotNull(f)) AS fungible_outputs, sum(toInt128(ifNull(f, 0))) AS fungible_token_amount,
     countIf(isNotNull(c)) AS nonfungible_outputs
   FROM (SELECT token_category, value_satoshis AS v, fungible_token_amount AS f,
           nonfungible_token_capability AS c FROM (${reference}))
   GROUP BY token_category`;

const randomHash = (random: () => number) =>
  Array.from({ length: 32 }, () =>
    Math.floor(random() * 256)
      .toString(16)
      .padStart(2, '0')
  ).join('');

/**
 * Compare a few random scopes of a node's stored set with the query-time
 * definition, all at ONE snapshot (the stored read at the snapshot's own
 * mark plus its correction): token categories (rows and per-category
 * totals), locking-bytecode prefixes (rows) and the node's whole-set totals.
 * Cheap scopes: categories come from `utxo_total` (small), prefixes from the
 * outputs at a random transaction hash (output key), and each scope reads
 * its outputs by key or projection. Undefined when the node has no stored
 * set.
 */
export const spotCheckUtxoSet = async (
  client: Pick<ClickHouseClient, 'query'>,
  nodeId: number,
  options: UtxoSpotCheckOptions = {}
): Promise<UtxoSetSpotCheck | undefined> => {
  const snapshot = snapshotForNode(
    await readSnapshotMulti(client, [nodeId], { utxo: true }),
    nodeId
  );
  const mark = snapshot.utxo;
  if (mark === undefined) return undefined;
  const params = snapshotParams(snapshot);
  const withChanges = mark.lagCommits > 0;
  const random = options.random ?? Math.random;
  const settings = {
    max_execution_time: options.maxExecutionSeconds ?? 120,
  } as unknown as ClickHouseSettings;
  const zeroCategory = '00'.repeat(32);
  const categories =
    options.categoryValues ??
    (
      await client.query<{ c: string }>(
        `SELECT lower(hex(token_category)) AS c FROM (SELECT DISTINCT token_category FROM utxo_total
         WHERE node_internal_id = {node:UInt32} AND generation = {utxo_generation:UInt32}
           AND batch <= {utxo:UInt64} AND token_category != unhex({zero:String}))
         ORDER BY cityHash64(token_category, {seed:UInt64}) LIMIT {n:UInt32}`,
        {
          ...params,
          n: options.categories ?? 3,
          seed: Math.floor(random() * 2 ** 32),
          zero: zeroCategory,
        },
        settings
      )
    ).map((row) => row.c);
  const prefixCount = options.prefixes ?? 3;
  const prefixes =
    options.prefixValues ??
    (prefixCount <= 0
      ? []
      : (
          await client.query<{ p: string }>(
            `SELECT DISTINCT lower(hex(locking_bytecode_prefix)) AS p FROM (
               SELECT locking_bytecode_prefix FROM output WHERE transaction_hash >= unhex({from:String})
               ORDER BY transaction_hash LIMIT 64)
             LIMIT {n:UInt32}`,
            { from: randomHash(random), n: prefixCount },
            settings
          )
        ).map((row) => row.p));
  const scopes: UtxoSpotCheckScope[] = [];
  const skipped: UtxoSetSpotCheck['skipped'] = [];
  const compare = async (
    kind: UtxoSpotCheckScope['kind'],
    value: string,
    build: (scalar: ScalarRegistrar) => { stored: string; reference: string },
    extra: QueryParams = {}
  ) => {
    const scalars = scalarCollector();
    const { reference, stored } = build(scalars.register);
    try {
      const [row] = await client.query<{ s: string; r: string }>(
        `${scalars.withClause()}SELECT (${stored}) AS s, (${reference}) AS r`,
        { ...params, ...extra },
        settings
      );
      scopes.push({
        equal: row!.s === row!.r,
        kind,
        reference: row!.r,
        stored: row!.s,
        value,
      });
    } catch (error: unknown) {
      skipped.push({ error: String(error), kind, value });
    }
  };
  const change = (scalar: ScalarRegistrar) => ({
    from: '{utxo:UInt64}',
    scalar,
    snapshot,
  });
  for (const value of categories) {
    const where = 'token_category = unhex({spot:String})';
    await compare(
      'category',
      value,
      (scalar) => {
        const reference = unspentAtQueryTimeSql(snapshot, where);
        return {
          reference: `SELECT concat(r1.answer, '; ', r2.answer) FROM (${scopeDigest(
            reference
          )}) AS r1, (${totalsDigest(referenceTotals(reference))}) AS r2`,
          stored: `SELECT concat(s1.answer, '; ', s2.answer) FROM (${scopeDigest(
            utxoSetRowsSql({ change: change(scalar), where, withChanges })
          )}) AS s1, (${totalsDigest(
            utxoTotalsSql({
              category: 'unhex({spot:String})',
              change: change(scalar),
              withChanges,
            })
          )}) AS s2`,
        };
      },
      { spot: value }
    );
  }
  for (const value of prefixes) {
    const where = 'locking_bytecode_prefix = unhex({spot:String})';
    await compare(
      'prefix',
      value,
      (scalar) => ({
        reference: scopeDigest(unspentAtQueryTimeSql(snapshot, where)),
        stored: scopeDigest(
          utxoSetRowsSql({ change: change(scalar), where, withChanges })
        ),
      }),
      { spot: value }
    );
  }
  if (options.totals ?? true) {
    await compare('totals', '', (scalar) => ({
      reference: totalsDigest(referenceTotals(unspentAtQueryTimeSql(snapshot))),
      stored: totalsDigest(
        utxoTotalsSql({ change: change(scalar), withChanges })
      ),
    }));
  }
  return {
    generation: mark.generation,
    node: nodeId,
    scopes,
    skipped,
    visible: snapshot.visible,
    watermark: mark.watermark,
  };
};

/** Every published (U, generation) of a node at or below `upTo`, oldest first. */
export const publishedUtxoMarks = async (
  client: Pick<ClickHouseClient, 'query'>,
  nodeId: number,
  upTo: bigint
) =>
  (
    await client.query<{ w: string; g: number }>(
      `SELECT DISTINCT toString(watermark) AS w, generation AS g FROM utxo_watermark
       WHERE node_internal_id = {node:UInt32} AND watermark <= {upTo:UInt64}
       ORDER BY watermark, generation`,
      { node: nodeId, upTo }
    )
  ).map((row) => ({
    generation: Number(row.g),
    watermark: BigInt(row.w),
  }));

/**
 * The maintainer's counters in the Prometheus text format (the agent's
 * internal API serves them at `/metrics`).
 */
export const utxoSetMetricsText = (status: UtxoSetStatus) =>
  [
    [
      'chaingraph_utxo_set_spot_checks_total',
      'Spot-checks run (one per node per round).',
      status.spotChecks,
    ],
    [
      'chaingraph_utxo_set_spot_check_scopes_total',
      'Scopes compared by spot-checks.',
      status.spotCheckScopes,
    ],
    [
      'chaingraph_utxo_set_spot_check_mismatches_total',
      'Spot-check scopes where the stored UTXO set differed from the query-time answer (a data discrepancy).',
      status.spotCheckMismatches,
    ],
    [
      'chaingraph_utxo_set_spot_check_skipped_total',
      'Spot-check scopes not compared (statement over budget).',
      status.spotCheckSkipped,
    ],
    [
      'chaingraph_utxo_set_batches_total',
      'Batches published by the UTXO set maintainer.',
      status.batches,
    ],
  ]
    .map(
      ([name, help, value]) =>
        `# HELP ${String(name)} ${String(help)}\n# TYPE ${String(
          name
        )} counter\n${String(name)} ${String(value)}\n`
    )
    .join('');

/* ------------------------------------------------------------------ */
/* the maintainer                                                       */
/* ------------------------------------------------------------------ */

/** A progress report of the maintainer (logged by the store, `utxo_set_progress`). */
export interface UtxoSetProgress {
  event: 'utxo_set_progress';
  node: number;
  kind: 'batch' | 'build' | 'rebase';
  /** The published batch end. */
  watermark: string;
  /** visible(n) the batch was bounded by. */
  visible: string;
  generation: number;
  /** Committed commits of the node above the watermark (the lag). */
  lagCommits: number;
  /** Rows written (change rows, or the rebased / built set). */
  rows: number;
  batchMs: number;
}

/** A broken invariant (sum of signs not 0 or 1): a data discrepancy, reported loudly. */
export interface UtxoSetViolation {
  event: 'utxo_set_invariant_violation';
  node: number;
  generation: number;
  watermark: string;
  /** Outpoints whose sum of signs is not 0 or 1 (up to 10 shown). */
  count: number;
  examples: string[];
}

/**
 * A spot-check scope where the stored set and the query-time answer differ:
 * a data discrepancy, reported loudly (logged as an error).
 */
export interface UtxoSetSpotCheckMismatch {
  event: 'utxo_set_spot_check_mismatch';
  node: number;
  generation: number;
  watermark: string;
  visible: string;
  scope: UtxoSpotCheckScope['kind'];
  value: string;
  stored: string;
  reference: string;
}

/** The summary of one spot-check of one node (logged at info). */
export interface UtxoSetSpotCheckReport {
  event: 'utxo_set_spot_check';
  node: number;
  watermark: string;
  visible: string;
  scopes: number;
  mismatches: number;
  skipped: number;
  ms: number;
}

export interface UtxoSetNodeStatus {
  watermark: bigint;
  generation: number;
  visible: bigint;
  lagCommits: number;
}

export interface UtxoSetStatus {
  running: boolean;
  batches: number;
  rows: number;
  rebases: number;
  builds: number;
  byNode: Map<number, UtxoSetNodeStatus>;
  /** Spot-checks run (one per node per round; counter). */
  spotChecks: number;
  /** Scopes compared by spot-checks (counter). */
  spotCheckScopes: number;
  /** Scopes where the stored set differed from the query-time answer (counter; must stay 0). */
  spotCheckMismatches: number;
  /** Scopes not compared (statement over budget; counter). */
  spotCheckSkipped: number;
}

export interface UtxoSetOptions {
  /** Poll interval while caught up (default 250 ms). */
  intervalMs?: number;
  /** Batch bound: acceptance rows of the commits in one batch (default 200k; at least one commit). */
  maxRowsPerBatch?: number;
  /**
   * A node without a stored set whose visible(n) has at least this many
   * committed commits gets a bulk build (the set at visible(n), query
   * time, in hash slices) instead of replaying every commit (default 1,000;
   * 0 = always replay).
   */
  bulkBuildMinCommits?: number;
  /** The same by recorded acceptance rows (default 2M; a backfill is one huge commit). */
  bulkBuildMinRows?: number;
  /** Hash slices of a bulk build or rebase of a large set (default 16). */
  slices?: number;
  /**
   * A generation is rebased when its rows exceed max(this, (1 +
   * rebaseFactor) × its base rows) (default 1M): its change rows then
   * outnumber the set by that factor.
   */
  rebaseMinRows?: number;
  /** See `rebaseMinRows` (default 1). */
  rebaseFactor?: number;
  /**
   * ... or when its running-total rows (`utxo_total`: about one per batch
   * and token category touched) exceed this (default 2M): whole-set totals
   * read every row of the generation.
   */
  rebaseMaxTotalRows?: number;
  /** An old generation is dropped this long after its successor was published (default 10 min). */
  dropGraceMs?: number;
  /** Watermark rows older than this are pruned (the newest per node stays; default 1 h). */
  watermarkRetentionMs?: number;
  /** Progress reports at most this often (default 10 s). */
  reportIntervalMs?: number;
  /**
   * The periodic spot-check (owner decision 2026-10-12): every this many ms
   * each node with a stored set is compared with the query-time answer on a
   * few random scopes (`spotCheckUtxoSet`). Default 1 h; 0 = off.
   */
  spotCheckIntervalMs?: number;
  /** Scopes of each spot-check (`spotCheckUtxoSet` options). */
  spotCheck?: UtxoSpotCheckOptions;
  onProgress?: (progress: UtxoSetProgress) => void;
  onViolation?: (violation: UtxoSetViolation) => void;
  onSpotCheck?: (report: UtxoSetSpotCheckReport) => void;
  onSpotCheckMismatch?: (mismatch: UtxoSetSpotCheckMismatch) => void;
  onError?: (error: unknown) => void;
}

export interface UtxoSetContext {
  client: ClickHouseClient;
  /** Throws unless this process holds the writer lease. */
  assertHeld: () => void;
  epoch: () => bigint;
}

const sleep = async (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

/** Settings of every maintainer insert: totals deduplicated with the rows. */
const insertSettings = {
  deduplicate_blocks_in_dependent_materialized_views: 1,
} as const;

/** The published state of every node with a stored set: newest (U, generation). */
export const readUtxoSetMarks = async (
  client: Pick<ClickHouseClient, 'query'>
) => {
  const rows = await client.query<{
    node: number;
    watermark: string;
    generation: number;
    filled_at: string;
  }>(
    `SELECT node_internal_id AS node, toString(wg.1) AS watermark, wg.2 AS generation, toString(at) AS filled_at
     FROM (SELECT node_internal_id, max((watermark, generation)) AS wg,
             argMax(filled_at, (watermark, generation)) AS at
           FROM utxo_watermark GROUP BY node_internal_id)`
  );
  return new Map(
    rows.map((row) => [
      Number(row.node),
      {
        filledAt: row.filled_at,
        generation: Number(row.generation),
        watermark: BigInt(row.watermark),
      },
    ])
  );
};

const hashSlices = (count: number) => {
  if (count <= 1) return ['1'];
  return Array.from({ length: count }, (_, index) => {
    const lo = Math.floor((index * 256) / count);
    const hi = Math.floor(((index + 1) * 256) / count);
    return `reinterpretAsUInt8(substring(COLUMN, 1, 1)) >= ${lo}${
      hi >= 256
        ? ''
        : ` AND reinterpretAsUInt8(substring(COLUMN, 1, 1)) < ${hi}`
    }`;
  });
};

/** The (node, generation) partitions of a table with active parts. */
export const utxoSetPartitions = async (
  client: Pick<ClickHouseClient, 'query'>,
  table: 'utxo_set' | 'utxo_total'
) => {
  const rows = await client.query<{ partition: string; rows: string }>(
    `SELECT partition, toString(sum(rows)) AS rows FROM system.parts
     WHERE database = currentDatabase() AND table = {table:String} AND active
     GROUP BY partition`,
    { table }
  );
  return rows.flatMap((row) => {
    const match = /^\((?<node>\d+),\s*(?<generation>\d+)\)$/u.exec(
      row.partition
    );
    return match === null
      ? []
      : [
          {
            generation: Number(match.groups!.generation),
            node: Number(match.groups!.node),
            rows: Number(row.rows),
          },
        ];
  });
};

/** The node-agnostic gate parameters of a multi-node snapshot (the maintainer binds node and range itself). */
const agnosticParams = (snapshot: MultiNodeSnapshot) => ({
  fence: snapshot.fence,
  tail: snapshot.committedTail,
  visible0: snapshot.visible0,
  void: snapshot.void,
});

/** A bulk build takes one hash slice per this many acceptance rows of the node (memory bound). */
const buildSliceAcceptanceRows = 8_000_000;

const insertColumns = `node_internal_id, generation, batch, ${utxoSetOutputColumns.join(
  ', '
)}, sign`;

/** The background maintainer of one writer epoch. */
export class UtxoSetMaintainer {
  private stopped = false;

  private loop: Promise<void> | undefined;

  private wake: (() => void) | undefined;

  private lastReportAt = 0;

  private lastPruneAt = 0;

  /** Generations to drop: node → [generation, not before (ms)]. */
  private readonly pendingDrops: {
    node: number;
    generation: number;
    at: number;
  }[] = [];

  /** Base rows of each node's current generation (set at rebase / build; else read). */
  private readonly baseRows = new Map<number, number>();

  /** Testing: never advance past this seq (simulated lag). */
  holdAt: bigint | undefined;

  private readonly runId = `${Date.now().toString(36)}${Math.random()
    .toString(36)
    .slice(2, 8)}`;

  readonly status: UtxoSetStatus = {
    batches: 0,
    builds: 0,
    byNode: new Map(),
    rebases: 0,
    rows: 0,
    running: false,
    spotCheckMismatches: 0,
    spotCheckScopes: 0,
    spotCheckSkipped: 0,
    spotChecks: 0,
  };

  private lastSpotCheckAt = Date.now();

  private spotCheckRunning: Promise<void> | undefined;

  /**
   * Spot-check every node with a stored set now, each at one snapshot;
   * mismatches are counted and reported (`onSpotCheckMismatch`). Returns
   * the checks.
   */
  async spotCheck(): Promise<UtxoSetSpotCheck[]> {
    const { client } = this.context;
    const marks = await readUtxoSetMarks(client);
    const checks: UtxoSetSpotCheck[] = [];
    for (const node of [...marks.keys()].sort((a, b) => a - b)) {
      if (this.stopped && this.loop !== undefined) break;
      const started = Date.now();
      const check = await spotCheckUtxoSet(
        client,
        node,
        this.options.spotCheck
      );
      if (check === undefined) continue;
      checks.push(check);
      const mismatches = check.scopes.filter((scope) => !scope.equal);
      this.status.spotChecks += 1;
      this.status.spotCheckScopes += check.scopes.length;
      this.status.spotCheckMismatches += mismatches.length;
      this.status.spotCheckSkipped += check.skipped.length;
      mismatches.forEach((scope) => {
        this.options.onSpotCheckMismatch?.({
          event: 'utxo_set_spot_check_mismatch',
          generation: check.generation,
          node,
          reference: scope.reference,
          scope: scope.kind,
          stored: scope.stored,
          value: scope.value,
          visible: check.visible.toString(),
          watermark: check.watermark.toString(),
        });
      });
      this.options.onSpotCheck?.({
        event: 'utxo_set_spot_check',
        mismatches: mismatches.length,
        ms: Date.now() - started,
        node,
        scopes: check.scopes.length,
        skipped: check.skipped.length,
        visible: check.visible.toString(),
        watermark: check.watermark.toString(),
      });
    }
    return checks;
  }

  /** Start a spot-check round in the background when one is due (never two at once). */
  private maybeSpotCheck() {
    const interval = this.options.spotCheckIntervalMs ?? 3_600_000;
    if (interval <= 0 || this.spotCheckRunning !== undefined) return;
    if (Date.now() - this.lastSpotCheckAt < interval) return;
    this.lastSpotCheckAt = Date.now();
    this.spotCheckRunning = this.spotCheck()
      .then(() => undefined)
      .catch((error: unknown) => {
        this.options.onError?.(error);
      })
      .finally(() => {
        this.spotCheckRunning = undefined;
      });
  }

  constructor(
    private readonly context: UtxoSetContext,
    private readonly options: UtxoSetOptions = {}
  ) {}

  start() {
    if (this.loop !== undefined) return;
    this.stopped = false;
    this.status.running = true;
    this.loop = this.run().finally(() => {
      this.status.running = false;
    });
  }

  poke() {
    this.wake?.();
  }

  /** Stop after the current statement; resolves once the loop has ended. */
  async stop() {
    this.stopped = true;
    this.wake?.();
    await this.loop?.catch(() => undefined);
    await this.spotCheckRunning;
    this.loop = undefined;
  }

  /**
   * Stop the loop, bring every node up to its visible(n) as read now, then
   * resume the loop if it ran and `resume()`. Returns the marks reached.
   */
  async catchUpAndResume(resume: () => boolean) {
    const wasRunning = this.loop !== undefined && !this.stopped;
    await this.stop();
    try {
      return await this.catchUp();
    } finally {
      if (wasRunning && resume()) this.start();
    }
  }

  /** Bring every node up to its visible(n) as read now (the loop must not run). */
  async catchUp() {
    await this.removeUnfinished();
    for (;;) {
      /* the loop is stopped here: pass() must not take that as its own stop */
      const advanced = await this.pass(false);
      if (!advanced) break;
    }
    return readUtxoSetMarks(this.context.client);
  }

  /** Delete rows of batches and generations that never published their watermark. */
  async removeUnfinished() {
    const { client } = this.context;
    const marks = await readUtxoSetMarks(client);
    const partitions = [
      ...(await utxoSetPartitions(client, 'utxo_set')),
      ...(await utxoSetPartitions(client, 'utxo_total')),
    ];
    const nodes = [...new Set(partitions.map((partition) => partition.node))];
    for (const id of nodes) {
      const mark = marks.get(id);
      const condition =
        mark === undefined
          ? `node_internal_id = ${id}`
          : `node_internal_id = ${id} AND (generation > ${mark.generation} OR (generation = ${mark.generation} AND batch > ${mark.watermark}))`;
      /* a build's work rows (only a running build has any) */
      const [work] = await client.query<{ n: string }>(
        `SELECT toString(count()) AS n FROM utxo_build_spent WHERE node_internal_id = {node:UInt32}`,
        { node: id }
      );
      if (work !== undefined && work.n !== '0') {
        this.context.assertHeld();
        await client.command(
          `ALTER TABLE utxo_build_spent DELETE WHERE node_internal_id = {node:UInt32}`,
          { node: id },
          { mutations_sync: '2' }
        );
      }
      for (const table of ['utxo_set', 'utxo_total']) {
        const [row] = await client.query<{ n: string }>(
          `SELECT toString(count()) AS n FROM ${table} WHERE ${condition}`
        );
        if (row !== undefined && row.n !== '0') {
          this.context.assertHeld();
          await client.command(
            `ALTER TABLE ${table} DELETE WHERE ${condition}`,
            {},
            { mutations_sync: '2' }
          );
        }
      }
      /* generations older than the current one: dropped after the grace period */
      if (mark !== undefined) {
        const since = Date.parse(`${mark.filledAt.replace(' ', 'T')}Z`);
        const older = new Set(
          partitions
            .filter(
              (partition) =>
                partition.node === id && partition.generation < mark.generation
            )
            .map((partition) => partition.generation)
        );
        older.forEach((generation) => {
          if (
            this.pendingDrops.some(
              (drop) => drop.node === id && drop.generation === generation
            )
          ) {
            return;
          }
          this.pendingDrops.push({
            at:
              (Number.isNaN(since) ? Date.now() : since) +
              (this.options.dropGraceMs ?? 600_000),
            generation,
            node: id,
          });
        });
      }
    }
  }

  private async run() {
    const intervalMs = this.options.intervalMs ?? 250;
    try {
      await this.removeUnfinished();
    } catch (error: unknown) {
      this.options.onError?.(error);
    }
    while (!this.stopped) {
      try {
        const advanced = await this.pass(true);
        await this.housekeeping();
        this.maybeSpotCheck();
        if (advanced) continue;
      } catch (error: unknown) {
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
        if (this.stopped) return;
        this.options.onError?.(error);
        await sleep(intervalMs * 4);
        try {
          await this.removeUnfinished();
        } catch (cleanupError: unknown) {
          this.options.onError?.(cleanupError);
        }
        continue;
      }
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, intervalMs);
        this.wake = () => {
          clearTimeout(timer);
          resolve();
        };
      });
      this.wake = undefined;
    }
  }

  /** One step for every node that lags; true if any node advanced. */
  private async pass(loop: boolean): Promise<boolean> {
    const { client } = this.context;
    const nodeRows = await client.query<{ node: number }>(
      `SELECT DISTINCT node_internal_id AS node FROM visibility WHERE node_internal_id != ${nodeAgnosticId} ORDER BY node`
    );
    const nodes = nodeRows.map((row) => Number(row.node));
    if (nodes.length === 0) return false;
    const snapshot = await readSnapshotMulti(client, nodes);
    const marks = await readUtxoSetMarks(client);
    let advanced = false;
    for (const node of nodes) {
      if (loop && this.stopped) break;
      let visible = snapshot.visibleByNode.get(node) ?? 0n;
      if (this.holdAt !== undefined && visible > this.holdAt) {
        visible = this.holdAt;
      }
      const mark = marks.get(node);
      if (mark === undefined) {
        if (visible === 0n) continue;
        const stats = await this.commitStats(node, 0n, visible);
        if (stats.commits === 0) continue;
        const minCommits = this.options.bulkBuildMinCommits ?? 1_000;
        const minRows = this.options.bulkBuildMinRows ?? 2_000_000;
        if (
          minCommits > 0 &&
          (stats.commits >= minCommits || stats.rows >= minRows)
        ) {
          await this.build(node, visible, snapshot, stats.rows);
          advanced = true;
          continue;
        }
        const first = await this.batch(
          node,
          { generation: 1, watermark: 0n },
          visible,
          snapshot
        );
        advanced ||= first !== 0n;
        continue;
      }
      if (visible > mark.watermark) {
        const next = await this.batch(node, mark, visible, snapshot);
        if (next === mark.watermark) continue;
        advanced = true;
        await this.maybeRebase(node, {
          generation: mark.generation,
          watermark: next,
        });
      } else {
        this.status.byNode.set(node, {
          generation: mark.generation,
          lagCommits: 0,
          visible,
          watermark: mark.watermark,
        });
      }
    }
    return advanced;
  }

  /** Committed commits of the node in (from, to] and their recorded acceptance rows. */
  private async commitStats(node: number, from: bigint, to: bigint) {
    const [row] = await this.context.client.query<{ n: string; r: string }>(
      `SELECT toString(count()) AS n,
         toString(sum(row_counts['tx_acceptance'] + row_counts['node_transaction'] + row_counts['node_block'])) AS r
       FROM commit_log FINAL
       WHERE commit_seq > {from:UInt64} AND commit_seq <= {to:UInt64} AND state = 'committed'
         AND has(node_scope, {node:UInt32})`,
      { from, node, to }
    );
    return { commits: Number(row?.n ?? '0'), rows: Number(row?.r ?? '0') };
  }

  private token(step: string) {
    return `utxo:${this.runId}:${step}`;
  }

  private async publish(
    node: number,
    row: {
      watermark: bigint;
      generation: number;
      from: bigint;
      rows: number;
      kind: 'batch' | 'build' | 'rebase';
    }
  ) {
    this.context.assertHeld();
    await this.context.client.insertSelect(
      `INSERT INTO utxo_watermark (node_internal_id, watermark, generation, batch_from, rows, kind, writer_epoch, filled_at)
       SELECT {node:UInt32}, {watermark:UInt64}, {generation:UInt32}, {from:UInt64}, {rows:Int64}, {kind:String},
         {epoch:UInt64}, now64(3)`,
      {
        epoch: this.context.epoch(),
        from: row.from,
        generation: row.generation,
        kind: row.kind,
        node,
        rows: row.rows,
        watermark: row.watermark,
      },
      {
        deduplicationToken: this.token(
          `w:${node}:${row.generation}:${row.watermark}:${row.kind}`
        ),
      }
    );
  }

  private report(progress: Omit<UtxoSetProgress, 'event'>) {
    const now = Date.now();
    const interval = this.options.reportIntervalMs ?? 10_000;
    if (
      this.options.onProgress !== undefined &&
      (now - this.lastReportAt >= interval || progress.kind !== 'batch')
    ) {
      this.lastReportAt = now;
      this.options.onProgress({ event: 'utxo_set_progress', ...progress });
    }
  }

  /** Apply the next batch after `mark` (bounded by `visible`); returns its end. */
  private async batch(
    node: number,
    mark: { generation: number; watermark: bigint },
    visible: bigint,
    snapshot: MultiNodeSnapshot
  ): Promise<bigint> {
    const { client } = this.context;
    const started = Date.now();
    const from = mark.watermark;
    const commits = (
      await client.query<{ seq: string; rows: string }>(
        `SELECT toString(commit_seq) AS seq,
           toString(row_counts['tx_acceptance'] + row_counts['node_transaction'] + row_counts['node_block'] + 1) AS rows
         FROM commit_log FINAL
         WHERE commit_seq > {from:UInt64} AND commit_seq <= {visible:UInt64} AND state = 'committed'
           AND has(node_scope, {node:UInt32})
         ORDER BY commit_seq`,
        { from, node, visible }
      )
    ).map((row) => ({ rows: Number(row.rows), seq: BigInt(row.seq) }));
    if (commits.length === 0) {
      /*
       * No commit of the node in (U, visible(n)]: its set at visible(n) is
       * the set at U. Nothing is published (readers see lagCommits = 0 and
       * add no correction), so an idle node writes no watermark rows.
       */
      this.status.byNode.set(node, {
        generation: mark.generation,
        lagCommits: 0,
        visible,
        watermark: from,
      });
      return from;
    }
    const to = nextBatchEnd(
      from,
      visible,
      commits,
      this.options.maxRowsPerBatch ?? 200_000
    );
    let rows = 0;
    if (commits.some((commit) => commit.seq <= to)) {
      const ctes = scalarCollector();
      const change = utxoChangeSql({
        from: '{from:UInt64}',
        scalar: ctes.register,
        snapshot,
        to: 'to',
      });
      this.context.assertHeld();
      const summary = await client.insertSelectCounted(
        `INSERT INTO utxo_set (${insertColumns})
         ${ctes.withClause()}SELECT {node:UInt32}, {generation:UInt32}, {to:UInt64}, ${utxoSetOutputColumns.join(
          ', '
        )}, sign
         FROM (${change})`,
        {
          ...agnosticParams(snapshot),
          from,
          generation: mark.generation,
          node,
          to,
        },
        {
          deduplicationToken: this.token(`b:${node}:${mark.generation}:${to}`),
          settings: insertSettings,
        }
      );
      rows =
        summary.writtenRows === 0
          ? 0
          : await this.batchRows(node, mark.generation, to);
    }
    await this.publish(node, {
      from,
      generation: mark.generation,
      kind: 'batch',
      rows,
      watermark: to,
    });
    const lagCommits = commits.filter((commit) => commit.seq > to).length;
    this.status.batches += 1;
    this.status.rows += rows;
    this.status.byNode.set(node, {
      generation: mark.generation,
      lagCommits,
      visible,
      watermark: to,
    });
    this.report({
      batchMs: Date.now() - started,
      generation: mark.generation,
      kind: 'batch',
      lagCommits,
      node,
      rows,
      visible: visible.toString(),
      watermark: to.toString(),
    });
    return to;
  }

  /** `utxo_set` rows a batch (or build / rebase) wrote (the insert summary also counts `utxo_total`). */
  private async batchRows(node: number, generation: number, batch: bigint) {
    const [row] = await this.context.client.query<{ n: string }>(
      `SELECT toString(count()) AS n FROM utxo_set
       WHERE node_internal_id = {node:UInt32} AND generation = {generation:UInt32} AND batch = {batch:UInt64}`,
      { batch, generation, node }
    );
    return Number(row?.n ?? '0');
  }

  /**
   * The node's set at `visible`, computed at query time, as a new
   * generation. Memory-bounded in two phases over hash slices (the build of
   * a mainnet-like database must never hold the node's whole accepted set or
   * its whole spent set at once):
   * 1. per slice of SPENDER hashes: the outpoints spent by the node's
   *    accepted transactions of the slice, into the work table
   *    `utxo_build_spent` (inputs by their primary key, the slice's
   *    collapsed acceptance rows as the set);
   * 2. per slice of CREATOR hashes: the outputs of the node's accepted
   *    transactions of the slice, minus the spent outpoints of the slice
   *    (a hash anti-join of one slice of the work table).
   * Slices: `slices` (default 16), more for a large node (one per ~8 M
   * acceptance rows, at most 256).
   */
  private async build(
    node: number,
    visible: bigint,
    snapshot: MultiNodeSnapshot,
    acceptanceRows = 0
  ) {
    const { client } = this.context;
    const started = Date.now();
    const [last] = await client.query<{ g: number }>(
      `SELECT max(generation) AS g FROM utxo_set WHERE node_internal_id = {node:UInt32}`,
      { node }
    );
    const generation = Number(last?.g ?? 0) + 1;
    const acceptance = pinnedGateTerms('node', {
      params: { node: 'node', visible: 'to' },
      snapshot,
    }).join(' AND ');
    const agnostic = pinnedGateTerms('agnostic', { snapshot }).join(' AND ');
    const accepted = (
      slice: string
    ) => `SELECT transaction_hash FROM tx_acceptance
      WHERE ${slice.replaceAll('COLUMN', 'transaction_hash')} AND ${acceptance}
      GROUP BY transaction_hash, node_internal_id, block_internal_id HAVING sum(sign) > 0`;
    const sliceCount = Math.min(
      256,
      Math.max(
        this.options.slices ?? 16,
        Math.ceil(acceptanceRows / buildSliceAcceptanceRows)
      )
    );
    const slices = hashSlices(sliceCount);
    const params = {
      ...agnosticParams(snapshot),
      generation,
      node,
      to: visible,
    };
    await client.command(
      `ALTER TABLE utxo_build_spent DELETE WHERE node_internal_id = {node:UInt32}`,
      { node },
      { mutations_sync: '2' }
    );
    for (const [index, slice] of slices.entries()) {
      this.context.assertHeld();
      await client.insertSelect(
        `INSERT INTO utxo_build_spent (node_internal_id, generation, outpoint_transaction_hash, outpoint_index)
         SELECT {node:UInt32}, {generation:UInt32}, outpoint_transaction_hash, outpoint_index FROM input
         WHERE ${slice.replaceAll('COLUMN', 'transaction_hash')}
           AND outpoint_transaction_hash != ${zeroHash} AND ${agnostic}
           AND transaction_hash IN (${accepted(slice)})`,
        params,
        {
          deduplicationToken: this.token(
            `build-spent:${node}:${generation}:${visible}:${index}`
          ),
        }
      );
    }
    let rows = 0;
    for (const [index, slice] of slices.entries()) {
      this.context.assertHeld();
      const summary = await client.insertSelectCounted(
        `INSERT INTO utxo_set (${insertColumns})
         SELECT {node:UInt32}, {generation:UInt32}, {to:UInt64}, ${utxoSetOutputColumns
           .map((column) => `o.${column}`)
           .join(', ')}, toInt8(1)
         FROM (SELECT ${utxoSetOutputColumns.join(', ')} FROM output
               WHERE ${slice.replaceAll(
                 'COLUMN',
                 'transaction_hash'
               )} AND ${agnostic}
                 AND transaction_hash IN (${accepted(slice)})
               LIMIT 1 BY transaction_hash, output_index) AS o
         LEFT ANTI JOIN (SELECT outpoint_transaction_hash, outpoint_index FROM utxo_build_spent
                         WHERE node_internal_id = {node:UInt32} AND generation = {generation:UInt32}
                           AND ${slice.replaceAll(
                             'COLUMN',
                             'outpoint_transaction_hash'
                           )}) AS s
           ON s.outpoint_transaction_hash = o.transaction_hash AND s.outpoint_index = o.output_index`,
        params,
        {
          deduplicationToken: this.token(
            `build:${node}:${generation}:${visible}:${index}`
          ),
          settings: insertSettings,
        }
      );
      rows += summary.writtenRows;
    }
    await client.command(
      `ALTER TABLE utxo_build_spent DROP PARTITION ({node:UInt32}, {generation:UInt32})`,
      { generation, node }
    );
    rows = rows === 0 ? 0 : await this.batchRows(node, generation, visible);
    await this.publish(node, {
      from: 0n,
      generation,
      kind: 'build',
      rows,
      watermark: visible,
    });
    this.baseRows.set(node, rows);
    this.status.builds += 1;
    this.status.byNode.set(node, {
      generation,
      lagCommits: 0,
      visible,
      watermark: visible,
    });
    this.report({
      batchMs: Date.now() - started,
      generation,
      kind: 'build',
      lagCommits: 0,
      node,
      rows,
      visible: visible.toString(),
      watermark: visible.toString(),
    });
  }

  /** Rows of the generation's base (its build or rebase), 0 for a replayed first generation. */
  private async generationBaseRows(node: number, generation: number) {
    const known = this.baseRows.get(node);
    if (known !== undefined && known >= 0) return known;
    const [row] = await this.context.client.query<{ rows: string }>(
      `SELECT toString(max(rows)) AS rows FROM utxo_watermark
       WHERE node_internal_id = {node:UInt32} AND generation = {generation:UInt32} AND kind != 'batch'`,
      { generation, node }
    );
    const rows = Number(row?.rows ?? '0');
    this.baseRows.set(node, rows);
    return rows;
  }

  /**
   * Rebase the node's set at `mark` into a new generation when the
   * generation's rows exceed max(rebaseMinRows, (1 + rebaseFactor) × its
   * base rows) (the change rows then outnumber the set by that factor) or
   * its running-total rows exceed rebaseMaxTotalRows.
   */
  private async maybeRebase(
    node: number,
    mark: { generation: number; watermark: bigint }
  ) {
    const partition = (
      await utxoSetPartitions(this.context.client, 'utxo_set')
    ).find(
      (candidate) =>
        candidate.node === node && candidate.generation === mark.generation
    );
    const total = partition?.rows ?? 0;
    const totals =
      (await utxoSetPartitions(this.context.client, 'utxo_total')).find(
        (candidate) =>
          candidate.node === node && candidate.generation === mark.generation
      )?.rows ?? 0;
    const base = await this.generationBaseRows(node, mark.generation);
    const bound = Math.max(
      this.options.rebaseMinRows ?? 1_000_000,
      (1 + (this.options.rebaseFactor ?? 1)) * base
    );
    if (
      total <= bound &&
      totals <= (this.options.rebaseMaxTotalRows ?? 2_000_000)
    ) {
      return;
    }
    await this.rebase(node, mark);
  }

  /**
   * The set at `mark` as generation + 1 (sign +1, batch = U). Checks the
   * invariant first: an outpoint with a sum of signs other than 0 or 1 is a
   * data discrepancy (reported, no rebase).
   */
  async rebase(node: number, mark: { generation: number; watermark: bigint }) {
    const { client } = this.context;
    const started = Date.now();
    const params = {
      generation: mark.generation,
      node,
      watermark: mark.watermark,
    };
    const scope = `node_internal_id = {node:UInt32} AND generation = {generation:UInt32} AND batch <= {watermark:UInt64}`;
    const violations = await client.query<{ key: string }>(
      `SELECT concat(lower(hex(transaction_hash)), ':', toString(output_index), ' sum ', toString(sum(sign))) AS key
       FROM utxo_set WHERE ${scope}
       GROUP BY transaction_hash, output_index HAVING sum(sign) NOT IN (0, 1)
       LIMIT 11`,
      params
    );
    if (violations.length > 0) {
      this.options.onViolation?.({
        count: violations.length,
        event: 'utxo_set_invariant_violation',
        examples: violations.slice(0, 10).map((row) => row.key),
        generation: mark.generation,
        node,
        watermark: mark.watermark.toString(),
      });
      return;
    }
    const generation = mark.generation + 1;
    const values = utxoSetOutputColumns
      .filter(
        (column) => column !== 'transaction_hash' && column !== 'output_index'
      )
      .map((column) => `any(${column})`);
    let rows = 0;
    const slices = hashSlices(this.options.slices ?? 16);
    for (const [index, slice] of slices.entries()) {
      this.context.assertHeld();
      const summary = await client.insertSelectCounted(
        `INSERT INTO utxo_set (node_internal_id, generation, batch, transaction_hash, output_index, ${utxoSetOutputColumns
          .filter(
            (column) =>
              column !== 'transaction_hash' && column !== 'output_index'
          )
          .join(', ')}, sign)
         SELECT {node:UInt32}, {next:UInt32}, {watermark:UInt64}, transaction_hash, output_index, ${values.join(
           ', '
         )}, toInt8(1)
         FROM utxo_set WHERE ${scope} AND ${slice.replaceAll(
          'COLUMN',
          'transaction_hash'
        )}
         GROUP BY transaction_hash, output_index HAVING sum(sign) = 1`,
        { ...params, next: generation },
        {
          deduplicationToken: this.token(
            `rebase:${node}:${generation}:${mark.watermark}:${index}`
          ),
          settings: insertSettings,
        }
      );
      rows += summary.writtenRows;
    }
    rows =
      rows === 0 ? 0 : await this.batchRows(node, generation, mark.watermark);
    await this.publish(node, {
      from: mark.watermark,
      generation,
      kind: 'rebase',
      rows,
      watermark: mark.watermark,
    });
    this.baseRows.set(node, rows);
    this.pendingDrops.push({
      at: Date.now() + (this.options.dropGraceMs ?? 600_000),
      generation: mark.generation,
      node,
    });
    this.status.rebases += 1;
    this.report({
      batchMs: Date.now() - started,
      generation,
      kind: 'rebase',
      lagCommits: 0,
      node,
      rows,
      visible: mark.watermark.toString(),
      watermark: mark.watermark.toString(),
    });
  }

  /** Drop old generations past their grace period; prune old watermark rows. */
  private async housekeeping() {
    const { client } = this.context;
    const now = Date.now();
    const due = this.pendingDrops.filter((drop) => drop.at <= now);
    for (const drop of due) {
      this.context.assertHeld();
      for (const table of ['utxo_set', 'utxo_total']) {
        await client.command(
          `ALTER TABLE ${table} DROP PARTITION (${drop.node}, ${drop.generation})`
        );
      }
      /* its marks too: no snapshot can name a dropped generation (a fresh one never would: a newer generation's mark at the same U is greater) */
      await client.command(
        `DELETE FROM utxo_watermark WHERE node_internal_id = {node:UInt32} AND generation = {generation:UInt32}`,
        { generation: drop.generation, node: drop.node }
      );
      this.pendingDrops.splice(this.pendingDrops.indexOf(drop), 1);
    }
    const retention = this.options.watermarkRetentionMs ?? 3_600_000;
    if (now - this.lastPruneAt >= Math.min(retention, 600_000)) {
      this.lastPruneAt = now;
      const [old] = await client.query<{ n: string }>(
        `SELECT toString(count()) AS n FROM utxo_watermark
         WHERE kind = 'batch' AND filled_at < now64(3) - toIntervalMillisecond({retention:UInt64})
           AND (node_internal_id, watermark, generation) NOT IN (
             SELECT node_internal_id, wg.1, wg.2 FROM (
               SELECT node_internal_id, max((watermark, generation)) AS wg FROM utxo_watermark GROUP BY node_internal_id))`,
        { retention }
      );
      if (old !== undefined && Number(old.n) > 1_000) {
        this.context.assertHeld();
        await client.command(
          `DELETE FROM utxo_watermark
           WHERE kind = 'batch' AND filled_at < now64(3) - toIntervalMillisecond({retention:UInt64})
             AND (node_internal_id, watermark, generation) NOT IN (
               SELECT node_internal_id, wg.1, wg.2 FROM (
                 SELECT node_internal_id, max((watermark, generation)) AS wg FROM utxo_watermark GROUP BY node_internal_id))`,
          { retention }
        );
      }
    }
  }
}
