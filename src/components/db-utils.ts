/* eslint-disable camelcase, @typescript-eslint/naming-convention */
export const indexDefinitions = {
  block_height_index: /* sql */ `CREATE INDEX block_height_index ON block USING btree (height);`,
  block_inclusions_index: /* sql */ `CREATE INDEX block_inclusions_index ON block_transaction USING btree (transaction_internal_id);`,
  output_acceptance_index: /* sql */ `CREATE INDEX output_acceptance_index ON output USING gin (accepted_node_ids);`,
  /**
   * The first 25 bytes of each locking bytecode (PostgreSQL positions start at
   * 1). Enough for complete P2PKH and P2SH20 bytecode; longer bytecode is
   * rechecked against the full value by `search_output`.
   */
  output_search_index: /* sql */ `CREATE INDEX output_search_index ON output USING btree (substring(locking_bytecode from 1 for 25));`,
  spent_by_index: /* sql */ `CREATE INDEX spent_by_index ON input USING btree (outpoint_transaction_hash, outpoint_index);`,
  token_category_index: /* sql */ `CREATE INDEX token_category_index ON output USING btree (token_category);`,
  unspent_output_category_index: /* sql */ `CREATE INDEX unspent_output_category_index ON output USING btree (token_category) WHERE cardinality(unspent_node_ids) > 0 AND token_category IS NOT NULL;`,
  unspent_output_index: /* sql */ `CREATE INDEX unspent_output_index ON output USING gin (unspent_node_ids);`,
  unspent_output_search_index: /* sql */ `CREATE INDEX unspent_output_search_index ON output USING btree (substring(locking_bytecode from 1 for 25)) WHERE cardinality(unspent_node_ids) > 0;`,
};
/* eslint-enable camelcase, @typescript-eslint/naming-convention */

/** Array indexes are built by the fenced finalizer, after deferred ingestion. */
export const managedIndexesForMembershipMode = (
  mode: 'baseline' | 'deferred' | 'incremental'
): (keyof typeof indexDefinitions)[] => {
  const arrayIndexes: (keyof typeof indexDefinitions)[] = [
    'output_acceptance_index',
    'unspent_output_category_index',
    'unspent_output_index',
    'unspent_output_search_index',
  ];
  return (
    Object.keys(indexDefinitions) as (keyof typeof indexDefinitions)[]
  ).filter((name) => mode !== 'deferred' || !arrayIndexes.includes(name));
};

/**
 * Based on the typical Chaingraph workload, the table scanning phase of index
 * building is counted as 40% of progress, while the 'loading tuples in tree'
 * phase is counted as the remaining 60%. Because there is a pause between these
 * step which can't be measured, this method will briefly return `40` until
 * tuple loading has begun.
 */
export const computeIndexCreationProgress = (
  progressQueryResult: {
    query: string;
    /* eslint-disable @typescript-eslint/naming-convention */
    blocks_done: string;
    blocks_total: string;
    tuples_done: string;
    tuples_total: string;
    /* eslint-enable @typescript-eslint/naming-convention */
  }[]
) => {
  const tableScanningPhasePercent = 40;
  const tupleLoadingPhasePercent = 60;
  return progressQueryResult
    .map((row) => {
      const indexName = Object.keys(indexDefinitions).find((name) =>
        row.query.includes(name)
      );
      if (indexName === undefined) {
        return undefined;
      }
      const rawResult =
        row.tuples_total === '0'
          ? Math.round(
              (Number(row.blocks_done) / Number(row.blocks_total)) *
                tableScanningPhasePercent
            ).toString()
          : row.tuples_done === '0'
          ? '40'
          : Math.round(
              (Number(row.tuples_done) / Number(row.tuples_total)) *
                tupleLoadingPhasePercent +
                tableScanningPhasePercent
            ).toString();
      return [indexName, rawResult === 'NaN' ? '0' : rawResult] as [
        keyof typeof indexDefinitions,
        string
      ];
    })
    .filter(
      (progressItem): progressItem is [keyof typeof indexDefinitions, string] =>
        progressItem !== undefined
    );
};
