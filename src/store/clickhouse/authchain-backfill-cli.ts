/* eslint-disable @typescript-eslint/no-magic-numbers */
// cspell:ignore clickhouse authchain
/**
 * Backfill the authchain index (ddl/080_authchain.sql) of a database whose
 * spend index predates it (`bin/chaingraph-clickhouse-authchain-backfill.js`):
 * places the output-0 spenders of every finished spend batch, in batch
 * order, on CHAINGRAPH_CLICKHOUSE_URL / _DATABASE. Idempotent; safe while the
 * agent runs (the DDL must be applied first).
 */
import { backfillAuthchainLinks } from './authchain-index.js';
import { ClickHouseClient } from './client.js';

const log = (message: string) => {
  // eslint-disable-next-line no-console
  console.log(`authchain-backfill: ${message}`);
};

const main = async () => {
  const client = ClickHouseClient.fromEnv();
  const started = Date.now();
  const reported = { at: 0 };
  const result = await backfillAuthchainLinks(client, {
    onProgress: (progress) => {
      if (Date.now() - reported.at < 10_000 && progress.batches < progress.of) {
        return;
      }
      reported.at = Date.now();
      log(
        `${progress.batches}/${progress.of} batches, ${progress.rows} rows (batch ${progress.batch})`
      );
    },
  });
  log(
    `done: ${result.batches} batches, ${result.rows} rows in ${Math.round(
      (Date.now() - started) / 1000
    )} s`
  );
  await client.close();
};

main().catch((error: unknown) => {
  // eslint-disable-next-line no-console
  console.error(error);
  process.exitCode = 1;
});
