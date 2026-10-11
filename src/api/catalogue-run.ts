/* eslint-disable functional/no-loop-statement, no-await-in-loop, no-console, @typescript-eslint/no-magic-numbers, complexity */
// cspell:ignore clickhouse chipnet
/**
 * Run the v2 document catalogue on chipnet (catalogue.ts) and record each
 * document's status, equality outcome and latency.
 *
 *   CHAINGRAPH_CLICKHOUSE_URL=http://localhost:18123 CHAINGRAPH_CLICKHOUSE_DATABASE=chipnet_ch_pp2 \
 *   CHAINGRAPH_API_V1_URL=http://localhost:18080/v1/graphql CHAINGRAPH_API_V1_ADMIN_SECRET=… \
 *   node build/api/catalogue-run.js [--latency 9] [--update-headers] [ids…]
 *
 * Results (raw, not committed) go to CHAINGRAPH_API_CATALOGUE_OUT (default
 * /tmp/chaingraph-v2-catalogue.json); `--update-headers` rewrites the
 * status / equality / latency header lines of each document file.
 */
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

import { ClickHouseClient } from '../store/clickhouse/client.js';

import type { CatalogueResult } from './catalogue.js';
import { catalogueDirectory, loadCatalogue, runEntry } from './catalogue.js';
import { createApi } from './server.js';

const args = process.argv.slice(2);
const flag = (name: string) => args.includes(name);
const option = (name: string) => {
  const index = args.indexOf(name);
  return index === -1 ? undefined : args[index + 1];
};
const latencyRuns = Number(option('--latency') ?? 0);
const ids = args.filter(
  (arg, index) => !arg.startsWith('--') && args[index - 1] !== '--latency'
);

const header = (result: CatalogueResult, expect: string | undefined) => {
  const status =
    result.status === 'works'
      ? 'works'
      : `blocked (v2 error: ${result.error ?? ''})`;
  const latency =
    result.v2P50 === undefined
      ? ''
      : `\n# latency (${
          process.env.CHAINGRAPH_API_CATALOGUE_LABEL ??
          process.env.CHAINGRAPH_CLICKHOUSE_DATABASE ??
          'chipnet'
        }, p50 warm, first page): v2 ${result.v2P50.toFixed(
          1
        )} ms; v1 Hasura/Postgres ${result.v1P50?.toFixed(1) ?? '?'} ms`;
  return `# status: ${status}\n# chipnet equality: ${result.equality} (v1 ${
    result.v1Rows
  } rows in ${result.v1Pages} pages, v2 ${result.v2Rows} rows in ${
    result.v2Pages
  } pages)${expect === undefined ? '' : `; expected: ${expect}`}${latency}`;
};

const main = async () => {
  const client = ClickHouseClient.fromEnv();
  const api = createApi(client);
  const endpoint = {
    adminSecret: process.env.CHAINGRAPH_API_V1_ADMIN_SECRET ?? '',
    url:
      process.env.CHAINGRAPH_API_V1_URL ?? 'http://localhost:18080/v1/graphql',
  };
  const catalogue = loadCatalogue().filter(
    (entry) => ids.length === 0 || ids.includes(entry.id)
  );
  const results: CatalogueResult[] = [];
  for (const entry of catalogue) {
    const result = await runEntry(api, endpoint, entry, { latencyRuns });
    results.push(result);
    console.log(
      [
        entry.id.padEnd(4),
        entry.name.padEnd(40),
        result.status.padEnd(6),
        result.equality.slice(0, 140),
        `${result.v1Rows}/${result.v2Rows} rows`,
        result.v2P50 === undefined
          ? ''
          : `v2 ${result.v2P50.toFixed(1)} ms, v1 ${
              result.v1P50?.toFixed(1) ?? ''
            } ms`,
      ].join(' | ')
    );
    if (result.error !== undefined) console.log(`     ${result.error}`);
    if (flag('--update-headers')) {
      const path = join(catalogueDirectory, entry.file);
      const lines = readFileSync(path, 'utf8').split('\n');
      const body = lines.filter(
        (line, index) => index === 0 || !line.startsWith('#')
      );
      writeFileSync(
        path,
        [body[0], header(result, entry.expect), ...body.slice(1)].join('\n')
      );
    }
  }
  const out =
    process.env.CHAINGRAPH_API_CATALOGUE_OUT ??
    '/tmp/chaingraph-v2-catalogue.json';
  writeFileSync(out, JSON.stringify(results, null, 1));
  console.log(`results: ${out}`);
  await client.close();
};

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
