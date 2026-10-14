// Capture the data statements (SQL + params) of one document. usage: node capture.mjs <wt> <db> <docs.json> <name> <outPrefix>
import { readFileSync, writeFileSync } from 'node:fs';
const [wt, db, docsFile, name, out] = process.argv.slice(2);
const { ClickHouseClient } = await import(`${wt}/build/store/clickhouse/client.js`);
const { createApi } = await import(`${wt}/build/api/server.js`);
const { disabledAdmissionOptions } = await import(`${wt}/build/api/admission.js`);
const d = JSON.parse(readFileSync(docsFile, 'utf8'))[name];
const client = new ClickHouseClient({ url: 'http://localhost:18123', database: db, username: 'default', password: '', requestTimeoutMs: 900000 });
const api = createApi(client, { admission: disabledAdmissionOptions, ...(d.options ?? {}) });
const events = [];
api.db.onQuery = (e) => { if (e.kind === 'data') events.push(e); };
const query = readFileSync(`${wt}/docs/clickhouse-port/v2-documents/${d.file}`, 'utf8').replace(/^subscription /mu, 'query ');
const r = await api.yoga.fetch('http://localhost/graphql', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query, variables: d.variables }) });
const body = await r.json();
if (body.errors) console.log(JSON.stringify(body.errors).slice(0, 500));
const big = (k, v) => (typeof v === 'bigint' ? v.toString() : v);
events.forEach((e, i) => {
  writeFileSync(`${out}-${i}.sql`, e.sql);
  writeFileSync(`${out}-${i}.params.json`, JSON.stringify(e.params, big));
  console.log(`${out}-${i}`, e.label, e.sql.length);
});
await client.close();
process.exit(0);
