// In-process API probe on a database: per document p50 wall (response cache off) and per-statement query_log
// stats (median over the measured runs). usage: node probe.mjs <worktree> <db> <runs> <label> <docs.json> [names,...]
import { readFileSync } from 'node:fs';
const [wt, db, runs = '5', label = '', docsFile, onlyArg] = process.argv.slice(2);
const { ClickHouseClient } = await import(`${wt}/build/store/clickhouse/client.js`);
const { createApi } = await import(`${wt}/build/api/server.js`);
const { disabledAdmissionOptions } = await import(`${wt}/build/api/admission.js`);
const docs = JSON.parse(readFileSync(docsFile, 'utf8'));
const only = onlyArg?.split(',');
const admin = new ClickHouseClient({ url: 'http://localhost:18123', database: db, username: 'default', password: '', requestTimeoutMs: 900000 });
const median = (xs) => { const s = [...xs].sort((a, b) => a - b); return s[Math.floor(s.length / 2)]; };
for (const [name, d] of Object.entries(docs)) {
  if (only && !only.includes(name)) continue;
  const tag = `fix6:${label}:${name}:${Date.now()}`;
  const client = new ClickHouseClient({ url: 'http://localhost:18123', database: db, username: 'default', password: '', requestTimeoutMs: 900000 });
  const api = createApi(client, { admission: disabledAdmissionOptions, ...(d.options ?? {}) });
  const query = readFileSync(`${wt}/docs/clickhouse-port/v2-documents/${d.file}`, 'utf8').replace(/^subscription /mu, 'query ');
  const run = async () => {
    const t0 = performance.now();
    const r = await api.yoga.fetch('http://localhost/graphql', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query, variables: d.variables }) });
    const body = await r.json();
    if (body.errors) throw new Error(JSON.stringify(body.errors).slice(0, 400));
    return { ms: performance.now() - t0, body };
  };
  const times = []; let last;
  try { await run(); } catch (error) { console.log(JSON.stringify({ label, name, error: String(error).slice(0, 400) })); await client.close(); continue; }
  const t1 = Date.now();
  for (let i = 0; i < Number(runs); i++) { last = await run(); times.push(last.ms); }
  const t2 = Date.now();
  await admin.command('SYSTEM FLUSH LOGS');
  const rows = await admin.query(`SELECT normalizedQueryHash(query) AS h, any(substring(query, 1, 60)) AS q, count() AS n,
      groupArray(query_duration_ms) AS wall, groupArray(read_rows) AS rr, groupArray(read_bytes) AS rb,
      groupArray(ProfileEvents['UserTimeMicroseconds']) AS cpu, groupArray(ProfileEvents['FilteringMarksWithPrimaryKeyMicroseconds']) AS kcpu,
      groupArray(ProfileEvents['SelectedMarksTotal']) AS marks, groupArray(memory_usage) AS mem, min(event_time_microseconds) AS first
    FROM system.query_log WHERE type = 'QueryFinish' AND query_id LIKE 'cgapi-%' AND current_database = {db:String}
      AND event_time_microseconds >= fromUnixTimestamp64Milli({t1:Int64}) AND event_time_microseconds <= fromUnixTimestamp64Milli({t2:Int64})
    GROUP BY h ORDER BY first`, { db, t1, t2 });
  const stmts = rows.map((r) => ({ q: r.q.replace(/\s+/g, ' '), n: Number(r.n), wall: median(r.wall.map(Number)), read_rows: median(r.rr.map(Number)), read_mb: Math.round(median(r.rb.map(Number)) / 1e6), cpu_ms: Math.round(median(r.cpu.map(Number)) / 1000), key_ms: Math.round(median(r.kcpu.map(Number)) / 1000), marks: median(r.marks.map(Number)), mem_mb: Math.round(median(r.mem.map(Number)) / 1e6) }));
  const data = last.body.data.transaction ?? last.body.data.output ?? last.body.data.search_output_prefix ?? last.body.data.transaction_aggregate ?? Object.values(last.body.data)[0];
  const size = data?.nodes?.length ?? data?.aggregate?.count ?? (Array.isArray(data) ? data.length : undefined);
  const heavy = stmts.filter((s) => s.n >= Number(runs)).reduce((a, s) => ({ rows: a.rows + s.read_rows, cpu: a.cpu + s.cpu_ms }), { rows: 0, cpu: 0 });
  console.log(JSON.stringify({ label, name, p50: Math.round(median(times)), min: Math.round(Math.min(...times)), size, has_more: data?.has_more, read_rows: heavy.rows, cpu_ms: heavy.cpu, sample: data?.aggregate, statements: stmts }));
  await client.close();
}
await admin.close();
process.exit(0);
