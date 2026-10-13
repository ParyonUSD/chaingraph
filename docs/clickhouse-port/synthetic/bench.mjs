// Run captured statements (capture.mjs: <prefix>.sql + <prefix>.params.json) N times after WARMUPS (default 1) warm-ups; medians from query_log.
// usage: node bench.mjs <db> <runs> <settingsJSON> <prefix>...
import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
const [db, runs, settingsArg, ...prefixes] = process.argv.slice(2);
const ch = async (text, extra = {}) => {
  const qs = new URLSearchParams({ database: db, ...extra });
  const r = await fetch(`http://localhost:18123/?${qs}`, { method: 'POST', body: text });
  const body = await r.text();
  if (!r.ok) throw new Error(body.slice(0, 1000));
  return body;
};
const median = (xs) => { const s = [...xs].sort((a, b) => a - b); return s[Math.floor(s.length / 2)]; };
for (const prefix of prefixes) {
  const sql = readFileSync(`${prefix}.sql`, 'utf8');
  const params = JSON.parse(readFileSync(`${prefix}.params.json`, 'utf8'));
  const qp = { ...JSON.parse(settingsArg) };
  for (const [k, v] of Object.entries(params)) {
    qp[`param_${k}`] = Array.isArray(v) ? `[${v.map((x) => (typeof x === 'string' && !/^\d+$/u.test(x) ? `'${x}'` : x)).join(',')}]` : String(v);
  }
  const ids = [];
  try {
    const warm = Number(process.env.WARMUPS ?? 1);
    for (let i = 0; i < Number(runs) + warm; i++) {
      const id = `fix6-${randomUUID()}`;
      await ch(`${sql} FORMAT Null`, { ...qp, query_id: id });
      if (i >= warm) ids.push(id);
    }
  } catch (error) { console.log(JSON.stringify({ prefix, error: String(error).slice(0, 600) })); continue; }
  await ch('SYSTEM FLUSH LOGS');
  const rows = (await ch(`SELECT query_duration_ms, read_rows, read_bytes, ProfileEvents['UserTimeMicroseconds'] AS cpu, ProfileEvents['FilteringMarksWithPrimaryKeyMicroseconds'] AS kcpu, ProfileEvents['SelectedMarksTotal'] AS marks, memory_usage, ProfileEvents['CompressedReadBufferBytes'] AS comp, ProfileEvents['OSReadBytes'] AS osr FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN (${ids.map((i) => `'${i}'`).join(',')}) FORMAT JSONEachRow`)).trim().split('\n').map((l) => JSON.parse(l));
  const m = (k) => median(rows.map((r) => Number(r[k])));
  console.log(JSON.stringify({ prefix: prefix.split('/').pop(), wall: m('query_duration_ms'), read_rows: m('read_rows'), read_mb: Math.round(m('read_bytes') / 1e6), cpu_ms: Math.round(m('cpu') / 1000), key_ms: Math.round(m('kcpu') / 1000), marks: m('marks'), mem_mb: Math.round(m('memory_usage') / 1e6), decomp_mb: Math.round(m('comp') / 1e6), disk_mb: Math.round(m('osr') / 1e6) }));
}
