// Build (or catch up) the stored UTXO set of a database: node build-utxo-set.mjs <worktree> <db> [bulkMin] (bulkMin 1: one bulk build)
const [wt, db, bulk = '0'] = process.argv.slice(2);
const { ClickHouseClient } = await import(`${wt}/build/store/clickhouse/client.js`);
const { UtxoSetMaintainer } = await import(`${wt}/build/store/clickhouse/utxo-set.js`);
const client = new ClickHouseClient({ url: 'http://localhost:18123', database: db, username: 'default', password: '', requestTimeoutMs: 600000 });
const m = new UtxoSetMaintainer({ client, assertHeld: () => {}, epoch: () => 3n }, {
  bulkBuildMinCommits: Number(bulk), reportIntervalMs: 0,
  onProgress: (p) => console.log(JSON.stringify(p)),
  onViolation: (v) => console.log('VIOLATION', JSON.stringify(v)),
});
const t0 = Date.now();
const marks = await m.catchUp();
console.log('done', Date.now() - t0, 'ms', [...marks.entries()].map(([n, v]) => `${n}:${v.watermark}:${v.generation}`), m.status.batches, m.status.rows);
await client.close();
