// cspell:ignore bytewise clickhouse nullness Milli unhex prefilter denormalised selftest orged unnest
/**
 * Engine access for the parity harness.
 *
 * Postgres: one coordinator connection opens a REPEATABLE READ READ ONLY
 * transaction and exports its snapshot; every worker connection imports it
 * (`SET TRANSACTION SNAPSHOT`), so all chunks of one run read one consistent
 * database state, however long the run takes.
 *
 * ClickHouse: plain HTTP. Consistency comes from the WP4 pinned views
 * (`*_at`): the harness reads visible(n) of every compared node first, then
 * visible(0) and the committed tail once (the readSnapshot order), and passes
 * the same values to every query of the run. Credentials come from CH_USER /
 * CH_PASSWORD (never argv).
 */
import pg from 'pg';

const { Client } = pg;

const cursorBatchRows = 5000;

export class PostgresSnapshotPool {
  constructor(url, size) {
    this.url = url;
    this.size = size;
    this.idle = [];
    this.waiting = [];
    this.all = [];
  }

  async open() {
    this.coordinator = new Client({ connectionString: this.url });
    await this.coordinator.connect();
    await this.coordinator.query(
      'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY'
    );
    const { rows } = await this.coordinator.query(
      'SELECT pg_export_snapshot() AS id'
    );
    this.snapshotId = rows[0].id;
    for (let index = 0; index < this.size; index += 1) {
      const client = new Client({ connectionString: this.url });
      await client.connect();
      await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
      await client.query(`SET TRANSACTION SNAPSHOT '${this.snapshotId}'`);
      this.all.push(client);
      this.idle.push(client);
    }
  }

  async acquire() {
    const client = this.idle.pop();
    if (client !== undefined) return client;
    return new Promise((resolve) => this.waiting.push(resolve));
  }

  release(client) {
    const next = this.waiting.shift();
    if (next === undefined) this.idle.push(client);
    else next(client);
  }

  async with(work) {
    const client = await this.acquire();
    try {
      return await work(client);
    } finally {
      this.release(client);
    }
  }

  async query(sql) {
    return this.with(async (client) => (await client.query(sql)).rows);
  }

  /** Stream rows (arrays of text) through a cursor inside the snapshot transaction. */
  async *stream(sql) {
    const client = await this.acquire();
    const cursor = `parity_${Math.random().toString(36).slice(2)}`;
    try {
      await client.query(`DECLARE ${cursor} NO SCROLL CURSOR FOR ${sql}`);
      for (;;) {
        const { rows } = await client.query({
          rowMode: 'array',
          text: `FETCH ${cursorBatchRows} FROM ${cursor}`,
        });
        if (rows.length === 0) break;
        for (const row of rows)
          yield row.map((value) => (value === null ? null : String(value)));
      }
      await client.query(`CLOSE ${cursor}`);
    } finally {
      this.release(client);
    }
  }

  async close() {
    for (const client of [...this.all, this.coordinator]) {
      if (client === undefined) continue;
      try {
        await client.query('ROLLBACK');
      } catch {
        // already closed
      }
      await client.end();
    }
  }
}

export class ClickHouseHttp {
  constructor(url, database) {
    this.url = url.replace(/\/$/, '');
    this.database = database;
    this.headers = { 'Content-Type': 'text/plain' };
    if (process.env.CH_USER)
      this.headers['X-ClickHouse-User'] = process.env.CH_USER;
    if (process.env.CH_PASSWORD)
      this.headers['X-ClickHouse-Key'] = process.env.CH_PASSWORD;
  }

  async post(sql) {
    const response = await fetch(
      `${this.url}/?database=${encodeURIComponent(this.database)}`,
      {
        body: sql,
        headers: this.headers,
        method: 'POST',
      }
    );
    if (!response.ok) {
      throw new Error(
        `ClickHouse ${response.status}: ${(await response.text()).slice(
          0,
          2000
        )}\n-- query:\n${sql.slice(0, 2000)}`
      );
    }
    return response;
  }

  async command(sql) {
    await (await this.post(sql)).text();
  }

  async query(sql) {
    const text = await (await this.post(`${sql} FORMAT JSONEachRow`)).text();
    return text
      .split('\n')
      .filter((line) => line.length > 0)
      .map((line) => JSON.parse(line));
  }

  /** Stream rows (arrays of text; `\N` -> null) of a TabSeparated result. */
  async *stream(sql) {
    const response = await this.post(`${sql} FORMAT TabSeparated`);
    const decoder = new TextDecoder();
    let buffered = '';
    for await (const bytes of response.body) {
      buffered += decoder.decode(bytes, { stream: true });
      let newline = buffered.indexOf('\n');
      while (newline !== -1) {
        const line = buffered.slice(0, newline);
        buffered = buffered.slice(newline + 1);
        yield line.split('\t').map((value) => (value === '\\N' ? null : value));
        newline = buffered.indexOf('\n');
      }
    }
    if (buffered.length > 0)
      yield buffered
        .split('\t')
        .map((value) => (value === '\\N' ? null : value));
  }
}

/** Read the WP4 snapshot for the compared nodes: visible(n) first, then visible(0) + committed tail. */
export const readClickHouseSnapshot = async (clickhouse, chNodeIds) => {
  const visible = new Map();
  for (const id of chNodeIds) {
    const [row] = await clickhouse.query(
      `SELECT toString(max(visible_seq)) AS v FROM visibility WHERE node_internal_id = ${Number(
        id
      )}`
    );
    visible.set(id, row?.v ?? '0');
  }
  const [row] = await clickhouse.query(
    `WITH (SELECT max(visible_seq) FROM visibility WHERE node_internal_id = 0) AS v0
     SELECT toString(v0) AS visible0, arrayMap(x -> toString(x), arraySort(groupArray(commit_seq))) AS tail
     FROM commit_log WHERE state = 'committed' AND commit_seq > v0`
  );
  return { tail: row?.tail ?? [], visible, visible0: row?.visible0 ?? '0' };
};
