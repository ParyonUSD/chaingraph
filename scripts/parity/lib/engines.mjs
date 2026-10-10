// cspell:ignore bytewise clickhouse nullness Milli unhex prefilter denormalised selftest orged unnest
/**
 * Engine access for the parity harness.
 *
 * Postgres: one coordinator connection opens a REPEATABLE READ READ ONLY
 * transaction and exports its snapshot; every worker connection imports it
 * (`SET TRANSACTION SNAPSHOT`), so all chunks of one run read one consistent
 * database state, however long the run takes.
 *
 * ClickHouse: plain HTTP. Consistency comes from the pinned views (`*_at`,
 * WP4/WP6b): the harness reads one snapshot (watermarks, committed tail, void
 * set, epoch fence) in one query (`readClickHouseSnapshot`) and passes the
 * same values to every query of the run. Credentials come from CH_USER /
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

/**
 * Read the WP4/WP6b snapshot for the compared nodes in ONE query, mirroring
 * visibility.ts `snapshotSql`: visible(n) of every compared node and
 * visible(0) from one read of `visibility`, then the committed tail above
 * visible(0), then (data-dependent, so evaluated after) the void set and the
 * per-epoch fence up to the snapshot's highest seq, and last the hidden
 * stand-in seqs (snapshotSql step 5, appended to the void set). The harness inlines the
 * values as literals, so there is no parameter size limit and the void set is
 * never truncated (the views' overflow fallback is not used).
 */
export const readClickHouseSnapshot = async (clickhouse, chNodeIds) => {
  const ids = chNodeIds.map((id) => Number(id));
  const counterMask = '1099511627775';
  const [row] = await clickhouse.query(
    `WITH
       (SELECT groupArray((n, v)) FROM
          (SELECT node_internal_id AS n, max(visible_seq) AS v FROM visibility
           WHERE node_internal_id IN (${[0, ...ids].join(', ')}) GROUP BY n)) AS marks,
       arrayMax(arrayMap(m -> if(m.1 = 0, m.2, toUInt64(0)), arrayPushBack(marks, (toUInt32(0), toUInt64(0))))) AS v0,
       (SELECT arraySort(groupArray(commit_seq)) FROM commit_log
        WHERE state = 'committed' AND commit_seq > v0) AS tail_seqs,
       greatest(arrayMax(arrayMap(m -> m.2, arrayPushBack(marks, (toUInt32(0), toUInt64(0))))),
                arrayMax(arrayPushBack(tail_seqs, toUInt64(0)))) AS bound,
       (SELECT arraySort(groupUniqArray(commit_seq)) FROM commit_void WHERE commit_seq <= bound) AS void_seqs,
       (SELECT (groupArray(epoch), groupArray(max_valid_seq)) FROM
          (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM epoch_fence
           WHERE epoch <= bitShiftRight(bound, 40) GROUP BY epoch)) AS fences,
       (SELECT groupUniqArray(seq) FROM
          (SELECT stand_in_seq AS seq, owner_seq AS by_seq, toUInt8(0) AS resolves
           FROM input_stand_in WHERE stand_in_seq <= bound
           UNION ALL
           SELECT stand_in_seq, commit_seq, toUInt8(1)
           FROM input_stand_in_resolution WHERE stand_in_seq <= bound)
        WHERE resolves = toUInt8(
          ((by_seq <= v0 AND by_seq NOT IN (SELECT commit_seq FROM commit_void)) OR has(tail_seqs, by_seq))
          AND (indexOf(fences.1, bitShiftRight(by_seq, 40)) = 0
               OR by_seq <= fences.2[indexOf(fences.1, bitShiftRight(by_seq, 40))]))) AS stand_in_hidden
     SELECT
       arrayMap(id -> toString(arrayMax(arrayMap(m -> if(m.1 = id, m.2, toUInt64(0)),
                                                 arrayPushBack(marks, (toUInt32(0), toUInt64(0)))))),
                [${ids.length === 0 ? '' : ids.join(', ')}]::Array(UInt32)) AS visible,
       toString(v0) AS visible0,
       arrayMap(x -> toString(x), tail_seqs) AS tail,
       arrayMap(x -> toString(x), arrayConcat(void_seqs, arraySort(stand_in_hidden))) AS void,
       arrayMap(e -> toString(if(indexOf(fences.1, e) = 0, ${counterMask},
                                 bitAnd(fences.2[indexOf(fences.1, e)], ${counterMask}))),
                range(1, toUInt64(bitShiftRight(bound, 40)) + 1)) AS fence`
  );
  const visible = new Map(
    chNodeIds.map((id, index) => [id, row?.visible?.[index] ?? '0'])
  );
  return {
    fence: row?.fence ?? [],
    tail: row?.tail ?? [],
    visible,
    visible0: row?.visible0 ?? '0',
    void: row?.void ?? [],
  };
};
