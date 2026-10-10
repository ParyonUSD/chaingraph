/* eslint-disable camelcase, functional/no-throw-statement, @typescript-eslint/parameter-properties, @typescript-eslint/naming-convention, complexity, max-classes-per-file */
// cspell:ignore clickhouse
/**
 * The API's access to ClickHouse: query counters (tests assert one
 * `readSnapshot` per request and per subscription re-run, and count data
 * queries), node-name resolution, and the per-request pinned snapshot.
 */
import { GraphQLError } from 'graphql';

import type {
  ClickHouseClient,
  ClickHouseSettings,
  QueryParams,
} from '../store/clickhouse/client.js';
import type { VisibilitySnapshot } from '../store/clickhouse/visibility.js';
import {
  nodeAgnosticId,
  readSnapshot,
} from '../store/clickhouse/visibility.js';

/**
 * Settings of every data query. `enable_materialized_cte`: a root page with
 * relationships is a `WITH page AS MATERIALIZED (…)` CTE (compile.ts).
 */
export const dataQuerySettings = {
  enable_materialized_cte: 1,
} as unknown as ClickHouseSettings;

export interface ApiStats {
  /** `readSnapshot` calls. */
  snapshots: number;
  /** Data queries (everything except snapshot reads, node lookups and watermark polls). */
  dataQueries: number;
  /** Node-name lookups (cached per process). */
  nodeLookups: number;
  /** Watermark polls of the live-query hub. */
  watermarkPolls: number;
}

export interface QueryEvent {
  kind: 'data' | 'nodeLookup' | 'snapshot' | 'watermarkPoll';
  sql: string;
  params: QueryParams;
  label: string;
}

export class ApiDb {
  readonly stats: ApiStats = {
    dataQueries: 0,
    nodeLookups: 0,
    snapshots: 0,
    watermarkPolls: 0,
  };

  /** Test hook: every query the API sends (after it is sent). */
  onQuery: ((event: QueryEvent) => void) | undefined;

  private readonly nodeIds = new Map<string, number>();

  constructor(readonly client: ClickHouseClient) {}

  /** Exactly one ClickHouse query: the visibility snapshot of `nodeId`. */
  async readSnapshot(nodeId: number): Promise<VisibilitySnapshot> {
    this.stats.snapshots += 1;
    return readSnapshot(
      {
        query: async <T>(sql: string, params?: QueryParams) => {
          const rows = await this.client.query<T>(sql, params);
          this.onQuery?.({
            kind: 'snapshot',
            label: 'readSnapshot',
            params: params ?? {},
            sql,
          });
          return rows;
        },
      },
      nodeId
    );
  }

  async query<T>(label: string, sql: string, params: QueryParams) {
    this.stats.dataQueries += 1;
    const rows = await this.client.query<T>(sql, params, dataQuerySettings);
    this.onQuery?.({ kind: 'data', label, params, sql });
    return rows;
  }

  /** Watermarks of the given nodes, one query (the live-query hub's poll). */
  async watermarks(nodeIds: readonly number[]) {
    this.stats.watermarkPolls += 1;
    const sql = `SELECT node_internal_id AS node, toString(max(visible_seq)) AS visible
       FROM visibility WHERE node_internal_id IN {nodes:Array(UInt32)} GROUP BY node`;
    const rows = await this.client.query<{ node: number; visible: string }>(
      sql,
      { nodes: nodeIds }
    );
    this.onQuery?.({
      kind: 'watermarkPoll',
      label: 'watermarks',
      params: { nodes: nodeIds },
      sql,
    });
    return new Map(rows.map((row) => [Number(row.node), BigInt(row.visible)]));
  }

  /** A node's internal id by name (node rows are few and permanent: cached). */
  async nodeId(name: string): Promise<number> {
    const cached = this.nodeIds.get(name);
    if (cached !== undefined) return cached;
    this.stats.nodeLookups += 1;
    const [row] = await this.client.query<{ internal_id: number }>(
      'SELECT internal_id FROM node_v WHERE name = {name:String} ORDER BY internal_id LIMIT 1',
      { name }
    );
    if (row === undefined) {
      throw new GraphQLError(`Unknown node: ${name}`, {
        extensions: { code: 'BAD_USER_INPUT' },
      });
    }
    const id = Number(row.internal_id);
    this.nodeIds.set(name, id);
    return id;
  }
}

/**
 * The one snapshot of a request (or of one subscription re-run). The node
 * is fixed when the request starts (from the operation's root `node`
 * arguments); the snapshot is read once, on first use, and shared by every
 * resolver. Node-agnostic roots ride on the same snapshot (its `visible0`,
 * `tail`), so a multi-root operation reads one consistent state.
 */
export class PinnedSnapshot {
  private pending: Promise<VisibilitySnapshot> | undefined;

  constructor(
    private readonly db: ApiDb,
    private readonly nodeNames: readonly string[],
    private readonly preset?: VisibilitySnapshot
  ) {}

  /** For a root that names `node` (undefined: a node-agnostic root). */
  async get(node?: string): Promise<VisibilitySnapshot> {
    if (node !== undefined && !this.nodeNames.includes(node)) {
      throw new GraphQLError(
        `Node ${node} is not a root node argument of this operation.`
      );
    }
    if (this.nodeNames.length > 1) {
      throw new GraphQLError(
        'All per-node roots of one operation must name the same node (one snapshot per request; spike limit).',
        { extensions: { code: 'BAD_USER_INPUT' } }
      );
    }
    if (this.pending === undefined) {
      this.pending =
        this.preset === undefined ? this.read() : Promise.resolve(this.preset);
    }
    return this.pending;
  }

  private async read() {
    const [name] = this.nodeNames;
    const nodeId =
      name === undefined ? nodeAgnosticId : await this.db.nodeId(name);
    return this.db.readSnapshot(nodeId);
  }
}
