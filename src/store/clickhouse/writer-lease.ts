/* eslint-disable max-classes-per-file, camelcase, @typescript-eslint/naming-convention, functional/no-mixed-type, @typescript-eslint/parameter-properties, @typescript-eslint/no-magic-numbers, complexity */
// cspell:ignore clickhouse dedup milli
/**
 * The single-writer lease with epoch fencing (plan §2.6/§3.3, WP4).
 *
 * ClickHouse has no compare-and-set, so the lease is a claim protocol:
 * 1. read the claims; refuse if another agent's newest claim is unexpired;
 * 2. claim epoch = (highest epoch seen) + 1, stamped with the server's clock;
 * 3. wait `settleMs`, re-read: the holder is the claim with the highest epoch,
 *    ties broken by the earliest server `claimed_at`, then `agent_id`. A
 *    claimant that is not the holder gives up before writing anything.
 *
 * The epoch is the high part of every `commit_seq` it writes
 * (commit-log.ts), and the next holder fences older epochs, so a stale
 * writer's later commits are invisible even if it ignores the lease. The
 * holder also stops itself: `assertHeld` fails once its local deadline
 * (ttl - safety margin, measured from before the server stamped the
 * claim/heartbeat) passes without a successful renewal.
 * Residual races: docs/clickhouse-port/wp4-commit-and-visibility.md (d).
 */
import { randomBytes } from 'node:crypto';
import { hostname } from 'node:os';

import type { ClickHouseClient } from './client.js';
import type { CommitLease } from './commit-log.js';

export class LeaseHeldError extends Error {}
export class LeaseLostError extends Error {}

export interface LeaseClaim {
  epoch: bigint;
  agentId: string;
  claimedAtMs: number;
  heartbeatAtMs: number;
  expiresAtMs: number;
}

export interface WriterLeaseOptions {
  leaseName?: string;
  /** Unique per process lifetime; default `host:pid:random`. */
  agentId?: string;
  ttlMs?: number;
  /** Wait between claiming and confirming (must exceed claim-insert latency). */
  settleMs?: number;
  /** Stop writing this long before the lease would expire. */
  safetyMarginMs?: number;
  /** Heartbeat period; default ttl / 3. */
  renewIntervalMs?: number;
  /** Local monotonic clock (ms). */
  monotonicNow?: () => number;
}

const defaultLeaseName = 'chaingraph-agent';
const defaultTtlMs = 30_000;
const defaultSettleMs = 1_000;
const defaultSafetyMarginMs = 5_000;
const randomSuffixBytes = 4;
const renewDivisor = 3;

const monotonicMs = () => Number(process.hrtime.bigint() / 1_000_000n);

/**
 * The holder among claims: highest epoch, then earliest server claim time,
 * then lowest agent id. Pure; every agent that sees the same claims agrees.
 */
export const leaseHolder = (
  claims: readonly LeaseClaim[]
): LeaseClaim | undefined =>
  [...claims].sort((a, b) => {
    if (a.epoch !== b.epoch) {
      return a.epoch > b.epoch ? -1 : 1;
    }
    if (a.claimedAtMs !== b.claimedAtMs) {
      return a.claimedAtMs - b.claimedAtMs;
    }
    return a.agentId < b.agentId ? -1 : a.agentId > b.agentId ? 1 : 0;
  })[0];

const sleep = async (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

export class WriterLease implements CommitLease {
  readonly leaseName: string;

  readonly agentId: string;

  readonly ttlMs: number;

  private readonly settleMs: number;

  private readonly safetyMarginMs: number;

  private readonly renewIntervalMs: number;

  private readonly monotonicNow: () => number;

  private held:
    | { epoch: bigint; claimedAtMs: number; deadline: number; lost: boolean }
    | undefined;

  private heartbeat: ReturnType<typeof setInterval> | undefined;

  constructor(
    private readonly client: Pick<
      ClickHouseClient,
      'command' | 'insertSelect' | 'query'
    >,
    options: WriterLeaseOptions = {}
  ) {
    this.leaseName = options.leaseName ?? defaultLeaseName;
    this.agentId =
      options.agentId ??
      `${hostname()}:${process.pid}:${randomBytes(randomSuffixBytes).toString(
        'hex'
      )}`;
    this.ttlMs = options.ttlMs ?? defaultTtlMs;
    this.settleMs = options.settleMs ?? defaultSettleMs;
    this.safetyMarginMs = Math.min(
      options.safetyMarginMs ?? defaultSafetyMarginMs,
      Math.floor(this.ttlMs / 2)
    );
    this.renewIntervalMs =
      options.renewIntervalMs ?? Math.floor(this.ttlMs / renewDivisor);
    this.monotonicNow = options.monotonicNow ?? monotonicMs;
  }

  /** The held epoch. Throws if no lease is held. */
  get epoch(): bigint {
    if (this.held === undefined) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new LeaseLostError('No writer lease is held.');
    }
    return this.held.epoch;
  }

  get isHeld() {
    return (
      this.held !== undefined &&
      !this.held.lost &&
      this.monotonicNow() < this.held.deadline
    );
  }

  /** Throws `LeaseLostError` unless the lease is held and within its local deadline. */
  assertHeld(): void {
    if (this.held === undefined || this.held.lost) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new LeaseLostError(`Writer lease ${this.leaseName} is not held.`);
    }
    if (this.monotonicNow() >= this.held.deadline) {
      this.held.lost = true;
      // eslint-disable-next-line functional/no-throw-statement
      throw new LeaseLostError(
        `Writer lease ${this.leaseName} epoch ${this.held.epoch} passed its deadline without renewal.`
      );
    }
  }

  /** All claims (latest heartbeat of each) and the server's clock. */
  async readClaims(): Promise<{ claims: LeaseClaim[]; serverNowMs: number }> {
    const rows = await this.client.query<{
      epoch: string;
      agent_id: string;
      claimed_ms: string;
      heartbeat_ms: string;
      expires_ms: string;
      server_now: string;
    }>(
      `SELECT epoch, agent_id,
         toUnixTimestamp64Milli(min(claimed_at)) AS claimed_ms,
         toUnixTimestamp64Milli(max(heartbeat_at)) AS heartbeat_ms,
         toUnixTimestamp64Milli(argMax(expires_at, heartbeat_at)) AS expires_ms,
         toUnixTimestamp64Milli(now64(3, 'UTC')) AS server_now
       FROM writer_lease
       WHERE lease_name = {name:String}
       GROUP BY epoch, agent_id`,
      { name: this.leaseName }
    );
    const serverNowRows =
      rows.length > 0
        ? rows
        : await this.client.query<{ server_now: string }>(
            "SELECT toUnixTimestamp64Milli(now64(3, 'UTC')) AS server_now"
          );
    return {
      claims: rows.map((row) => ({
        agentId: row.agent_id,
        claimedAtMs: Number(row.claimed_ms),
        epoch: BigInt(row.epoch),
        expiresAtMs: Number(row.expires_ms),
        heartbeatAtMs: Number(row.heartbeat_ms),
      })),
      serverNowMs: Number(serverNowRows[0]?.server_now ?? Date.now()),
    };
  }

  /** Acquire the lease; resolves to the new epoch. */
  async acquire(): Promise<bigint> {
    const { claims, serverNowMs } = await this.readClaims();
    const holder = leaseHolder(claims);
    if (
      holder !== undefined &&
      holder.agentId !== this.agentId &&
      holder.expiresAtMs > serverNowMs
    ) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new LeaseHeldError(
        `Writer lease ${this.leaseName} is held by ${holder.agentId} (epoch ${
          holder.epoch
        }) for another ${holder.expiresAtMs - serverNowMs} ms.`
      );
    }
    const highestEpoch = claims.reduce(
      (highest, claim) => (claim.epoch > highest ? claim.epoch : highest),
      0n
    );
    const epoch = highestEpoch + 1n;
    const startedAt = this.monotonicNow();
    await this.writeClaimRow(epoch, undefined, false);
    await sleep(this.settleMs);
    const confirmed = await this.readClaims();
    const winner = leaseHolder(confirmed.claims);
    if (
      winner === undefined ||
      winner.epoch !== epoch ||
      winner.agentId !== this.agentId
    ) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new LeaseLostError(
        `Lost the race for writer lease ${this.leaseName} epoch ${epoch} to ${
          winner?.agentId ?? 'nobody'
        } (epoch ${winner?.epoch ?? 0n}).`
      );
    }
    this.held = {
      claimedAtMs: winner.claimedAtMs,
      deadline: startedAt + this.ttlMs - this.safetyMarginMs,
      epoch,
      lost: false,
    };
    return epoch;
  }

  /**
   * Renew: confirm this agent is still the holder, then write a heartbeat
   * extending the expiry. Marks the lease lost and throws otherwise.
   */
  async renew(): Promise<void> {
    this.assertHeld();
    const held = this.held!;
    const startedAt = this.monotonicNow();
    const { claims } = await this.readClaims();
    const holder = leaseHolder(claims);
    if (
      holder === undefined ||
      holder.epoch !== held.epoch ||
      holder.agentId !== this.agentId
    ) {
      held.lost = true;
      // eslint-disable-next-line functional/no-throw-statement
      throw new LeaseLostError(
        `Writer lease ${this.leaseName} epoch ${held.epoch} was taken over by ${
          holder?.agentId ?? 'nobody'
        } (epoch ${holder?.epoch ?? 0n}).`
      );
    }
    this.assertHeld();
    await this.writeClaimRow(held.epoch, held.claimedAtMs, false);
    held.deadline = startedAt + this.ttlMs - this.safetyMarginMs;
  }

  /** Release: expire the claim now so a successor need not wait for the ttl. */
  async release(): Promise<void> {
    this.stopHeartbeat();
    const { held } = this;
    this.held = undefined;
    if (held === undefined || held.lost) {
      return;
    }
    await this.writeClaimRow(held.epoch, held.claimedAtMs, true);
  }

  /** Renew every `renewIntervalMs`; on failure the lease is lost and `onLost` runs. */
  startHeartbeat(onLost: (error: unknown) => void) {
    this.stopHeartbeat();
    this.heartbeat = setInterval(() => {
      this.renew().catch((error: unknown) => {
        if (this.held !== undefined) {
          this.held.lost = true;
        }
        this.stopHeartbeat();
        onLost(error);
      });
    }, this.renewIntervalMs);
    this.heartbeat.unref();
  }

  stopHeartbeat() {
    if (this.heartbeat !== undefined) {
      clearInterval(this.heartbeat);
      this.heartbeat = undefined;
    }
  }

  private async writeClaimRow(
    epoch: bigint,
    claimedAtMs: number | undefined,
    expireNow: boolean
  ) {
    await this.client.command(
      `INSERT INTO writer_lease (lease_name, epoch, agent_id, claimed_at, heartbeat_at, expires_at)
       SELECT {name:String}, {epoch:UInt64}, {agent:String},
         if({hasClaimedAt:UInt8} = 1, fromUnixTimestamp64Milli({claimedAtMs:Int64}, 'UTC'), now64(3, 'UTC')),
         now64(3, 'UTC'),
         if({expireNow:UInt8} = 1, now64(3, 'UTC'), now64(3, 'UTC') + toIntervalMillisecond({ttlMs:UInt64}))`,
      {
        agent: this.agentId,
        claimedAtMs: claimedAtMs ?? 0,
        epoch,
        expireNow: expireNow ? 1 : 0,
        hasClaimedAt: claimedAtMs === undefined ? 0 : 1,
        name: this.leaseName,
        ttlMs: this.ttlMs,
      },
      // heartbeats are distinct rows (server timestamps); no dedup by content
      { async_insert: 0, insert_deduplicate: 0 }
    );
  }
}
