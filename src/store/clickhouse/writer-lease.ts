/* eslint-disable max-classes-per-file, functional/no-try-statement, camelcase, @typescript-eslint/naming-convention, functional/no-mixed-type, @typescript-eslint/parameter-properties, @typescript-eslint/no-magic-numbers, complexity */
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
 *
 * Loss and recovery (WP6b; contract in docs/clickhouse-port/wp6b-gate-cost.md
 * §lease): the heartbeat is stall-tolerant. Each tick measures real elapsed
 * (monotonic) time; if the local deadline already passed (the event loop was
 * stalled past the safety margin) it reports the lease lost at once instead
 * of renewing late. A renewal that fails with an I/O error is retried on the
 * next tick and only becomes a loss when the deadline passes. `onLost` runs
 * exactly once per held epoch, with a `LeaseLostError` whose `reason` says
 * why; from then on `assertHeld` throws. The owner then either exits or calls
 * `reacquire()`, which claims a new epoch (the next `CommitLog.init()` fences
 * the old one) and must be followed by the recovery steps of the contract.
 */
import { randomBytes } from 'node:crypto';
import { hostname } from 'node:os';

import type { ClickHouseClient } from './client.js';
import type { CommitLease } from './commit-log.js';

export class LeaseHeldError extends Error {}

/**
 * Why a held lease was lost:
 * - `stalled`: the local deadline passed before a renewal could start (the
 *   event loop or the process was paused past ttl - safety margin);
 * - `taken-over`: a renewal saw another holder (a higher epoch);
 * - `deadline`: renewals failed (I/O) until the local deadline passed;
 * - `not-held`: no lease was held (never acquired, released or already lost).
 */
export type LeaseLossReason =
  | 'deadline'
  | 'not-held'
  | 'stalled'
  | 'taken-over';

export class LeaseLostError extends Error {
  constructor(
    message: string,
    readonly reason: LeaseLossReason = 'not-held',
    readonly epoch?: bigint
  ) {
    super(message);
  }
}

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
  /** Default: CHAINGRAPH_CLICKHOUSE_LEASE_TTL_MS, else 120 s. */
  ttlMs?: number;
  /** Wait between claiming and confirming (must exceed claim-insert latency). */
  settleMs?: number;
  /** Stop writing this long before the lease would expire; default ttl / 6 (20 s at 120 s), at most ttl / 2. */
  safetyMarginMs?: number;
  /** Heartbeat period; default ttl / 6 (20 s at 120 s). */
  renewIntervalMs?: number;
  /** Local monotonic clock (ms). */
  monotonicNow?: () => number;
}

const defaultLeaseName = 'chaingraph-agent';
/**
 * 120 s: WP6 saw synchronous stalls of about 60 s (twice per 100k-tx block,
 * the O(n^2) pending-spend pass) with the old 30 s ttl / 5 s margin; with a
 * 20 s renew period and a 20 s margin a stall of up to ttl - margin - period
 * = 80 s is survived, while a crashed writer is replaced within 2 minutes.
 */
export const defaultTtlMs = 120_000;
const defaultSettleMs = 1_000;
const randomSuffixBytes = 4;
/** margin = ttl / 6 and renew period = ttl / 6 by default */
const ttlFractionDivisor = 6;
const ttlEnvName = 'CHAINGRAPH_CLICKHOUSE_LEASE_TTL_MS';
const minTtlMs = 1_000;

/** `CHAINGRAPH_CLICKHOUSE_LEASE_TTL_MS` (an integer, at least 1000), or `undefined` if unset. */
export const leaseTtlFromEnv = (
  env: { [name: string]: string | undefined } = process.env
): number | undefined => {
  const raw = env[ttlEnvName];
  if (raw === undefined || raw === '') {
    return undefined;
  }
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minTtlMs) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new RangeError(
      `${ttlEnvName} must be an integer >= ${minTtlMs} (got ${raw}).`
    );
  }
  return value;
};

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

  private heartbeat: ReturnType<typeof setTimeout> | undefined;

  /** The current heartbeat's onLost; cleared when it has fired. */
  private onLost: ((error: LeaseLostError) => void) | undefined;

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
    this.ttlMs = options.ttlMs ?? leaseTtlFromEnv() ?? defaultTtlMs;
    this.settleMs = options.settleMs ?? defaultSettleMs;
    this.safetyMarginMs = Math.min(
      options.safetyMarginMs ?? Math.floor(this.ttlMs / ttlFractionDivisor),
      Math.floor(this.ttlMs / 2)
    );
    this.renewIntervalMs =
      options.renewIntervalMs ?? Math.floor(this.ttlMs / ttlFractionDivisor);
    this.monotonicNow = options.monotonicNow ?? monotonicMs;
  }

  /** The held epoch. Throws if no lease is held. */
  get epoch(): bigint {
    if (this.held === undefined) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new LeaseLostError('No writer lease is held.', 'not-held');
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
      throw new LeaseLostError(
        `Writer lease ${this.leaseName} is not held.`,
        'not-held',
        this.held?.epoch
      );
    }
    if (this.monotonicNow() >= this.held.deadline) {
      this.held.lost = true;
      // eslint-disable-next-line functional/no-throw-statement
      throw new LeaseLostError(
        `Writer lease ${this.leaseName} epoch ${this.held.epoch} passed its deadline without renewal.`,
        'deadline',
        this.held.epoch
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
        } (epoch ${winner?.epoch ?? 0n}).`,
        'taken-over',
        epoch
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
        } (epoch ${holder?.epoch ?? 0n}).`,
        'taken-over',
        held.epoch
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

  /**
   * After a loss (or with no lease held): claim a NEW epoch, as `acquire()`
   * does (so it fails with `LeaseHeldError` while another agent holds an
   * unexpired claim). The old epoch is never resumed: its open commits must be
   * treated as dead and the next `CommitLog.init()` fences it. Resolves to the
   * new epoch. The heartbeat is stopped; the caller restarts it after the
   * recovery steps (docs/clickhouse-port/wp6b-gate-cost.md §lease).
   */
  async reacquire(): Promise<bigint> {
    this.stopHeartbeat();
    if (this.held !== undefined) {
      this.held.lost = true;
    }
    this.held = undefined;
    return this.acquire();
  }

  /**
   * Renew every `renewIntervalMs` from a self-rescheduling timer (stall
   * tolerant, see the file comment). `onLost` runs exactly once, with a
   * `LeaseLostError` (`reason` `stalled`, `taken-over` or `deadline`); the
   * heartbeat stops itself before calling it. It is not called for
   * `stopHeartbeat()` or `release()`.
   */
  startHeartbeat(onLost: (error: LeaseLostError) => void) {
    this.stopHeartbeat();
    this.onLost = onLost;
    this.scheduleTick();
  }

  stopHeartbeat() {
    if (this.heartbeat !== undefined) {
      clearTimeout(this.heartbeat);
      this.heartbeat = undefined;
    }
    this.onLost = undefined;
  }

  /**
   * One heartbeat tick (exposed for tests; the timer calls it). Returns the
   * loss it reported, if any.
   */
  async tick(): Promise<LeaseLostError | undefined> {
    const { held } = this;
    if (held === undefined || held.lost) {
      return this.lose(
        new LeaseLostError(
          `Writer lease ${this.leaseName} is not held.`,
          'not-held',
          held?.epoch
        )
      );
    }
    const now = this.monotonicNow();
    if (now >= held.deadline) {
      held.lost = true;
      return this.lose(
        new LeaseLostError(
          `Writer lease ${this.leaseName} epoch ${
            held.epoch
          }: the heartbeat ran ${
            now - held.deadline
          } ms after the local deadline (stalled past the ${
            this.safetyMarginMs
          } ms safety margin); not renewing late.`,
          'stalled',
          held.epoch
        )
      );
    }
    try {
      await this.renew();
      return undefined;
    } catch (error: unknown) {
      if (error instanceof LeaseLostError) {
        held.lost = true;
        return this.lose(error);
      }
      // I/O error: keep the lease until the deadline; the next tick retries
      if (this.monotonicNow() >= held.deadline) {
        held.lost = true;
        return this.lose(
          new LeaseLostError(
            `Writer lease ${this.leaseName} epoch ${
              held.epoch
            }: renewals failed until the deadline (${String(error)}).`,
            'deadline',
            held.epoch
          )
        );
      }
      return undefined;
    }
  }

  private scheduleTick() {
    this.heartbeat = setTimeout(() => {
      this.heartbeat = undefined;
      this.tick()
        .then((lost) => {
          if (lost === undefined && this.onLost !== undefined) {
            this.scheduleTick();
          }
        })
        .catch(() => undefined);
    }, this.renewIntervalMs);
    this.heartbeat.unref();
  }

  /** Stop the heartbeat and report `error` to onLost (once). */
  private lose(error: LeaseLostError): LeaseLostError {
    const { onLost } = this;
    this.stopHeartbeat();
    onLost?.(error);
    return error;
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
