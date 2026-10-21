import type pg from 'pg';

/**
 * Error codes Postgres sends when it closes a session: `57P01`
 * (admin_shutdown – fast shutdown/restart or `pg_terminate_backend`), `57P02`
 * (crash_shutdown), `57P03` (cannot_connect_now – starting up or shutting
 * down).
 */
const connectionLossSqlStates = ['57P01', '57P02', '57P03'];
/**
 * Socket-level errors seen while Postgres is restarting or unreachable.
 */
const connectionLossSystemErrorCodes = [
  'ECONNREFUSED',
  'ECONNRESET',
  'EPIPE',
  'ETIMEDOUT',
  'EHOSTUNREACH',
  'ENOTFOUND',
  'EAI_AGAIN',
];
/**
 * Messages node-postgres uses for connections that ended underneath a client.
 */
const connectionLossMessages = [
  'Connection terminated',
  'Client has encountered a connection error and is not queryable',
];

const isConnectionLossCode = (code: unknown) =>
  typeof code === 'string' &&
  (connectionLossSqlStates.includes(code) ||
    code.startsWith('08') ||
    connectionLossSystemErrorCodes.includes(code));

const isConnectionLossMessage = (message: unknown) =>
  typeof message === 'string' &&
  connectionLossMessages.some((lossMessage) => message.includes(lossMessage));

/**
 * Returns `true` if `err` indicates the Postgres connection was lost (e.g.
 * Postgres restarted), rather than a problem with the query or the data.
 * Operations failing with such an error can be retried once Postgres is back.
 * @param err - the error to test
 */
export const isPostgresConnectionError = (err: unknown) => {
  if (typeof err !== 'object' || err === null) {
    return false;
  }
  const { code, message } = err as { code?: unknown; message?: unknown };
  return isConnectionLossCode(code) || isConnectionLossMessage(message);
};

/**
 * Summarize a Postgres connection error for logging without the attached
 * client/socket state (node-postgres errors can carry the whole client).
 * @param err - the error to describe
 */
export const describePostgresError = (err: unknown) => {
  if (typeof err !== 'object' || err === null) {
    return String(err);
  }
  const { code, message } = err as { code?: unknown; message?: unknown };
  const text = typeof message === 'string' ? message : String(err);
  return typeof code === 'string' ? `${code}: ${text}` : text;
};

/**
 * Keep a lost Postgres connection from crashing the process.
 *
 * When Postgres restarts (or a connection is terminated), node-postgres emits
 * an `error` event: on the pool for idle clients, and on the client itself for
 * checked-out clients between queries. With no listener, Node.js throws it as
 * an uncaught exception. The broken client is already unusable: the pool
 * discards it on release (or immediately, if idle), and the next `connect()`
 * opens a fresh connection, so the only thing to do here is log it. Queries in
 * flight still reject and are handled by their callers.
 * @param targetPool - the pool to protect
 * @param onConnectionError - called once per lost connection
 */
export const handlePoolConnectionErrors = (
  targetPool: pg.Pool,
  onConnectionError: (err: Error) => void
) => {
  targetPool.on('connect', (client) => {
    // eslint-disable-next-line functional/no-let
    let reported = false;
    // a terminated client can emit more than once (error, then socket end)
    client.on('error', (err) => {
      if (reported) {
        return;
      }
      reported = true;
      onConnectionError(err);
    });
  });
  /*
   * Idle clients also emit through the pool (after the client-level listener
   * above has already reported the error); this listener only prevents the
   * pool's re-emitted `error` from being thrown.
   */
  targetPool.on('error', () => undefined);
};
