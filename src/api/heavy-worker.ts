/* eslint-disable functional/no-try-statement, @typescript-eslint/naming-convention */
// cspell:ignore clickhouse
/**
 * A heavy-lane worker thread (heavy-lane.ts): its own API instance answers
 * the heavy requests the main thread admitted. It admits nothing itself:
 * its admission gate (unbounded budgets) only records the statements and
 * attributes this thread's loop time to the requests, which the main
 * thread charges to its budgets.
 */
import { parentPort, workerData } from 'node:worker_threads';

import { ClickHouseClient } from '../store/clickhouse/client.js';

import type { AdmissionOptions } from './admission.js';
import { defaultAdmissionOptions } from './admission.js';
import type { RequestScope } from './db.js';
import { requestScope } from './db.js';
import type {
  HeavyLaneWorkerData,
  LaneCommand,
  LaneMessage,
} from './heavy-lane.js';
import { createApi } from './server.js';

const data = workerData as HeavyLaneWorkerData;
const port = parentPort!;

/** Unbounded: the main thread decided; this gate only records and attributes. */
const recordOnly: AdmissionOptions = {
  ...defaultAdmissionOptions,
  clickhouse: { burstSeconds: 1, lightShare: 0, ratePerSecond: 1e12 },
  contendedSettings: {},
  cpuSource: 'rows',
  enabled: true,
  heavyMaxInFlight: Number.MAX_SAFE_INTEGER,
  loop: { burstSeconds: 1, lightShare: 0, ratePerSecond: 1e12 },
  loopGuard: 2,
  settings: { heavy: {}, light: {} },
};

const client = new ClickHouseClient(data.clickhouse);
const api = createApi(client, {
  admission: recordOnly,
  responseCache: false,
  ...(data.maxResultRows === undefined
    ? {}
    : { maxResultRows: data.maxResultRows }),
  ...(data.authchainMaxBranches === undefined
    ? {}
    : { authchainMaxBranches: data.authchainMaxBranches }),
  ...(data.statementTimeouts === undefined
    ? {}
    : { statementTimeouts: data.statementTimeouts }),
  ...(data.utxoSet === undefined ? {} : { utxoSet: data.utxoSet }),
  ...(data.limits === undefined ? {} : { limits: data.limits }),
  ...(data.trustedDocuments === undefined
    ? {}
    : { trustedDocuments: data.trustedDocuments }),
});

const controllers = new Map<number, AbortController>();

const post = (message: LaneMessage, transfer?: ArrayBuffer[]) => {
  port.postMessage(message, transfer ?? []);
};

const run = async (command: Extract<LaneCommand, { type: 'run' }>) => {
  const controller = new AbortController();
  controllers.set(command.id, controller);
  const { admission } = api.db;
  const ticket = admission.admit(undefined);
  ticket.settings = command.settings;
  const scope: RequestScope = {
    signal: controller.signal,
    snapshots: [],
    ticket,
  };
  try {
    const response = await requestScope.run(scope, async () =>
      api.yoga.fetch('http://lane/graphql', {
        body: command.body,
        headers: {
          'content-type': 'application/json',
          ...(command.accept === undefined ? {} : { accept: command.accept }),
        },
        method: 'POST',
      })
    );
    const body = new Uint8Array(await response.arrayBuffer());
    admission.finish(ticket);
    const resolved = scope.snapshots?.[0]?.resolved;
    const marks: [number, string][] | undefined =
      resolved === undefined
        ? undefined
        : [
            [0, resolved.visible0.toString()],
            ...[...resolved.visibleByNode]
              .filter(([node]) => node !== 0)
              .map(([node, mark]): [number, string] => [node, mark.toString()]),
          ];
    post(
      {
        body,
        headers: [...response.headers],
        id: command.id,
        loopMs: ticket.loopMs,
        ...(marks === undefined ? {} : { marks }),
        statements: ticket.statements.map((statement) => ({
          queryId: statement.queryId,
          rows: statement.rows,
          shape: statement.shape,
        })),
        status: response.status,
        type: 'done',
      },
      [body.buffer]
    );
  } catch (error: unknown) {
    admission.finish(ticket);
    post({ id: command.id, message: String(error), type: 'error' });
  } finally {
    controllers.delete(command.id);
  }
};

port.on('message', (command: LaneCommand) => {
  if (command.type === 'cancel') {
    controllers.get(command.id)?.abort(new Error('The HTTP client went away.'));
    return;
  }
  // eslint-disable-next-line @typescript-eslint/no-floating-promises
  run(command);
});

post({ type: 'ready' });
