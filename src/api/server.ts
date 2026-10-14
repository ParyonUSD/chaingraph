/* eslint-disable functional/no-mixed-type, complexity, functional/no-let, @typescript-eslint/init-declarations, functional/no-try-statement */
// cspell:ignore clickhouse serialised serialisation Pothos
/**
 * The Phase 2 spike server: GraphQL Yoga over the Pothos schema, HTTP
 * (queries, SSE subscriptions) and graphql-ws on `/graphql`.
 *
 * The snapshot plugin fixes, per execution, the node of the operation (from
 * its root fields' `node` arguments) and attaches a `PinnedSnapshot` that
 * reads ONE snapshot on first use; every resolver of the request uses it.
 */
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';

import type {
  DocumentNode,
  ExecutionArgs,
  GraphQLError,
  GraphQLSchema,
  OperationDefinitionNode,
  ValueNode,
} from 'graphql';
import { getOperationAST, Kind, print } from 'graphql';
import { useServer } from 'graphql-ws/use/ws';
import type { Plugin, YogaServerInstance } from 'graphql-yoga';
import { createYoga } from 'graphql-yoga';
import { WebSocketServer } from 'ws';

import type { ClickHouseClient } from '../store/clickhouse/client.js';

import type { AdmissionOptions, Ticket } from './admission.js';
import { planKeys } from './admission.js';
import type { RequestScope, StatementTimeouts } from './db.js';
import { ApiDb, PinnedSnapshot, requestScope } from './db.js';
import type { FastExecutionMode } from './fast-execute.js';
import {
  fastExecute,
  fastExecutionModeFromEnv,
  verifiedExecute,
} from './fast-execute.js';
import { handlePost } from './front.js';
import type { HeavyLaneOptions } from './heavy-lane.js';
import { HeavyLane } from './heavy-lane.js';
import type { LiveHubOptions } from './live.js';
import { LiveHub } from './live.js';
import type {
  QueryLimits,
  ResponseCacheOptions,
  TrustedDocuments,
} from './plugins.js';
import {
  defaultQueryLimits,
  resolveTrustedDocument,
  ResponseCache,
  useQueryLimits,
  useTrustedDocuments,
} from './plugins.js';
import type { ApiContext } from './schema.js';
import { schema } from './schema.js';
import type { UtxoSetPolicy } from './sql.js';

const argumentValue = (
  value: ValueNode,
  variables: { [name: string]: unknown } | null | undefined
): unknown => {
  if (value.kind === Kind.VARIABLE) return variables?.[value.name.value];
  if (value.kind === Kind.STRING) return value.value;
  return undefined;
};

/**
 * Node names inside an input value: every `accepted_by` field of an input
 * object at any depth (the `spent_by` filter's opt-in acceptance), in a
 * literal or in a variable's value.
 */
const inputNodeNames = (
  value: unknown,
  names: Set<string>,
  variables: { [name: string]: unknown } | null | undefined
): void => {
  if (value === null || typeof value !== 'object') return;
  if (Array.isArray(value)) {
    value.forEach((item) => {
      inputNodeNames(item, names, variables);
    });
    return;
  }
  const node = value as ValueNode;
  if (typeof node.kind === 'string' && 'loc' in node) {
    if (node.kind === Kind.VARIABLE) {
      inputNodeNames(variables?.[node.name.value], names, variables);
    } else if (node.kind === Kind.LIST) {
      inputNodeNames(node.values, names, variables);
    } else if (node.kind === Kind.OBJECT) {
      node.fields.forEach((field) => {
        if (field.name.value === 'accepted_by') {
          const name = argumentValue(field.value, variables);
          if (typeof name === 'string') names.add(name);
        } else {
          inputNodeNames(field.value, names, variables);
        }
      });
    }
    return;
  }
  Object.entries(value as { [key: string]: unknown }).forEach(([key, item]) => {
    if (key === 'accepted_by') {
      if (typeof item === 'string') names.add(item);
    } else {
      inputNodeNames(item, names, variables);
    }
  });
};

/**
 * The node names of an operation: every `node:` and `accepted_by:` argument
 * of every field at any depth (root fields, per-node relationships, opt-in
 * acceptance), and every `accepted_by` inside an argument's input value
 * (the `spent_by` filter), with fragments flattened. The request snapshot
 * pins exactly these nodes.
 */
export const operationNodeNames = (
  document: DocumentNode,
  operationName: string | null | undefined,
  variables: { [name: string]: unknown } | null | undefined
): string[] => {
  const operation: OperationDefinitionNode | null | undefined = getOperationAST(
    document,
    operationName ?? undefined
  );
  const names = new Set<string>();
  const fragments = new Map(
    document.definitions
      .filter((definition) => definition.kind === Kind.FRAGMENT_DEFINITION)
      .map((definition) => [
        (definition as { name: { value: string } }).name.value,
        definition as { selectionSet: OperationDefinitionNode['selectionSet'] },
      ])
  );
  const visit = (
    selectionSet: OperationDefinitionNode['selectionSet'] | undefined,
    seen: Set<string>
  ) => {
    selectionSet?.selections.forEach((selection) => {
      if (selection.kind === Kind.FIELD) {
        selection.arguments?.forEach((argument) => {
          if (
            argument.name.value !== 'node' &&
            argument.name.value !== 'accepted_by'
          ) {
            inputNodeNames(argument.value, names, variables);
            return;
          }
          const value = argumentValue(argument.value, variables);
          if (typeof value === 'string') names.add(value);
        });
        visit(selection.selectionSet, seen);
      } else if (selection.kind === Kind.INLINE_FRAGMENT) {
        visit(selection.selectionSet, seen);
      } else if (!seen.has(selection.name.value)) {
        seen.add(selection.name.value);
        const fragment = fragments.get(selection.name.value);
        if (fragment !== undefined) visit(fragment.selectionSet, seen);
      }
    });
  };
  if (operation !== null && operation !== undefined) {
    visit(operation.selectionSet, new Set());
  }
  return [...names];
};

/**
 * Request-level admission (admission.ts, fix-pass-8.md): a query operation
 * is admitted or refused before any of its statements (or its snapshot
 * read) runs; refused, it answers OVERLOADED (503, Retry-After) at once.
 * Runs after the response cache (a hit costs nothing and is not admitted).
 * The admitted execution runs in a request scope that carries the ticket,
 * so every statement records itself on it (db.ts). An HTTP request's ticket
 * ends when its response closes (`listen`: the response's serialisation is
 * loop time of the request too); any other's when execution is done.
 */
const useAdmission = (db: ApiDb): Plugin<ApiContext> => ({
  onExecute: ({ args, executeFn, setExecuteFn, setResultAndStopExecution }) => {
    const { admission } = db;
    if (!admission.options.enabled) return undefined;
    const document = args.document as DocumentNode;
    const operationName = args.operationName as string | null | undefined;
    const variables = args.variableValues as
      | { [name: string]: unknown }
      | null
      | undefined;
    const operation = getOperationAST(document, operationName ?? undefined);
    if (operation?.operation !== 'query') return undefined;
    /* admitted already (front.ts, or a heavy-lane worker's request) */
    if (requestScope.getStore()?.ticket !== undefined) return undefined;
    let ticket: Ticket;
    try {
      ticket = admission.admit(
        planKeys(
          document,
          operationName,
          variables,
          operationNodeNames(document, operationName, variables)
        )
      );
    } catch (error: unknown) {
      setResultAndStopExecution({ errors: [error as GraphQLError] });
      return undefined;
    }
    const outer = requestScope.getStore();
    const http = outer?.http === true;
    if (http) outer.ticket = ticket;
    const scope = { ...outer, ticket };
    const execute = executeFn as (a: ExecutionArgs) => Promise<unknown>;
    setExecuteFn(async (executeArgs: ExecutionArgs) =>
      requestScope.run(scope, async () => execute(executeArgs))
    );
    return {
      onExecuteDone: () => {
        if (!http) admission.finish(ticket);
      },
    };
  },
});

/**
 * Query operations answered by the fast executor (fast-execute.ts): the
 * response JSON written directly, graphql-js when it gives up. Before
 * `useAdmission`, which wraps it in the request scope.
 */
const useFastExecution = (mode: FastExecutionMode): Plugin<ApiContext> => ({
  onExecute: ({ executeFn, setExecuteFn }) => {
    if (mode === 'off') return;
    const execute = executeFn as (a: ExecutionArgs) => unknown;
    setExecuteFn((async (args: ExecutionArgs) =>
      mode === 'verify'
        ? verifiedExecute(args, execute, print)
        : fastExecute(args, execute)) as never);
  },
});

const useRequestSnapshot = (db: ApiDb): Plugin<ApiContext> => ({
  onExecute: ({ args, extendContext }) => {
    const snapshot = new PinnedSnapshot(
      db,
      operationNodeNames(
        args.document as DocumentNode,
        args.operationName as string | null | undefined,
        args.variableValues as { [name: string]: unknown } | undefined
      )
    );
    requestScope.getStore()?.snapshots?.push(snapshot);
    extendContext({ snapshot });
  },
});

export interface Api {
  db: ApiDb;
  hub: LiveHub;
  yoga: YogaServerInstance<{ [key: string]: unknown }, ApiContext>;
  /** The response cache (R37), when enabled. */
  responseCache?: ResponseCache;
  /** Trusted documents (R34), when configured. */
  trustedDocuments?: TrustedDocuments;
  /** Worker threads answering admitted heavy requests (heavy-lane.ts), when configured. */
  heavyLane?: HeavyLane;
}

export interface ApiOptions {
  live?: LiveHubOptions;
  graphiql?: boolean;
  /** Depth / alias / root-field / field limits (R34); false: none. */
  limits?: Partial<QueryLimits> | false;
  /** Run only trusted documents (or also arbitrary ones), by sha256. */
  trustedDocuments?: TrustedDocuments;
  /** Watermark-keyed response cache (R37); default off. */
  responseCache?: ResponseCacheOptions | false;
  /** The agent's internal API, for send_transaction (R38). */
  agentUrl?: string;
  /** Rows one root statement may return (R33). */
  maxResultRows?: number;
  /** `authchain_branches` per base (default 1,000): above it, AUTHCHAIN_TOO_WIDE. */
  authchainMaxBranches?: number;
  /** Admission control of ClickHouse statements (admission.ts; default on). */
  admission?: AdmissionOptions;
  /** The fast executor (fast-execute.ts; default CHAINGRAPH_API_FAST_EXECUTION, on). */
  fastExecution?: FastExecutionMode;
  /** Server-side time budget per statement class (db.ts `defaultStatementTimeouts`). */
  statementTimeouts?: Partial<StatementTimeouts>;
  /**
   * Stored UTXO set use (sql.ts `defaultUtxoSetPolicy`: on, used when the
   * store has a set for the node; `main.ts` reads `utxoSetPolicyFromEnv`).
   */
  utxoSet?: Partial<UtxoSetPolicy>;
  /**
   * Heavy requests on worker threads (heavy-lane.ts, fix-pass-9.md §3):
   * the ClickHouse connection and thread count; their API instances get
   * this API's statement options. Needs admission (the class decides).
   */
  heavyLane?: Pick<HeavyLaneOptions, 'clickhouse' | 'threads'>;
}

export const createApi = (
  client: ClickHouseClient,
  options: ApiOptions = {}
): Api => {
  const db = new ApiDb(client);
  if (options.maxResultRows !== undefined) {
    db.maxResultRows = options.maxResultRows;
  }
  if (options.admission !== undefined) db.setAdmission(options.admission);
  if (options.authchainMaxBranches !== undefined) {
    db.authchainMaxBranches = options.authchainMaxBranches;
  }
  db.timeouts = { ...db.timeouts, ...options.statementTimeouts };
  db.utxoSetPolicy = { ...db.utxoSetPolicy, ...options.utxoSet };
  const hub = new LiveHub(db, options.live);
  const responseCache =
    options.responseCache === undefined || options.responseCache === false
      ? undefined
      : new ResponseCache(db, options.responseCache);
  const yoga = createYoga<{ [key: string]: unknown }, ApiContext>({
    context: () => ({ agentUrl: options.agentUrl, db, hub }),
    graphiql: options.graphiql ?? false,
    graphqlEndpoint: '/graphql',
    landingPage: false,
    logging: false,
    maskedErrors: false,
    plugins: [
      ...(options.trustedDocuments === undefined
        ? []
        : [useTrustedDocuments(options.trustedDocuments)]),
      ...(options.limits === false
        ? []
        : [useQueryLimits({ ...defaultQueryLimits, ...options.limits })]),
      ...(responseCache === undefined
        ? []
        : [
            responseCache.plugin((args) =>
              operationNodeNames(
                args.document,
                args.operationName,
                args.variableValues
              )
            ),
          ]),
      useFastExecution(options.fastExecution ?? fastExecutionModeFromEnv()),
      useAdmission(db),
      useRequestSnapshot(db),
    ],
    schema,
  });
  const heavyLane =
    options.heavyLane === undefined ||
    options.heavyLane.threads < 1 ||
    options.admission?.enabled !== true
      ? undefined
      : new HeavyLane({
          ...options.heavyLane,
          ...(options.maxResultRows === undefined
            ? {}
            : { maxResultRows: options.maxResultRows }),
          ...(options.authchainMaxBranches === undefined
            ? {}
            : { authchainMaxBranches: options.authchainMaxBranches }),
          ...(options.statementTimeouts === undefined
            ? {}
            : { statementTimeouts: options.statementTimeouts }),
          ...(options.utxoSet === undefined
            ? {}
            : { utxoSet: options.utxoSet }),
          ...(options.limits === undefined ? {} : { limits: options.limits }),
          ...(options.trustedDocuments === undefined
            ? {}
            : { trustedDocuments: options.trustedDocuments }),
        });
  return {
    db,
    hub,
    yoga,
    ...(heavyLane === undefined ? {} : { heavyLane }),
    ...(responseCache === undefined ? {} : { responseCache }),
    ...(options.trustedDocuments === undefined
      ? {}
      : { trustedDocuments: options.trustedDocuments }),
  };
};

export interface RunningServer {
  url: string;
  wsUrl: string;
  close: () => Promise<void>;
}

/** The parts of Yoga's enveloped engine graphql-ws needs. */
interface Enveloped {
  contextFactory: () => Promise<unknown>;
  execute: (args: unknown) => Promise<unknown>;
  parse: (source: string) => DocumentNode;
  schema: GraphQLSchema;
  subscribe: (args: unknown) => Promise<unknown>;
  validate: (
    schema: GraphQLSchema,
    document: DocumentNode
  ) => readonly GraphQLError[];
}

/** Listen on HTTP (queries, SSE) and WebSocket (graphql-ws), same path. */
export const listen = async (
  api: Api,
  port = 0,
  host = '127.0.0.1'
): Promise<RunningServer> => {
  /*
   * Each HTTP request runs in a request scope whose signal aborts when the
   * response closes before it finished (the client went away): its queued
   * statements are dropped and running ones cancelled (db.ts).
   */
  const server = createServer((request, response) => {
    if (request.method === 'GET' && request.url === '/admission') {
      /* the admission gate's state (fix-pass-8.md §3): counters, budgets, loop */
      // eslint-disable-next-line @typescript-eslint/no-magic-numbers, @typescript-eslint/naming-convention
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(JSON.stringify(api.db.admission.snapshot()));
      return;
    }
    const controller = new AbortController();
    const scope: RequestScope = { http: true, signal: controller.signal };
    response.once('close', () => {
      if (!response.writableFinished) {
        controller.abort(new Error('The HTTP client went away.'));
      }
      /* the admitted request ends with its response (serialisation included) */
      if (scope.ticket !== undefined) api.db.admission.finish(scope.ticket);
    });
    requestScope.run(scope, () => {
      if (request.method === 'POST') {
        /* decided before Yoga: cache hit, refusal, light here, heavy on the lane (front.ts) */
        handlePost(api, request, response, scope).catch((error: unknown) => {
          if (response.headersSent || response.destroyed) {
            response.destroy();
            return;
          }
          // eslint-disable-next-line @typescript-eslint/no-magic-numbers, @typescript-eslint/naming-convention
          response.writeHead(500, { 'content-type': 'application/json' });
          response.end(
            JSON.stringify({
              errors: [{ message: `Internal error: ${String(error)}` }],
            })
          );
        });
        return;
      }
      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      api.yoga(request, response);
    });
  });
  const wsServer = new WebSocketServer({
    path: api.yoga.graphqlEndpoint,
    server,
  });
  /* graphql-ws: an operation's statements stop when its socket closes */
  const socketSignals = new WeakMap<object, AbortController>();
  const socketScope = (socket: unknown) => {
    if (typeof socket !== 'object' || socket === null) {
      return { signal: new AbortController().signal };
    }
    const key = socket;
    let controller = socketSignals.get(key);
    if (controller === undefined) {
      const created = new AbortController();
      (socket as { once: (event: string, f: () => void) => void }).once(
        'close',
        () => {
          created.abort(new Error('The WebSocket closed.'));
        }
      );
      socketSignals.set(key, created);
      controller = created;
    }
    return { signal: controller.signal };
  };
  const disposable = useServer(
    {
      execute: async (args) =>
        requestScope.run(
          socketScope(
            (args.contextValue as { socket?: unknown } | undefined)?.socket
          ),
          async () =>
            (
              args.rootValue as {
                execute: (a: typeof args) => Promise<unknown>;
              }
            ).execute(args) as never
        ),
      onSubscribe: async (context, _id, payload) => {
        const enveloped = api.yoga.getEnveloped({
          ...context,
          params: payload,
          req: context.extra.request,
          socket: context.extra.socket,
        }) as unknown as Enveloped;
        let query: string;
        try {
          query =
            api.trustedDocuments === undefined
              ? payload.query
              : resolveTrustedDocument(api.trustedDocuments, payload as never);
        } catch (error) {
          return [error as GraphQLError];
        }
        const args = {
          contextValue: await enveloped.contextFactory(),
          document: enveloped.parse(query),
          operationName: payload.operationName,
          rootValue: {
            execute: enveloped.execute,
            subscribe: enveloped.subscribe,
          },
          schema: enveloped.schema,
          variableValues: payload.variables,
        };
        const errors = enveloped.validate(args.schema, args.document);
        if (errors.length > 0) return [...errors];
        return args;
      },
      subscribe: async (args) =>
        (
          args.rootValue as {
            subscribe: (a: typeof args) => Promise<unknown>;
          }
        ).subscribe(args) as never,
    },
    wsServer
  );
  await new Promise<void>((resolve) => {
    server.listen(port, host, resolve);
  });
  const address = server.address() as AddressInfo;
  return {
    close: async () => {
      api.hub.close();
      await api.heavyLane?.close();
      wsServer.clients.forEach((socket) => {
        socket.terminate();
      });
      // closes the WebSocket server too
      await disposable.dispose();
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) reject(error);
          else resolve();
        });
      });
    },
    url: `http://${host}:${address.port}${api.yoga.graphqlEndpoint}`,
    wsUrl: `ws://${host}:${address.port}${api.yoga.graphqlEndpoint}`,
  };
};
