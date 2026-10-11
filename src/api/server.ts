/* eslint-disable functional/no-mixed-type, complexity, functional/no-let, @typescript-eslint/init-declarations, functional/no-try-statement */
// cspell:ignore clickhouse serialised Pothos
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
  GraphQLError,
  GraphQLSchema,
  OperationDefinitionNode,
  ValueNode,
} from 'graphql';
import { getOperationAST, Kind } from 'graphql';
import { useServer } from 'graphql-ws/use/ws';
import type { Plugin, YogaServerInstance } from 'graphql-yoga';
import { createYoga } from 'graphql-yoga';
import { WebSocketServer } from 'ws';

import type { ClickHouseClient } from '../store/clickhouse/client.js';

import { ApiDb, PinnedSnapshot } from './db.js';
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

const argumentValue = (
  value: ValueNode,
  variables: { [name: string]: unknown } | null | undefined
): unknown => {
  if (value.kind === Kind.VARIABLE) return variables?.[value.name.value];
  if (value.kind === Kind.STRING) return value.value;
  return undefined;
};

/**
 * The node names of an operation: every `node:` and `accepted_by:` argument
 * of every field at any depth (root fields, per-node relationships, opt-in
 * acceptance), with fragments flattened. The request snapshot pins exactly
 * these nodes.
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

const useRequestSnapshot = (db: ApiDb): Plugin<ApiContext> => ({
  onExecute: ({ args, extendContext }) => {
    extendContext({
      snapshot: new PinnedSnapshot(
        db,
        operationNodeNames(
          args.document as DocumentNode,
          args.operationName as string | null | undefined,
          args.variableValues as { [name: string]: unknown } | undefined
        )
      ),
    });
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
}

export const createApi = (
  client: ClickHouseClient,
  options: ApiOptions = {}
): Api => {
  const db = new ApiDb(client);
  if (options.maxResultRows !== undefined) {
    db.maxResultRows = options.maxResultRows;
  }
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
      useRequestSnapshot(db),
    ],
    schema,
  });
  return {
    db,
    hub,
    yoga,
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
  // eslint-disable-next-line @typescript-eslint/no-misused-promises
  const server = createServer(api.yoga);
  const wsServer = new WebSocketServer({
    path: api.yoga.graphqlEndpoint,
    server,
  });
  const disposable = useServer(
    {
      execute: async (args) =>
        (
          args.rootValue as { execute: (a: typeof args) => Promise<unknown> }
        ).execute(args) as never,
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
