/* eslint-disable functional/no-let, functional/no-try-statement, complexity, @typescript-eslint/no-magic-numbers, functional/no-loop-statement, @typescript-eslint/init-declarations, max-params, no-await-in-loop */
// cspell:ignore clickhouse serialisation serialised recognise
/**
 * The HTTP front path of a POSTed GraphQL query (fix-pass-9.md §3).
 *
 * Before Yoga sees the request, the main thread reads the body, parses the
 * document (cached by its text) and decides on it:
 * - **cache hit** (the watermark-keyed response cache, plugins.ts): the
 *   stored body is written at once; no admission, no execution;
 * - **refusal**: OVERLOADED (503, Retry-After) at once; no GraphQL work;
 * - **admitted light**: Yoga on this thread, with the ticket;
 * - **admitted heavy**: the heavy lane (heavy-lane.ts, worker threads) when
 *   configured, so its decoding, execution and serialisation never block
 *   this loop.
 * Anything it does not recognise (no operation, a mutation or
 * subscription, a body that is not one JSON object, a parse error, an
 * Accept the regular result processor does not serve) goes to Yoga as
 * before, which admits queries itself (server.ts `useAdmission`).
 */
import type { IncomingMessage, ServerResponse } from 'node:http';

import type { DocumentNode, GraphQLError } from 'graphql';
import { getOperationAST, parse } from 'graphql';
import LRUCache from 'lru-cache';

import type { Ticket } from './admission.js';
import { planKeys } from './admission.js';
import type { RequestScope } from './db.js';
import { resolveTrustedDocument } from './plugins.js';
import type { Api } from './server.js';
import { operationNodeNames } from './server.js';

/** Parsed documents by text (the front path parses before Yoga does). */
const documents = new LRUCache<string, DocumentNode>({ max: 2_000 });

const parseCached = (text: string) => {
  let document = documents.get(text);
  if (document === undefined) {
    document = parse(text, { noLocation: true });
    documents.set(text, document);
  }
  return document;
};

const regularMediaTypes = [
  'application/graphql-response+json',
  'application/json',
] as const;

/**
 * The media type Yoga's regular result processor answers for `accept`
 * (use-result-processor.js: the last match over the accepted types in
 * reverse order, i.e. the first accepted one it serves; utf-8 only), or
 * undefined when only another processor (multipart, SSE) matches.
 */
export const regularMediaType = (accept: string | undefined) => {
  let picked: string | undefined;
  const accepted = (accept ?? '*/*')
    .replace(/\s/gu, '')
    .toLowerCase()
    .split(',')
    .flatMap((item) => {
      const [mediaType, ...params] = item.split(';');
      const charset =
        params.find((param) => param.includes('charset=')) ?? 'charset=utf-8';
      return mediaType === undefined || charset !== 'charset=utf-8'
        ? []
        : [mediaType];
    })
    .reverse();
  for (const asked of accepted) {
    const [askedType, askedSub] = asked.split('/');
    for (const offered of regularMediaTypes) {
      const [type, sub] = offered.split('/');
      if (
        (askedType === '*' || askedType === type) &&
        (askedSub === '*' || askedSub === sub)
      ) {
        picked = offered;
      }
    }
  }
  return picked;
};

const readBody = async (request: IncomingMessage) => {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(chunk as Buffer);
  return Buffer.concat(chunks);
};

const hopByHop = new Set(['connection', 'keep-alive', 'transfer-encoding']);

const send = (
  response: ServerResponse,
  status: number,
  headers: [string, string][],
  body: Uint8Array | string
) => {
  if (response.destroyed || response.writableEnded) return;
  const bytes = typeof body === 'string' ? Buffer.from(body) : body;
  response.writeHead(status, [
    ...headers.filter(([name]) => {
      const lower = name.toLowerCase();
      return lower !== 'content-length' && !hopByHop.has(lower);
    }),
    ['content-length', String(bytes.byteLength)],
  ]);
  response.end(bytes);
};

/**
 * Hand the request (its body read already) to Yoga on this thread. The
 * answer is streamed (SSE subscriptions, incremental delivery), and the
 * request's signal ends it when the client goes away.
 */
const viaYoga = async (
  api: Api,
  request: IncomingMessage,
  response: ServerResponse,
  body: Buffer,
  signal: AbortSignal | undefined
) => {
  const headers: [string, string][] = Object.entries(request.headers).flatMap(
    ([name, value]): [string, string][] =>
      value === undefined || hopByHop.has(name)
        ? []
        : [[name, Array.isArray(value) ? value.join(', ') : value]]
  );
  const answered = await api.yoga.fetch(
    `http://${request.headers.host ?? 'localhost'}${request.url ?? '/'}`,
    {
      body,
      headers,
      method: 'POST',
      ...(signal === undefined ? {} : { signal }),
    }
  );
  const contentType = answered.headers.get('content-type') ?? '';
  const streamed =
    contentType.startsWith('text/event-stream') ||
    contentType.startsWith('multipart/');
  if (!streamed || answered.body === null) {
    send(
      response,
      answered.status,
      [...answered.headers],
      new Uint8Array(await answered.arrayBuffer())
    );
    return;
  }
  if (response.destroyed) return;
  response.writeHead(
    answered.status,
    [...answered.headers].filter(([name]) => !hopByHop.has(name.toLowerCase()))
  );
  response.flushHeaders();
  const reader = answered.body.getReader();
  response.once('close', () => {
    reader.cancel().catch(() => undefined);
  });
  /* the client may go away while the stream runs (the narrowing above does not hold across awaits) */
  const gone = () => response.destroyed;
  for (;;) {
    const { done, value } = await reader.read();
    if (done || gone()) break;
    response.write(value);
  }
  response.end();
};

/** OVERLOADED as Yoga renders it (error extensions without `http`). */
const refusalBody = (error: GraphQLError) => {
  const extensions = Object.fromEntries(
    Object.entries(error.extensions).filter(([name]) => name !== 'http')
  );
  return JSON.stringify({ errors: [{ extensions, message: error.message }] });
};

interface Payload {
  query?: unknown;
  variables?: unknown;
  operationName?: unknown;
  documentId?: unknown;
  extensions?: unknown;
}

/** Answer a POSTed request (listen's handler for POST; `scope` is its request scope). */
export const handlePost = async (
  api: Api,
  request: IncomingMessage,
  response: ServerResponse,
  scope: RequestScope
) => {
  const body = await readBody(request);
  const mediaType = regularMediaType(request.headers.accept);
  let payload: Payload | undefined;
  try {
    const parsed = JSON.parse(body.toString('utf8')) as unknown;
    payload =
      parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)
        ? (parsed as Payload)
        : undefined;
  } catch {
    payload = undefined;
  }
  if (payload === undefined || mediaType === undefined) {
    await viaYoga(api, request, response, body, scope.signal);
    return;
  }
  let text: unknown = payload.query;
  if (api.trustedDocuments !== undefined) {
    try {
      text = resolveTrustedDocument(api.trustedDocuments, payload as never);
    } catch {
      text = undefined;
    }
  }
  let document: DocumentNode | undefined;
  try {
    document = typeof text === 'string' ? parseCached(text) : undefined;
  } catch {
    document = undefined;
  }
  const operationName =
    typeof payload.operationName === 'string'
      ? payload.operationName
      : undefined;
  const operation =
    document === undefined
      ? undefined
      : getOperationAST(document, operationName);
  if (document === undefined || operation?.operation !== 'query') {
    await viaYoga(api, request, response, body, scope.signal);
    return;
  }
  const variables =
    payload.variables !== null &&
    typeof payload.variables === 'object' &&
    !Array.isArray(payload.variables)
      ? (payload.variables as { [name: string]: unknown })
      : undefined;
  const nodeNames = operationNodeNames(document, operationName, variables);
  const contentType: [string, string] = [
    'content-type',
    `${mediaType}; charset=utf-8`,
  ];
  const cache = api.responseCache;
  const cacheKey = cache?.keyOf(document, operationName, variables);
  if (cache !== undefined && cacheKey !== undefined) {
    const hit = await cache.lookupBody(cacheKey, nodeNames);
    if (hit !== undefined) {
      send(response, 200, [contentType], hit);
      return;
    }
    scope.cacheChecked = true;
  }
  const { admission } = api.db;
  if (!admission.options.enabled) {
    await viaYoga(api, request, response, body, scope.signal);
    return;
  }
  let ticket: Ticket;
  try {
    ticket = admission.admit(
      planKeys(document, operationName, variables, nodeNames)
    );
  } catch (error: unknown) {
    const refusal = error as GraphQLError;
    const retryAfter = refusal.extensions.retryAfterSeconds;
    send(
      response,
      503,
      [contentType, ['retry-after', String(retryAfter)]],
      refusalBody(refusal)
    );
    return;
  }
  scope.ticket = ticket;
  const lane = api.heavyLane;
  if (ticket.cls !== 'heavy' || lane === undefined) {
    await viaYoga(api, request, response, body, scope.signal);
    return;
  }
  /* the lane's thread does the work: none of this loop's time is the request's */
  admission.waiting(ticket, 1);
  let reply;
  try {
    reply = await lane.run(
      {
        body: body.toString('utf8'),
        ...(request.headers.accept === undefined
          ? {}
          : { accept: request.headers.accept }),
        settings: ticket.settings,
      },
      scope.signal
    );
  } finally {
    admission.waiting(ticket, -1);
  }
  for (const statement of reply.statements) {
    const record = { ...statement };
    ticket.statements.push(record);
    admission.statementFinished(ticket, record);
  }
  ticket.loopMs += reply.loopMs;
  if (
    cache !== undefined &&
    cacheKey !== undefined &&
    reply.status === 200 &&
    reply.marks !== undefined
  ) {
    /* graphql-js puts errors first: a body starting with data has none */
    const head = Buffer.from(
      reply.body.buffer,
      reply.body.byteOffset,
      Math.min(8, reply.body.byteLength)
    ).toString('latin1');
    if (head === '{"data":') cache.storeBody(cacheKey, reply.marks, reply.body);
  }
  send(response, reply.status, reply.headers, reply.body);
};
