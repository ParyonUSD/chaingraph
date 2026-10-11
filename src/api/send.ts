/* eslint-disable camelcase, @typescript-eslint/naming-convention, complexity, functional/no-throw-statement */
// cspell:ignore clickhouse
/**
 * R38 `send_transaction`: relayed to a node over P2P by the agent, which
 * holds the broadcast connections (its internal API's `/send-transaction`,
 * the endpoint Chaingraph v1's Hasura action called). The API resolves the
 * node name to its internal id and forwards the encoded transaction.
 */
import { GraphQLError } from 'graphql';

export interface SendTransactionResult {
  transaction_hash: string;
  validation_success: boolean;
  validation_error_message: string | null;
  transmission_success: boolean;
  transmission_error_message: string | null;
}

const timeoutMs = 3_000;

export const sendTransaction = async (
  agentUrl: string | undefined,
  nodeInternalId: number,
  encodedHex: string
): Promise<SendTransactionResult> => {
  if (agentUrl === undefined || agentUrl === '') {
    throw new GraphQLError(
      'send_transaction is not configured (CHAINGRAPH_API_AGENT_URL: the agent internal API).',
      { extensions: { code: 'NOT_CONFIGURED' } }
    );
  }
  const response = await fetch(
    `${agentUrl.replace(/\/$/u, '')}/send-transaction`,
    {
      body: JSON.stringify({
        action: { name: 'send_transaction' },
        input: {
          request: {
            encoded_hex: encodedHex,
            node_internal_id: nodeInternalId,
          },
        },
        request_query: '',
        session_variables: { 'x-hasura-role': 'public' },
      }),
      headers: { 'content-type': 'application/json' },
      method: 'POST',
      signal: AbortSignal.timeout(timeoutMs),
    }
  );
  const body = (await response.json()) as Partial<SendTransactionResult> & {
    message?: string;
  };
  if (!response.ok || body.transaction_hash === undefined) {
    throw new GraphQLError(
      `send_transaction failed: ${body.message ?? response.statusText}`,
      { extensions: { code: 'SEND_FAILED' } }
    );
  }
  return {
    transaction_hash: body.transaction_hash,
    transmission_error_message: body.transmission_error_message ?? null,
    transmission_success: body.transmission_success === true,
    validation_error_message: body.validation_error_message ?? null,
    validation_success: body.validation_success === true,
  };
};
