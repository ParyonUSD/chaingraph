// cspell:ignore Pothos
/**
 * The v2 schema as SDL (R23): sorted, so the committed file
 * (docs/clickhouse-port/v2-schema.graphql) changes only when the schema does.
 * schema.spec.ts fails on any difference: review the change, then
 * `yarn api:schema:update`.
 */
import { lexicographicSortSchema, printSchema } from 'graphql';

import { schema } from './schema.js';

export const schemaSdl = () =>
  `${printSchema(lexicographicSortSchema(schema))}\n`;
