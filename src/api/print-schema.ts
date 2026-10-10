/** `yarn api:schema`: print the spike's SDL (for typed clients, R23). */
import { printSchema } from 'graphql';

import { schema } from './schema.js';

// eslint-disable-next-line no-console
console.log(printSchema(schema));
