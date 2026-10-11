/** `yarn api:schema`: print the v2 SDL (for typed clients, R23). */
import { schemaSdl } from './sdl.js';

process.stdout.write(schemaSdl());
