/**
 * @deprecated Moved to `src/store/postgres/postgres-store.ts`; use
 * `createStore()` from `src/store/index.ts`. This re-export shim only exists so
 * in-flight branches and the Postgres-only tests keep importing `../db.js`.
 */
export * from './store/postgres/postgres-store.js';
