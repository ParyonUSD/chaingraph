import { EventEmitter } from 'events';

import test from 'ava';
import type pg from 'pg';

import {
  describePostgresError,
  handlePoolConnectionErrors,
  isPostgresConnectionError,
} from './postgres-errors.js';

const withCode = (code: string, message = 'error') =>
  Object.assign(new Error(message), { code });

test('isPostgresConnectionError: recognizes lost connections', (t) => {
  t.true(
    isPostgresConnectionError(
      withCode('57P01', 'terminating connection due to administrator command')
    )
  );
  t.true(isPostgresConnectionError(withCode('57P03')));
  t.true(isPostgresConnectionError(withCode('08006')));
  t.true(isPostgresConnectionError(withCode('ECONNREFUSED')));
  t.true(isPostgresConnectionError(withCode('ECONNRESET')));
  t.true(
    isPostgresConnectionError(new Error('Connection terminated unexpectedly'))
  );
  t.true(
    isPostgresConnectionError(
      new Error(
        'Client has encountered a connection error and is not queryable'
      )
    )
  );
});

test('isPostgresConnectionError: ignores query and data errors', (t) => {
  t.false(isPostgresConnectionError(withCode('23505', 'duplicate key')));
  t.false(isPostgresConnectionError(withCode('42P01', 'missing relation')));
  t.false(isPostgresConnectionError(new Error('something else')));
  t.false(isPostgresConnectionError(undefined));
  t.false(isPostgresConnectionError('Connection terminated'));
});

test('describePostgresError: includes the code, omits attached state', (t) => {
  const err = Object.assign(withCode('57P01', 'terminating connection'), {
    client: { large: 'state' },
  });
  t.is(describePostgresError(err), '57P01: terminating connection');
  t.is(describePostgresError(new Error('plain')), 'plain');
});

test('handlePoolConnectionErrors: client and pool errors do not throw', (t) => {
  const pool = new EventEmitter();
  const client = new EventEmitter();
  const reported: Error[] = [];
  handlePoolConnectionErrors(pool as unknown as pg.Pool, (err) => {
    reported.push(err);
  });
  pool.emit('connect', client);
  const terminated = withCode('57P01');
  t.notThrows(() => client.emit('error', terminated));
  t.notThrows(() =>
    client.emit('error', new Error('Connection terminated unexpectedly'))
  );
  t.notThrows(() => pool.emit('error', terminated, client));
  t.deepEqual(reported, [terminated]);
});
