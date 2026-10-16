// cspell:ignore subquery
import test from 'ava';

import { leanSql } from './lean-sql.js';

const view =
  'SELECT *, locking_bytecode_prefix FROM output WHERE commit_seq <= {v:UInt64}';

test('leanSql: a wrapper without a filter, or with WHERE 1, goes (rule 1)', (t) => {
  t.is(
    leanSql('SELECT x FROM (SELECT * FROM (SELECT a FROM t) WHERE 1)'),
    'SELECT x FROM (SELECT a FROM t)'
  );
  t.is(
    leanSql('SELECT x FROM (SELECT * FROM (SELECT a FROM t))'),
    'SELECT x FROM (SELECT a FROM t)'
  );
  /* a real filter or an alias stays */
  t.is(
    leanSql('SELECT x FROM (SELECT * FROM (SELECT a FROM t) WHERE a > 1)'),
    'SELECT x FROM (SELECT * FROM (SELECT a FROM t) WHERE a > 1)'
  );
});

test("leanSql: a view is merged into the query reading it, its condition and the reader's kept apart (rule 2)", (t) => {
  t.is(
    leanSql(
      `SELECT * FROM (SELECT * FROM (${view}) WHERE transaction_hash IN (SELECT h FROM s) OR output_index = 1)`
    ),
    /* the result is a view-like read itself: merged into the outer star as well */
    'SELECT *, locking_bytecode_prefix FROM output WHERE (commit_seq <= {v:UInt64}) AND (transaction_hash IN (SELECT h FROM s) OR output_index = 1)'
  );
  t.is(
    leanSql(
      `SELECT transaction_hash AS __pk0, row_number() OVER (PARTITION BY transaction_hash) AS __rn FROM (${view})`
    ),
    'SELECT transaction_hash AS __pk0, row_number() OVER (PARTITION BY transaction_hash) AS __rn FROM output WHERE commit_seq <= {v:UInt64}'
  );
  /* the reader's clauses after its WHERE stay after the merged one */
  t.is(
    leanSql(
      'SELECT hash FROM (SELECT * FROM transaction WHERE a = 1) WHERE b = 2 ORDER BY hash LIMIT 3'
    ),
    'SELECT hash FROM transaction WHERE (a = 1) AND (b = 2) ORDER BY hash LIMIT 3'
  );
});

test('leanSql: a view is left alone when merging could change the query (rule 2)', (t) => {
  const kept = [
    /* an alias of the reader named by the view's condition: it would capture the name */
    'SELECT any(id) AS id FROM (SELECT * FROM t WHERE id > 1) GROUP BY k',
    /* the inner query has more than a WHERE */
    'SELECT a FROM (SELECT * FROM t WHERE a = 1 ORDER BY a LIMIT 1)',
    'SELECT a FROM (SELECT * FROM t WHERE a = 1 SETTINGS max_threads = 1)',
    'SELECT a FROM (SELECT * FROM t FINAL WHERE a = 1)',
    /* the subquery has an alias or is joined */
    'SELECT x.a FROM (SELECT * FROM t WHERE a = 1) AS x',
    'SELECT a FROM (SELECT * FROM t WHERE a = 1) AS x INNER JOIN u USING (a)',
    /* a star beside other columns while the view adds columns */
    `SELECT *, 1 AS one FROM (${view})`,
    /* not a table */
    'SELECT a FROM (SELECT * FROM (SELECT a FROM t) WHERE a = 1) WHERE a = 2 ORDER BY a',
  ];
  kept.forEach((sql) => {
    t.is(leanSql(sql), sql, sql);
  });
});

test("leanSql: a projection of plain columns reads the inner query's source directly (rule 4)", (t) => {
  t.is(
    leanSql(
      'SELECT groupArray(tuple(a, b)) FROM (SELECT a, b FROM (SELECT a, b, c FROM t WHERE x = 1) ORDER BY a LIMIT 3)'
    ),
    'SELECT groupArray(tuple(a, b)) FROM (SELECT a, b FROM t WHERE x = 1 ORDER BY a LIMIT 3)'
  );
  const kept = [
    /* the inner query orders or limits itself: the outer ORDER BY may not join it */
    'SELECT a FROM (SELECT a, b FROM t ORDER BY b LIMIT 1) ORDER BY a',
    /* an outer WHERE */
    'SELECT a FROM (SELECT a, b FROM t) WHERE b = 1',
    /* a column the inner list does not have, or an expression */
    'SELECT a, b FROM (SELECT a, c FROM t)',
    'SELECT a FROM (SELECT a, b + 1 AS c FROM t)',
  ];
  kept.forEach((sql) => {
    t.is(leanSql(sql), sql, sql);
  });
});

test('leanSql: a link set reads its level array directly, only as an IN argument (rule 3)', (t) => {
  t.is(
    leanSql(
      'SELECT a FROM t WHERE h IN (SELECT tupleElement(__r, 3) AS hash FROM (SELECT arrayJoin(__page) AS __r))'
    ),
    'SELECT a FROM t WHERE h IN (SELECT tupleElement(arrayJoin(__page), 3))'
  );
  t.is(
    leanSql(
      'SELECT a FROM t WHERE id IN (SELECT internal_id FROM (SELECT tupleElement(__r, 3) AS hash, tupleElement(__r, 4) AS internal_id FROM (SELECT arrayJoin(__l2) AS __r)))'
    ),
    'SELECT a FROM t WHERE id IN (SELECT tupleElement(arrayJoin(__l2), 4))'
  );
  t.is(
    leanSql(
      'SELECT a FROM t WHERE h IN (SELECT tupleElement(__w, 1) FROM (SELECT arrayJoin(__cand_window_1) AS __w))'
    ),
    'SELECT a FROM t WHERE h IN (SELECT tupleElement(arrayJoin(__cand_window_1), 1))'
  );
  /* as a FROM source its column name is used: kept */
  const source =
    'SELECT hash FROM (SELECT tupleElement(__r, 3) AS hash FROM (SELECT arrayJoin(__l1) AS __r)) GROUP BY hash';
  t.is(leanSql(source), source);
  const grouped =
    'SELECT a FROM t WHERE h IN (SELECT tupleElement(__r, 2) AS transaction_hash FROM (SELECT arrayJoin(__incl_1) AS __r) GROUP BY transaction_hash HAVING count() > 1)';
  t.is(leanSql(grouped), grouped);
});

test('leanSql: parentheses and words inside quotes are not structure; a lean statement is a fixed point', (t) => {
  const quoted =
    "SELECT * FROM (SELECT * FROM t WHERE s = ') WHERE 1 (') WHERE u = 'x'";
  t.is(
    leanSql(quoted),
    "SELECT * FROM t WHERE (s = ') WHERE 1 (') AND (u = 'x')"
  );
  const lean = leanSql(`SELECT * FROM (SELECT * FROM (${view}) WHERE 1)`);
  t.is(lean, view);
  t.is(leanSql(lean), lean);
});
