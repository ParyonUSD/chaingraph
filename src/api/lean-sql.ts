/* eslint-disable functional/no-let, functional/no-loop-statement, no-continue, complexity, @typescript-eslint/no-magic-numbers */
// cspell:ignore subquery subqueries parenthesised PREWHERE optimises
/**
 * Lean statements (fix-pass-14.md §2).
 *
 * The compiler builds each read from parts that nest: a view (a SELECT of
 * the table with the visibility condition), the relationship's source
 * around it (a SELECT of the view with the link condition), the level's
 * filter (a SELECT of the source with `WHERE 1` when the level has none) and
 * its numbering, and every link reads its parent level through two
 * subqueries (the tuple elements of an arrayJoin of the level's array).
 * ClickHouse analyses, plans and optimises every one of those query nodes
 * for every scalar level: on the golden-like store a node-2 point read costs
 * ~2.3 ms as one SELECT and ~4.7 ms as the compiler wrote it, and the
 * median-band documents have four to six such levels. The rows read are the
 * same either way.
 *
 * `leanSql` rewrites a statement into the same query with fewer nodes, by
 * four rules applied until none applies:
 *
 * 1. A parenthesised `SELECT * FROM (X) WHERE 1` or `SELECT * FROM (X)`
 *    becomes `X`, for a SELECT `X`: the same columns, the same rows.
 * 2. `SELECT L FROM (SELECT * FROM t WHERE V) WHERE C` becomes
 *    `SELECT L FROM t WHERE (V) AND (C)` (the outer WHERE optional; the inner
 *    list may add columns of `t` after the star), for a table `t` and an
 *    inner query with nothing but its WHERE (no ORDER BY, LIMIT, GROUP BY,
 *    JOIN, FINAL, SETTINGS …). A select list that is a lone star becomes the
 *    inner list; any other star beside extra inner columns, or an alias of
 *    the outer list that `V` names (it would capture the name), leaves the
 *    statement as it is.
 * 3. An IN argument that reads tuple element n of an arrayJoin of a level's
 *    array through two subqueries becomes
 *    `SELECT tupleElement(arrayJoin(A), n)`: the same set.
 *
 * 4. A SELECT of plain columns from a subquery of plain columns (`projectOnce`)
 *    reads the subquery's source itself.
 *
 * It sees the compiler's own SQL only (values are bound as parameters);
 * quoted text is skipped when matching parentheses. CHAINGRAPH_API_LEAN_SQL
 * = off sends the statements as compiled (db.ts).
 */

/** Words that end a WHERE clause at its own depth. */
const clauseEnds = [
  ' ORDER BY ',
  ' LIMIT ',
  ' GROUP BY ',
  ' HAVING ',
  ' SETTINGS ',
  ' UNION ',
  ' WINDOW ',
  ' QUALIFY ',
  ' FORMAT ',
] as const;

/** Words the inner query of rule 2 must not have at its own depth. */
const innerForbidden = [
  ...clauseEnds,
  ' JOIN ',
  ' PREWHERE ',
  ' FINAL',
  ' SAMPLE ',
  ' INTERSECT ',
  ' EXCEPT ',
] as const;

/** Depth of every position of `sql` (parentheses outside quotes); -1 inside quotes. */
const depthsOf = (sql: string) => {
  const depth = new Int32Array(sql.length + 1);
  let level = 0;
  let quote = '';
  for (let index = 0; index < sql.length; index += 1) {
    const char = sql[index]!;
    if (quote !== '') {
      depth[index] = -1;
      if (char === '\\' && index + 1 < sql.length) {
        index += 1;
        depth[index] = -1;
      } else if (char === quote) {
        quote = '';
      }
    } else if (char === "'" || char === '`' || char === '"') {
      quote = char;
      depth[index] = -1;
    } else if (char === '(') {
      depth[index] = level;
      level += 1;
    } else if (char === ')') {
      level -= 1;
      depth[index] = level;
    } else {
      depth[index] = level;
    }
  }
  depth[sql.length] = level;
  return depth;
};

/** One statement text with the depth of each position. */
class Scan {
  readonly sql: string;

  readonly depth: Int32Array;

  constructor(sql: string) {
    this.sql = sql;
    this.depth = depthsOf(sql);
  }

  /** The parenthesis closing the one at `open`, or -1. */
  closing(open: number) {
    const level = this.depth[open]!;
    for (let index = open + 1; index < this.sql.length; index += 1) {
      if (this.sql[index] === ')' && this.depth[index] === level) return index;
    }
    return -1;
  }

  /** The first position in `range` at depth `level` where one of `words` starts, or -1. */
  find(
    range: { from: number; to: number; level: number },
    words: readonly string[]
  ) {
    for (let index = range.from; index < range.to; index += 1) {
      if (this.depth[index] !== range.level) continue;
      if (words.some((word) => this.sql.startsWith(word, index))) return index;
    }
    return -1;
  }

  /** Where a WHERE clause from `from` (at depth `level`) ends: a clause word, or the enclosing `)`. */
  clauseEnd(from: number, level: number) {
    for (let index = from; index < this.sql.length; index += 1) {
      const at = this.depth[index]!;
      if (at === -1) continue;
      if (at < level) return index;
      if (
        at === level &&
        clauseEnds.some((word) => this.sql.startsWith(word, index))
      ) {
        return index;
      }
    }
    return this.sql.length;
  }

  /** Aliases (`AS name`) in `range` at its depth. */
  aliases(range: { from: number; to: number; level: number }) {
    const names: string[] = [];
    for (let index = range.from; index < range.to; index += 1) {
      if (
        this.depth[index] !== range.level ||
        !this.sql.startsWith(' AS ', index)
      ) {
        continue;
      }
      const name = /^\w+/u.exec(this.sql.slice(index + 4, index + 132));
      if (name !== null) names.push(name[0]);
    }
    return names;
  }

  /** Whether `range` has a `*` at its depth (a select list's own star; `count(*)` is deeper). */
  hasStar(range: { from: number; to: number; level: number }) {
    for (let index = range.from; index < range.to; index += 1) {
      if (this.depth[index] === range.level && this.sql[index] === '*') {
        return true;
      }
    }
    return false;
  }

  /** The start of the `SELECT ` owning the clause at `at` (the same depth), or -1. */
  ownerSelect(at: number) {
    const level = this.depth[at]!;
    for (let index = at - 1; index >= 0; index -= 1) {
      const here = this.depth[index]!;
      if (here === -1) continue;
      if (here < level) return -1;
      if (here === level && this.sql.startsWith('SELECT ', index)) {
        const before = index === 0 ? ' ' : this.sql[index - 1]!;
        if (before === '(' || before === ' ' || before === '\n') return index;
      }
    }
    return -1;
  }
}

/** Rule 1: `(SELECT * FROM (X) WHERE 1)` / `(SELECT * FROM (X))` → `(X)`. */
const unwrapOnce = (scan: Scan): string | undefined => {
  const { sql } = scan;
  const head = 'SELECT * FROM (';
  for (let at = sql.indexOf(head); at >= 0; at = sql.indexOf(head, at + 1)) {
    if (scan.depth[at] === -1 || at === 0 || sql[at - 1] !== '(') continue;
    const open = at + head.length - 1;
    const close = scan.closing(open);
    if (close < 0) continue;
    const inner = sql.slice(open + 1, close);
    if (!inner.startsWith('SELECT ')) continue;
    if (sql.startsWith(' WHERE 1)', close + 1)) {
      return `${sql.slice(0, at)}${inner}${sql.slice(close + 1 + 8)}`;
    }
    if (sql[close + 1] === ')') {
      return `${sql.slice(0, at)}${inner}${sql.slice(close + 1)}`;
    }
  }
  return undefined;
};

const viewPattern =
  /^SELECT \*(?<extras>(?:, \w+)*) FROM (?<table>\w+) WHERE /u;

/** Rule 2: a view-like subquery (`SELECT *[, c…] FROM t WHERE V`) merged into the query reading it. */
const inlineOnce = (scan: Scan): string | undefined => {
  const { sql } = scan;
  const head = ' FROM (SELECT *';
  for (let at = sql.indexOf(head); at >= 0; at = sql.indexOf(head, at + 1)) {
    const level = scan.depth[at]!;
    if (level === -1) continue;
    const open = at + 6;
    const close = scan.closing(open);
    if (close < 0) continue;
    const inner = sql.slice(open + 1, close);
    const view = viewPattern.exec(inner);
    if (view?.groups === undefined) continue;
    const innerRange = { from: open + 1, level: level + 1, to: close };
    /* a table read with a WHERE and nothing else */
    if (
      scan.find(innerRange, innerForbidden) >= 0 ||
      scan.find({ ...innerRange, from: open + 1 + view[0].length }, [
        ' WHERE ',
      ]) >= 0
    ) {
      continue;
    }
    const after = close + 1;
    const conditioned = sql.startsWith(' WHERE ', after);
    if (
      !conditioned &&
      after < sql.length &&
      sql[after] !== ')' &&
      !clauseEnds.some((word) => sql.startsWith(word, after))
    ) {
      /* an alias, a JOIN …: left as it is */
      continue;
    }
    const restAt = conditioned ? scan.clauseEnd(after + 7, level) : after;
    const select = scan.ownerSelect(at);
    if (select < 0) continue;
    const listRange = { from: select + 7, level, to: at };
    const list = sql.slice(listRange.from, at);
    const extras = view.groups.extras ?? '';
    let newList = list;
    if (scan.hasStar(listRange)) {
      if (list === '*') newList = `*${extras}`;
      else if (extras !== '') continue;
    }
    const where = inner.slice(view[0].length);
    const named = new Set(where.match(/[A-Za-z_]\w*/gu) ?? []);
    if (scan.aliases(listRange).some((name) => named.has(name))) continue;
    const condition = conditioned
      ? `(${where}) AND (${sql.slice(after + 7, restAt)})`
      : where;
    return `${sql.slice(0, listRange.from)}${newList} FROM ${
      view.groups.table ?? ''
    } WHERE ${condition}${sql.slice(restAt)}`;
  }
  return undefined;
};

const linkOne =
  /^SELECT tupleElement\((?<row>__\w+), (?<at>\d+)\)(?: AS \w+)? FROM \(SELECT arrayJoin\((?<level>__\w+)\) AS (?<alias>__\w+)\)$/u;

const linkOf =
  /^SELECT (?<column>\w+) FROM \(SELECT (?<items>(?:tupleElement\(__r, \d+\) AS \w+, )*tupleElement\(__r, \d+\) AS \w+) FROM \(SELECT arrayJoin\((?<level>__\w+)\) AS __r\)\)$/u;

const linkItem = /tupleElement\(__r, (?<at>\d+)\) AS (?<name>\w+)/gu;

const columnList = /^(?:\w+, )*\w+$/u;

/**
 * Rule 4: a SELECT of plain columns from a subquery that selects plain
 * columns (a superset) reads the subquery's source itself:
 * `SELECT a FROM (SELECT a, b FROM X R)` becomes `SELECT a FROM X R`. The
 * outer may add ORDER BY / LIMIT … only when the inner has none of its own,
 * and no WHERE.
 */
const projectOnce = (scan: Scan): string | undefined => {
  const { sql } = scan;
  const head = ' FROM (SELECT ';
  for (let at = sql.indexOf(head); at >= 0; at = sql.indexOf(head, at + 1)) {
    const level = scan.depth[at]!;
    if (level === -1) continue;
    const open = at + 6;
    const close = scan.closing(open);
    if (close < 0) continue;
    const after = close + 1;
    const trailing =
      after < sql.length &&
      sql[after] !== ')' &&
      (sql.startsWith(' WHERE ', after) ||
        !clauseEnds.some((word) => sql.startsWith(word, after)));
    if (trailing) continue;
    const clauses = after < sql.length && sql[after] !== ')';
    const inner = sql.slice(open + 1, close);
    const from = scan.find({ from: open + 8, level: level + 1, to: close }, [
      ' FROM ',
    ]);
    if (from < 0) continue;
    const innerList = sql.slice(open + 8, from);
    if (!columnList.test(innerList)) continue;
    /* the outer's ORDER BY / LIMIT … may follow the inner's WHERE, not its own ORDER BY / LIMIT … */
    if (
      clauses &&
      scan.find({ from, level: level + 1, to: close }, clauseEnds) >= 0
    ) {
      continue;
    }
    const select = scan.ownerSelect(at);
    if (select < 0) continue;
    const outerList = sql.slice(select + 7, at);
    if (!columnList.test(outerList)) continue;
    const available = new Set(innerList.split(', '));
    if (!outerList.split(', ').every((name) => available.has(name))) continue;
    return `${sql.slice(0, at)} ${inner.slice(from - open - 1 + 1)}${sql.slice(
      close + 1
    )}`;
  }
  return undefined;
};

/** Rule 3: a link's set read straight from its level's array. */
const linkOnce = (scan: Scan): string | undefined => {
  const { sql } = scan;
  const head = ' IN (SELECT ';
  for (let at = sql.indexOf(head); at >= 0; at = sql.indexOf(head, at + 1)) {
    if (scan.depth[at] === -1) continue;
    const open = at + 4;
    const close = scan.closing(open);
    if (close < 0) continue;
    const inner = sql.slice(open + 1, close);
    const read = (level: string, position: string) =>
      `${sql.slice(
        0,
        open + 1
      )}SELECT tupleElement(arrayJoin(${level}), ${position})${sql.slice(
        close
      )}`;
    const one = linkOne.exec(inner)?.groups;
    if (
      one?.row !== undefined &&
      one.row === one.alias &&
      one.level !== undefined &&
      one.at !== undefined
    ) {
      return read(one.level, one.at);
    }
    const of = linkOf.exec(inner)?.groups;
    if (of?.items !== undefined && of.level !== undefined) {
      const item = [...of.items.matchAll(linkItem)].find(
        (match) => match.groups?.name === of.column
      );
      if (item?.groups?.at !== undefined) return read(of.level, item.groups.at);
    }
  }
  return undefined;
};

const leanCache = new Map<string, string>();
const leanCacheMax = 2_000;

/** The statement with fewer query nodes (the rules above): the same query. Memoised per text. */
export const leanSql = (sql: string): string => {
  const cached = leanCache.get(sql);
  if (cached !== undefined) return cached;
  let text = sql;
  for (let rounds = 0; rounds < 1_000; rounds += 1) {
    const scan = new Scan(text);
    const next =
      unwrapOnce(scan) ??
      inlineOnce(scan) ??
      projectOnce(scan) ??
      linkOnce(scan);
    if (next === undefined) break;
    text = next;
  }
  if (leanCache.size >= leanCacheMax) leanCache.clear();
  leanCache.set(sql, text);
  return text;
};
