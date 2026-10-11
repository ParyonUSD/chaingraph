// cspell:ignore clickhouse inlinable inlines
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import test from 'ava';

import { pinnedViewStatement, viewsDdl, viewsOutside050 } from './view-ddl.js';
import { inlinablePinnedViews, pinnedViewDefinitions } from './visibility.js';

const ddl = (file: string) =>
  readFileSync(
    fileURLToPath(
      new URL(`../../../src/store/clickhouse/ddl/${file}`, import.meta.url)
    ),
    'utf8'
  );

test('view DDL: the committed 050_views.sql equals the text generated from the gate definition (yarn clickhouse:views:generate)', (t) => {
  t.is(ddl('050_views.sql'), viewsDdl());
});

test('view DDL: spend_at and authchain_link_at in 070/080 equal their generated statements', (t) => {
  viewsOutside050.forEach(([file, names]) => {
    const text = ddl(file);
    names.forEach((name) => {
      t.true(
        text.includes(pinnedViewStatement(name)),
        `${file} must contain the generated ${name}:\n${pinnedViewStatement(
          name
        )}`
      );
    });
  });
});

test('view DDL: every pinned view the API inlines is defined in the DDL', (t) => {
  const all = `${ddl('050_views.sql')}${viewsOutside050
    .map(([file]) => ddl(file))
    .join('')}`;
  inlinablePinnedViews.forEach((name) => {
    t.true(all.includes(`CREATE OR REPLACE VIEW cg.${name} AS`), name);
  });
  t.is(Object.keys(pinnedViewDefinitions).length, inlinablePinnedViews.length);
});
