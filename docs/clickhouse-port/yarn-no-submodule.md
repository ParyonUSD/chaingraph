# Yarn without the `.yarn` submodule

## Why

Upstream Chaingraph vendored Yarn through a `.yarn` git submodule
(`bitauth/chaingraph-dependencies`): the Yarn 3.3.1 release (`yarnPath`), two plugins and a
zero-install offline cache, installed with `--immutable --immutable-cache`. That broke for
the fork:

- `git archive` and clean clones without `--recurse-submodules` had no Yarn at all.
- New dependencies (`@clickhouse/client@1.24.1`) are not in the upstream dependency repo, so a
  clean submodule checkout failed with `YN0056 Cache entry required but missing`; the image
  could only be built by copying a developer's working `.yarn` into the build context.
- The superproject config pointed the submodule URL at a local path from the initial setup.

## What changed

| File | Change |
| --- | --- |
| `.gitmodules`, `.yarn` gitlink | removed |
| `.yarn/plugins/@yarnpkg/*.cjs` | tracked as plain files (same bytes as the submodule's; `plugin-production-install` runs `yarn prod-install` in the Dockerfile) |
| `.yarnrc.yml` | no `yarnPath`; `enableGlobalCache: true`; `nodeLinker: node-modules` and plugins unchanged |
| `package.json` | unchanged: `"packageManager": "yarn@3.3.1"` is what Corepack reads |
| `.gitignore` | `.yarn/cache`, `.yarn/install-state.gz`, `.yarn/unplugged`, `.yarn/build-state.yml`, `.pnp.*` |
| `images/agent/Dockerfile` | `corepack enable && corepack prepare yarn@3.3.1 --activate`; copies only `.yarn/plugins`; `yarn install --immutable`; `node:24-alpine` pinned by digest |
| `.github/workflows/ci.yaml` | Node 24, `corepack enable`, no `submodules: true`, `yarn install --immutable` (`--check-cache` kept in the lockfile job) |

`yarn.lock` did not change: `yarn install --immutable` against the registry and the global
cache succeeds with the existing lockfile and checksums.

## Install from a clean clone

```sh
git clone <repo> && cd chaingraph
corepack enable            # Node 24 bundles Corepack; on Node >= 25: npm i -g corepack
yarn --version             # 3.3.1
yarn install --immutable
```

Installs are no longer offline: they need the npm registry (packages land in
`~/.yarn/berry/cache`). An existing checkout that still has the submodule's `.yarn/cache` or
`.yarn/releases` can delete them; both are ignored now.

## Verified (2026-10-10)

- Local, Node v24.14.1, Corepack 0.34.6: `yarn --version` 3.3.1; `yarn install --immutable`
  ok in 26 s, `yarn.lock` unchanged; `import('@clickhouse/client')` ok.
- Image from a `git archive HEAD` export with no `.yarn` release/cache: see `image.md`
  ("Verification without the `.yarn` submodule").
