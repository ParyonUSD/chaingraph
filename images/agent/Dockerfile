# node:24-alpine pinned by index digest (multi-arch), resolved 2026-10-10 via
# `docker buildx imagetools inspect node:24-alpine`. Bump deliberately.
ARG NODE_IMAGE=node:24-alpine@sha256:ebfe2f90462722a7a4de65e91990e97fe0d401c70e0e762c5b53302f905ec1c1

FROM $NODE_IMAGE AS build-stage
WORKDIR /chaingraph
COPY package.json yarn.lock .yarnrc.yml ./
COPY .yarn .yarn
RUN yarn install --immutable --immutable-cache
COPY tsconfig.json defaults.env ./
COPY bin bin
COPY src src
RUN yarn build
# Use Yarn plugin-production-install to copy only production dependencies (https://gitlab.com/Larry1123/yarn-contrib/-/tree/master/packages/plugin-production-install)
RUN yarn prod-install ./production-dependencies

FROM $NODE_IMAGE AS production
WORKDIR /chaingraph
COPY --from=build-stage /chaingraph/production-dependencies ./
COPY --from=build-stage /chaingraph/defaults.env ./
COPY --from=build-stage /chaingraph/bin bin
COPY --from=build-stage /chaingraph/build build
# ClickHouse DDL next to the compiled applier (ddl-apply.js resolves ./ddl);
# applied by `node bin/chaingraph-clickhouse-ddl.js` (init container / Job).
COPY --from=build-stage /chaingraph/src/store/clickhouse/ddl build/store/clickhouse/ddl
ENV NODE_ENV production
EXPOSE 3200
CMD ["node", "bin/chaingraph.js"]
