FROM node@sha256:83f1c388c31fb2e51f7cbd4dea949b96260798c98f206e8e4696bc93bd964e3a AS build-stage
WORKDIR /chaingraph
COPY package.json yarn.lock .yarnrc.yml ./
COPY .yarn .yarn
RUN yarn install --immutable --immutable-cache
COPY tsconfig.json defaults.env ./
COPY bin bin
COPY src src
RUN yarn build
RUN yarn prod-install ./production-dependencies

FROM node@sha256:83f1c388c31fb2e51f7cbd4dea949b96260798c98f206e8e4696bc93bd964e3a AS production
ARG SOURCE_REVISION
LABEL org.opencontainers.image.revision=$SOURCE_REVISION io.chaingraph.build.profile="no-array-baseline"
WORKDIR /chaingraph
COPY --from=build-stage /chaingraph/production-dependencies ./
COPY --from=build-stage /chaingraph/defaults.env ./
COPY --from=build-stage /chaingraph/bin bin
COPY --from=build-stage /chaingraph/build build
COPY no-array-build-manifest.json ./
ENV NODE_ENV=production CHAINGRAPH_OUTPUT_MEMBERSHIP_MODE=baseline
EXPOSE 3200
CMD ["node", "bin/chaingraph.js"]
