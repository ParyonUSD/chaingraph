FROM hasura/graphql-engine@sha256:010af30d3887580dd4df57a375facd9abde2bd4c53d20641a0d3121cfa88e48a
ARG SOURCE_REVISION
LABEL org.opencontainers.image.revision=$SOURCE_REVISION io.chaingraph.build.profile="no-array-baseline"
COPY hasura-data /hasura-data
COPY no-array-build-manifest.json /hasura-data/no-array-build-manifest.json
ENV HASURA_GRAPHQL_MIGRATIONS_DIR=/hasura-data/migrations HASURA_GRAPHQL_METADATA_DIR=/hasura-data/metadata
