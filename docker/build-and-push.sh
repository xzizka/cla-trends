#!/usr/bin/env bash
# Build the four images of the docker compose deployment and push them to
# Docker Hub. All four share one stack version tag (plus `latest`), so a
# compose file pins the whole system with a single STACK_VERSION.
#
#   docker login                        # once, as a member of $REGISTRY_NS
#   docker/build-and-push.sh            # build + push
#   PUSH=0 docker/build-and-push.sh     # build only
set -euo pipefail

REGISTRY_NS="${REGISTRY_NS:-lojdr}"
STACK_VERSION="${STACK_VERSION:-1.1.0}"
TIMESCALEDB_TAG="${TIMESCALEDB_TAG:-2.31.0-pg18}"
GRAFANA_TAG="${GRAFANA_TAG:-13.2.4}"
PUSH="${PUSH:-1}"

cd "$(dirname "$0")/.."
revision="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"

build() {  # name context dockerfile [build args...]
    local name="$1" ctx="$2" file="$3"; shift 3
    local ref="$REGISTRY_NS/$name"
    echo "==> $ref:$STACK_VERSION"
    docker build --pull -f "$file" "$@" \
        --label "org.opencontainers.image.version=$STACK_VERSION" \
        --label "org.opencontainers.image.revision=$revision" \
        -t "$ref:$STACK_VERSION" -t "$ref:latest" "$ctx"
    if [ "$PUSH" = 1 ]; then
        docker push "$ref:$STACK_VERSION"
        docker push "$ref:latest"
    fi
}

build cla-receiver    roles/receiver/files/app roles/receiver/files/app/Containerfile
build cla-importer    roles/importer/files/app roles/importer/files/app/Containerfile
build cla-timescaledb . docker/timescaledb/Dockerfile --build-arg "TIMESCALEDB_TAG=$TIMESCALEDB_TAG"
build cla-grafana     . docker/grafana/Dockerfile     --build-arg "GRAFANA_TAG=$GRAFANA_TAG"
