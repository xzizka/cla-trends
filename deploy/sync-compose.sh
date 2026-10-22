#!/usr/bin/env bash
# Copy the files the compose deployment shares with the Ansible roles into
# deploy/compose/, so roles/ stays the single source of truth for the schema
# and the dashboards. Run after changing any of them; commit the result.
#
#   deploy/sync-compose.sh           # copy
#   deploy/sync-compose.sh --check   # exit 1 if deploy/compose/ is stale
set -euo pipefail
cd "$(dirname "$0")/.."

pairs=(
    roles/timescaledb/files/init/01-schema.sql:deploy/compose/timescaledb/01-schema.sql
    roles/timescaledb/files/init/02-continuous-aggregates.sql:deploy/compose/timescaledb/02-continuous-aggregates.sql
    roles/timescaledb/files/init/03-policies.sql:deploy/compose/timescaledb/03-policies.sql
)
for f in roles/grafana/files/provisioning/dashboards-json/*.json; do
    pairs+=("$f:deploy/compose/grafana/dashboards/$(basename "$f")")
done

stale=0
for p in "${pairs[@]}"; do
    src="${p%%:*}" dst="${p#*:}"
    if [ "${1:-}" = --check ]; then
        cmp -s "$src" "$dst" || { echo "stale: $dst (from $src)"; stale=1; }
    else
        # 0644: bind-mounted into containers that run as their own uid
        install -m 0644 "$src" "$dst"
    fi
done
exit "$stale"
