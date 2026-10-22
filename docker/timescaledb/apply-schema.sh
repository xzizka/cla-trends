#!/bin/sh
# Apply schema, continuous aggregates, policies and roles/grants to a running
# TimescaleDB. Every file is idempotent, so this runs on every
# `docker compose up` (service `db-init`), not only on first start.
#
# Connection: standard libpq env (PGHOST, PGUSER, PGPASSWORD, PGDATABASE).
set -eu

: "${RECEIVER_DB_PASSWORD:?must be set}"
: "${IMPORTER_DB_PASSWORD:?must be set}"
: "${GRAFANA_RO_DB_PASSWORD:?must be set}"

tries=0
until pg_isready -q; do
    tries=$((tries + 1))
    [ "$tries" -ge 60 ] && { echo "database not ready after 120s" >&2; exit 1; }
    sleep 2
done

# CREATE EXTENSION timescaledb can make the server restart once and drop the
# connection mid-file on a brand-new volume; retry instead of failing.
apply() {
    n=0
    until psql -v ON_ERROR_STOP=1 -q "$@"; do
        n=$((n + 1))
        [ "$n" -ge 6 ] && { echo "giving up on: $*" >&2; exit 1; }
        echo "retrying in 5s: $*" >&2
        sleep 5
    done
}

for f in 01-schema.sql 02-continuous-aggregates.sql 03-policies.sql; do
    echo "applying $f"
    apply -f "/opt/sql-init/$f"
done

echo "applying 04-roles-grants.sql"
apply -v receiver_password="$RECEIVER_DB_PASSWORD" \
      -v importer_password="$IMPORTER_DB_PASSWORD" \
      -v grafana_ro_password="$GRAFANA_RO_DB_PASSWORD" \
      -f /opt/sql-init/04-roles-grants.sql
echo "schema up to date"
