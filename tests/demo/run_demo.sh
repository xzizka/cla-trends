#!/usr/bin/env bash
# End-to-end demo of the importer against REAL source endpoints.
#
# Brings up a disposable stack on a private Docker network:
#
#   demo-timescaledb   the real project image + the real 01-schema.sql
#   demo-prometheus    scrapes itself -> genuine live Prometheus metrics
#   demo-influxdb1     InfluxDB 1.8, HTTP auth ON (exercises basic auth)
#   demo-influxdb2     InfluxDB 2.7, token auth (exercises Flux + annotated CSV)
#   demo-importer      the importer image built from roles/importer/files/app
#
# then seeds the two Influxes with backdated self-verifying points, waits for
# the importer's first pass, RESTARTS the importer several times (each restart
# re-imports immediately, which is what would expose a broken watermark as
# duplicate rows), and finally asserts the contents of metrics_raw.
#
# Everything is namespaced `demo-*` and torn down by `--clean`. It touches
# nothing belonging to the real stack and needs no vault access.
#
#   ./tests/demo/run_demo.sh          # full run, leaves the stack up
#   ./tests/demo/run_demo.sh --clean  # tear everything down
#   ./tests/demo/run_demo.sh --verify-only
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
APP="$REPO/roles/importer/files/app"
NET=importer-demo

TS_IMAGE=timescale/timescaledb:2.29.2-pg18
PROM_IMAGE=prom/prometheus:latest
IFX1_IMAGE=influxdb:1.8
IFX2_IMAGE=influxdb:2.7
IMPORTER_IMAGE=metrics-importer:demo

# Host ports are deliberately non-default so the demo cannot collide with a
# real service on the workstation.
TS_PORT=15432
PROM_PORT=19090
IFX1_PORT=18086
IFX2_PORT=28086
STATUS_PORT=19202

SUPER_PW=demo-super-pw
IMPORTER_PW=demo-importer-pw
IFX2_TOKEN=demo-influx2-token-0123456789

# How many minutes of history to seed, and how long to let Prometheus scrape
# itself before importing.
HOURS_BACK=2
PROM_WARMUP_S=45
RESTARTS=3

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
info() { printf '   %s\n' "$*"; }
die()  { printf '\n\033[31mFAILED: %s\033[0m\n' "$*" >&2; exit 1; }

clean() {
  say "Tearing down the demo stack"
  docker rm -f demo-importer demo-prometheus demo-influxdb1 demo-influxdb2 \
                demo-timescaledb >/dev/null 2>&1 || true
  docker volume rm -f demo-timescaledb-data demo-influxdb2-data >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  info "removed demo-* containers, volumes and the $NET network"
}

# Wait until a command succeeds, or give up with the last output.
wait_for() {
  local label="$1" tries="$2"; shift 2
  info "waiting for $label ..."
  for _ in $(seq "$tries"); do
    if "$@" >/dev/null 2>&1; then info "$label ready"; return 0; fi
    sleep 2
  done
  "$@" || true
  die "$label did not become ready"
}

case "${1:-}" in
  --clean) clean; exit 0 ;;
  --verify-only) VERIFY_ONLY=1 ;;
  "") VERIFY_ONLY=0 ;;
  *) die "unknown argument: $1" ;;
esac

BUCKETS=$(( HOURS_BACK * 60 ))

if [ "$VERIFY_ONLY" = "0" ]; then
clean

say "Creating the demo network"
docker network create "$NET" >/dev/null
info "$NET"

# ---------------------------------------------------------------------------
say "Starting TimescaleDB and applying the project's real schema"
docker run -d --name demo-timescaledb --network "$NET" \
  -e POSTGRES_DB=metrics -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD="$SUPER_PW" -e TIMESCALEDB_TELEMETRY=off \
  -v demo-timescaledb-data:/var/lib/postgresql \
  -v "$REPO/roles/timescaledb/files/init:/opt/sql-init:ro" \
  -v "$HERE:/opt/demo:ro" \
  -p "127.0.0.1:$TS_PORT:5432" "$TS_IMAGE" >/dev/null

wait_for "TimescaleDB" 60 docker exec demo-timescaledb pg_isready -U postgres -d metrics

# 01-schema.sql is the REAL file: same hypertable, same metrics_raw, same
# importer_state. CREATE EXTENSION restarts postgres once, hence the retries
# (exactly the race documented in bug-014/015).
for f in 01-schema.sql 02-continuous-aggregates.sql 03-policies.sql; do
  for attempt in 1 2 3 4 5 6; do
    if docker exec -i demo-timescaledb psql -U postgres -d metrics \
         -v ON_ERROR_STOP=1 -q -f "/opt/sql-init/$f" >/dev/null 2>&1; then
      info "applied $f"; break
    fi
    [ "$attempt" = 6 ] && die "could not apply $f"
    sleep 3
  done
done
docker exec -i demo-timescaledb psql -U postgres -d metrics \
  -v ON_ERROR_STOP=1 -q -f /opt/demo/grants.sql >/dev/null
info "applied grants.sql (importer role: INSERT on metrics_raw only)"

# ---------------------------------------------------------------------------
say "Starting Prometheus (scraping itself)"
docker run -d --name demo-prometheus --network "$NET" \
  -v "$HERE/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  -p "127.0.0.1:$PROM_PORT:9090" "$PROM_IMAGE" >/dev/null
wait_for "Prometheus" 45 \
  curl -fsS "http://127.0.0.1:$PROM_PORT/-/ready"

# ---------------------------------------------------------------------------
say "Starting InfluxDB 1.8 (HTTP auth enabled)"
docker run -d --name demo-influxdb1 --network "$NET" \
  -e INFLUXDB_HTTP_AUTH_ENABLED=true \
  -e INFLUXDB_DB=demo \
  -e INFLUXDB_ADMIN_USER=admin \
  -e INFLUXDB_ADMIN_PASSWORD=demo-admin-pw \
  -e INFLUXDB_USER=demo_writer \
  -e INFLUXDB_USER_PASSWORD=demo-writer-pw \
  -e INFLUXDB_READ_USER=demo_reader \
  -e INFLUXDB_READ_USER_PASSWORD=demo-reader-pw \
  -p "127.0.0.1:$IFX1_PORT:8086" "$IFX1_IMAGE" >/dev/null
wait_for "InfluxDB 1.8" 45 \
  curl -fsS "http://127.0.0.1:$IFX1_PORT/ping"

# ---------------------------------------------------------------------------
say "Starting InfluxDB 2.7 (token auth)"
docker run -d --name demo-influxdb2 --network "$NET" \
  -e DOCKER_INFLUXDB_INIT_MODE=setup \
  -e DOCKER_INFLUXDB_INIT_USERNAME=admin \
  -e DOCKER_INFLUXDB_INIT_PASSWORD=demo-admin-pw \
  -e DOCKER_INFLUXDB_INIT_ORG=demo-org \
  -e DOCKER_INFLUXDB_INIT_BUCKET=demo-bucket \
  -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN="$IFX2_TOKEN" \
  -v demo-influxdb2-data:/var/lib/influxdb2 \
  -p "127.0.0.1:$IFX2_PORT:8086" "$IFX2_IMAGE" >/dev/null
wait_for "InfluxDB 2.7" 60 \
  curl -fsS "http://127.0.0.1:$IFX2_PORT/health"

# ---------------------------------------------------------------------------
say "Seeding the Influx instances with $HOURS_BACK h of backdated points"
python3 "$HERE/seed.py" \
  --influx1-url "http://127.0.0.1:$IFX1_PORT" \
  --influx2-url "http://127.0.0.1:$IFX2_PORT" \
  --hours-back "$HOURS_BACK"

# ---------------------------------------------------------------------------
say "Building the importer image"
docker build -q -t "$IMPORTER_IMAGE" -f "$APP/Containerfile" "$APP" >/dev/null
info "$IMPORTER_IMAGE built from roles/importer/files/app"

say "Letting Prometheus accumulate ${PROM_WARMUP_S}s of self-scrape data"
sleep "$PROM_WARMUP_S"
samples=$(curl -fsS "http://127.0.0.1:$PROM_PORT/api/v1/query?query=count(up)" \
          | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["data"]["result"][0]["value"][1] if d["data"]["result"] else 0)')
info "Prometheus reports $samples target(s) up"

# ---------------------------------------------------------------------------
say "Starting the importer against all three sources"
docker run -d --name demo-importer --network "$NET" \
  -e IMPORTER_CONFIG=/etc/importer/sources.yml \
  -e IMPORTER_STATUS_PORT=9202 \
  -e IMPORTER_PG_DSN="postgresql://importer@demo-timescaledb:5432/metrics" \
  -e PGPASSWORD="$IMPORTER_PW" \
  -e LOG_LEVEL=INFO \
  -v "$HERE/sources.yml:/etc/importer/sources.yml:ro" \
  -p "127.0.0.1:$STATUS_PORT:9202" "$IMPORTER_IMAGE" >/dev/null

wait_for "importer /healthz" 30 curl -fsS "http://127.0.0.1:$STATUS_PORT/healthz"

# The first pass has to finish before the DB means anything. Poll /status
# until every stream has reported a result rather than sleeping a guess.
info "waiting for the first import pass to finish on all 5 streams ..."
for _ in $(seq 60); do
  done_n=$(curl -fsS "http://127.0.0.1:$STATUS_PORT/status" \
    | python3 -c 'import json,sys; s=json.load(sys.stdin)["streams"]; print(sum(1 for v in s.values() if v.get("last_result")))' 2>/dev/null || echo 0)
  [ "${done_n:-0}" -ge 5 ] && break
  sleep 2
done
info "streams reporting a result: ${done_n:-0}/5"

rows_after_first=$(docker exec -i demo-timescaledb psql -U postgres -d metrics -tAc \
  "SELECT count(*) FROM metrics_raw WHERE labels ? 'source'")
info "rows in metrics_raw after the first pass: $rows_after_first"

# ---------------------------------------------------------------------------
# The duplicate test. run_on_start=true means every restart imports again
# straight away; if the watermark were inclusive, or windows overlapped by a
# step, this is where duplicate rows would appear.
say "Restarting the importer ${RESTARTS}x to prove re-imports do not duplicate"
for i in $(seq "$RESTARTS"); do
  docker restart demo-importer >/dev/null
  for _ in $(seq 30); do
    curl -fsS "http://127.0.0.1:$STATUS_PORT/healthz" >/dev/null 2>&1 && break
    sleep 1
  done
  sleep 6   # let the run_on_start pass complete
  n=$(docker exec -i demo-timescaledb psql -U postgres -d metrics -tAc \
    "SELECT count(*) FROM metrics_raw WHERE labels ? 'source'")
  d=$(docker exec -i demo-timescaledb psql -U postgres -d metrics -tAc \
    "SELECT coalesce(sum(n-1),0) FROM (SELECT count(*) n FROM metrics_raw
       WHERE labels ? 'source' GROUP BY metric_name, labels, time
       HAVING count(*)>1) x")
  info "restart $i/$RESTARTS: $n rows, $d duplicate row(s)"
done
fi

# ---------------------------------------------------------------------------
say "Verifying the contents of metrics_raw"
python3 "$HERE/verify.py" \
  --dsn "postgresql://postgres:$SUPER_PW@127.0.0.1:$TS_PORT/metrics" \
  --influx-buckets "$BUCKETS"
rc=$?

say "Demo stack is still running - useful endpoints"
cat <<INFO
   importer status : curl -s http://127.0.0.1:$STATUS_PORT/status | python3 -m json.tool
   importer logs   : docker logs -f demo-importer
   TimescaleDB     : docker exec -it demo-timescaledb psql -U postgres -d metrics
   Prometheus UI   : http://127.0.0.1:$PROM_PORT
   tear down       : $HERE/run_demo.sh --clean
INFO
exit $rc
