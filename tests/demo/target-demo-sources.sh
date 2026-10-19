#!/usr/bin/env bash
# Bring demo source endpoints up/down ON THE DEPLOYMENT HOST, attached to the
# stack's own `metrics` network so the real deployed importer container can
# reach them by name.
#
# This is for verifying a live deployment end-to-end. It does NOT touch the
# production stack: only `demo-*` containers are created, and the matching
# `target-demo-extra-vars.yml` injects the demo sources via --extra-vars so
# group_vars stays clean.
#
#   ./target-demo-sources.sh up     # start + seed
#   ./target-demo-sources.sh down   # remove containers, images kept
#   ./target-demo-sources.sh purge  # also remove the pulled images
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NET=metrics
IFX2_TOKEN=demo-influx2-token-0123456789
HOURS_BACK=2

case "${1:-}" in
down|purge)
  docker rm -f demo-prometheus demo-influxdb1 demo-influxdb2 demo-postgres >/dev/null 2>&1 || true
  docker volume rm -f demo-influxdb2-data demo-postgres-data >/dev/null 2>&1 || true
  echo "demo source containers removed"
  if [ "$1" = "purge" ]; then
    docker rmi -f prom/prometheus:latest influxdb:1.8 influxdb:2.7 postgres:18-alpine >/dev/null 2>&1 || true
    echo "demo source images removed"
  fi
  exit 0
  ;;
up) ;;
*) echo "usage: $0 {up|down|purge}" >&2; exit 1 ;;
esac

docker network inspect "$NET" >/dev/null 2>&1 || {
  echo "network '$NET' does not exist - deploy the stack first" >&2; exit 1; }

echo "== pulling demo source images (first run only) =="
for img in prom/prometheus:latest influxdb:1.8 influxdb:2.7 postgres:18-alpine; do
  docker image inspect "$img" >/dev/null 2>&1 || docker pull -q "$img"
done

echo "== starting demo sources on the '$NET' network =="
docker rm -f demo-prometheus demo-influxdb1 demo-influxdb2 demo-postgres >/dev/null 2>&1 || true

docker run -d --name demo-prometheus --network "$NET" \
  -v "$HERE/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  -p 127.0.0.1:19090:9090 prom/prometheus:latest >/dev/null

docker run -d --name demo-influxdb1 --network "$NET" \
  -e INFLUXDB_HTTP_AUTH_ENABLED=true -e INFLUXDB_DB=demo \
  -e INFLUXDB_ADMIN_USER=admin -e INFLUXDB_ADMIN_PASSWORD=demo-admin-pw \
  -e INFLUXDB_USER=demo_writer -e INFLUXDB_USER_PASSWORD=demo-writer-pw \
  -e INFLUXDB_READ_USER=demo_reader -e INFLUXDB_READ_USER_PASSWORD=demo-reader-pw \
  -p 127.0.0.1:18086:8086 influxdb:1.8 >/dev/null

docker run -d --name demo-influxdb2 --network "$NET" \
  -e DOCKER_INFLUXDB_INIT_MODE=setup \
  -e DOCKER_INFLUXDB_INIT_USERNAME=admin -e DOCKER_INFLUXDB_INIT_PASSWORD=demo-admin-pw \
  -e DOCKER_INFLUXDB_INIT_ORG=demo-org -e DOCKER_INFLUXDB_INIT_BUCKET=demo-bucket \
  -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN="$IFX2_TOKEN" \
  -v demo-influxdb2-data:/var/lib/influxdb2 \
  -p 127.0.0.1:28086:8086 influxdb:2.7 >/dev/null

# Plain (non-Timescale) PostgreSQL — the SQL source's counterpart to the two
# Influx flavours: exercises the only non-HTTP plugin end to end. Data lives
# in a named volume so `down`/`up` cycles keep the seeded history and the
# importer's watermark keeps meaning the same thing across restarts.
docker run -d --name demo-postgres --network "$NET" \
  -e POSTGRES_USER=demo_pg -e POSTGRES_PASSWORD=demo-pg-super-pw \
  -e POSTGRES_DB=demo \
  -v demo-postgres-data:/var/lib/postgresql \
  -p 127.0.0.1:15433:5432 postgres:18-alpine >/dev/null

for probe in "19090/-/ready" "18086/ping" "28086/health"; do
  port="${probe%%/*}"; path="${probe#*/}"
  printf '   waiting for 127.0.0.1:%s ... ' "$port"
  for _ in $(seq 45); do
    curl -fsS "http://127.0.0.1:$port/$path" >/dev/null 2>&1 && { echo ok; break; }
    sleep 2
  done
done
printf '   waiting for demo-postgres ... '
pg_ready=
for _ in $(seq 45); do
  if docker exec demo-postgres pg_isready -U demo_pg -d demo >/dev/null 2>&1; then
    pg_ready=1; echo ok; break
  fi
  sleep 2
done
[ -n "$pg_ready" ] || { echo "FAILED (container: $(docker ps -aqf name=demo-postgres | head -c 12))"; docker logs demo-postgres 2>&1 | tail -5; exit 1; }

# The read-only role the importer authenticates as, plus the metrics table
# with the same self-verifying invariant seed.py uses for Influx: every
# point's value IS its own epoch-minute, so value == floor(epoch(ts)/60)
# must hold for every row that lands in metrics_raw (see verify.py).
echo "== seeding $HOURS_BACK h of backdated, self-verifying rows into demo-postgres =="
docker exec -i demo-postgres psql -U demo_pg -d demo >/dev/null <<SQL
DO \$\$ BEGIN
  CREATE ROLE demo_reader LOGIN PASSWORD 'demo-reader-pw';
EXCEPTION WHEN duplicate_object THEN NULL; END \$\$;
GRANT CONNECT ON DATABASE demo TO demo_reader;
GRANT USAGE ON SCHEMA public TO demo_reader;

CREATE TABLE IF NOT EXISTS queue_stats (
  ts    timestamptz      NOT NULL,
  queue text             NOT NULL,
  host  text             NOT NULL,
  depth double precision NOT NULL
);
TRUNCATE queue_stats;
INSERT INTO queue_stats (ts, queue, host, depth)
SELECT to_timestamp(m * 60) AS ts, q.queue, q.host, m AS depth
  FROM generate_series(
         floor(extract(epoch FROM now()) / 60)::bigint - $((HOURS_BACK * 60)),
         floor(extract(epoch FROM now()) / 60)::bigint - 3, 1) AS m
 CROSS JOIN (VALUES ('orders', 'app-1'), ('orders', 'app-2'),
                    ('payments', 'app-1')) AS q(queue, host);
GRANT SELECT ON queue_stats TO demo_reader;
SQL
rows=$(docker exec demo-postgres psql -U demo_pg -d demo -tAc "SELECT count(*) FROM queue_stats")
echo "   seeded $rows rows into queue_stats"

echo "== seeding $HOURS_BACK h of backdated, self-verifying points =="
python3 "$HERE/seed.py" --influx1-url http://127.0.0.1:18086 \
                        --influx2-url http://127.0.0.1:28086 \
                        --hours-back "$HOURS_BACK"
