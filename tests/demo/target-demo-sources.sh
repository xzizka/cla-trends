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
  docker rm -f demo-prometheus demo-influxdb1 demo-influxdb2 >/dev/null 2>&1 || true
  docker volume rm -f demo-influxdb2-data >/dev/null 2>&1 || true
  echo "demo source containers removed"
  if [ "$1" = "purge" ]; then
    docker rmi -f prom/prometheus:latest influxdb:1.8 influxdb:2.7 >/dev/null 2>&1 || true
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
for img in prom/prometheus:latest influxdb:1.8 influxdb:2.7; do
  docker image inspect "$img" >/dev/null 2>&1 || docker pull -q "$img"
done

echo "== starting demo sources on the '$NET' network =="
docker rm -f demo-prometheus demo-influxdb1 demo-influxdb2 >/dev/null 2>&1 || true

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

for probe in "19090/-/ready" "18086/ping" "28086/health"; do
  port="${probe%%/*}"; path="${probe#*/}"
  printf '   waiting for 127.0.0.1:%s ... ' "$port"
  for _ in $(seq 45); do
    curl -fsS "http://127.0.0.1:$port/$path" >/dev/null 2>&1 && { echo ok; break; }
    sleep 2
  done
done

echo "== seeding $HOURS_BACK h of backdated, self-verifying points =="
python3 "$HERE/seed.py" --influx1-url http://127.0.0.1:18086 \
                        --influx2-url http://127.0.0.1:28086 \
                        --hours-back "$HOURS_BACK"
