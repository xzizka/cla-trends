#!/usr/bin/env bash
# Bring the Mimir import test's generator side up/down ON THE DEPLOYMENT HOST,
# attached to the stack's own `metrics` network so the real deployed importer
# container can reach Mimir by name.
#
# Three containers, all prefixed `demo-mimir`:
#   demo-mimir            Grafana Mimir, monolithic, multi-tenant (tenant "demo")
#   demo-mimir-exporter   20 self-verifying dummy gauges, exits after --hours
#   demo-mimir-prom       Prometheus, scrapes every 30s, remote_writes to Mimir
#
# Nothing here touches the production stack or the playbook: the importer is
# pointed at Mimir by an --extra-vars overlay (mimir-demo-extra-vars.yml).
#
#   ./mimir-demo.sh up [hours]   # default 4
#   ./mimir-demo.sh status
#   ./mimir-demo.sh down         # remove containers + volume, keep images
#   ./mimir-demo.sh purge        # also remove the pulled images
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NET=metrics
TENANT=demo
MIMIR_IMAGE=grafana/mimir:3.2.1
PROM_IMAGE=prom/prometheus:v3.13.3
PY_IMAGE=python:3.14-slim
CONTAINERS="demo-mimir-prom demo-mimir-exporter demo-mimir"
VOLUME=demo-mimir-data

# Host-side ports, loopback only — for verification from the host, never LAN.
MIMIR_PORT=18080
PROM_PORT=19090

case "${1:-}" in
down|purge)
  # -v, not just -f: prom/prometheus declares /prometheus as a VOLUME, so each
  # run creates an anonymous volume that `docker rm -f` would leave dangling.
  docker rm -fv $CONTAINERS >/dev/null 2>&1 || true
  docker volume rm -f "$VOLUME" >/dev/null 2>&1 || true
  echo "demo-mimir containers and volume removed"
  if [ "${1}" = "purge" ]; then
    docker rmi -f "$MIMIR_IMAGE" "$PROM_IMAGE" >/dev/null 2>&1 || true
    echo "demo-mimir images removed"
  fi
  exit 0
  ;;
status)
  docker ps -a --filter "name=demo-mimir" \
    --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
  echo
  echo "-- samples in Mimir (tenant $TENANT) --"
  curl -fsS -H "X-Scope-OrgID: $TENANT" \
    --data-urlencode 'query=count({__name__=~"demo_gauge_.+"})' \
    "http://127.0.0.1:$MIMIR_PORT/prometheus/api/v1/query" 2>/dev/null \
    || echo "  (Mimir not answering)"
  echo
  exit 0
  ;;
up) ;;
*) echo "usage: $0 {up [hours]|status|down|purge}" >&2; exit 1 ;;
esac

HOURS="${2:-4}"
DURATION=$(python3 -c "print(int(float('$HOURS') * 3600))")

docker network inspect "$NET" >/dev/null 2>&1 || {
  echo "network '$NET' does not exist - deploy the stack first" >&2; exit 1; }

echo "== pulling images (first run only) =="
for img in "$MIMIR_IMAGE" "$PROM_IMAGE" "$PY_IMAGE"; do
  docker image inspect "$img" >/dev/null 2>&1 || docker pull -q "$img"
done

echo "== (re)creating demo-mimir containers on the '$NET' network =="
docker rm -fv $CONTAINERS >/dev/null 2>&1 || true
docker volume rm -f "$VOLUME" >/dev/null 2>&1 || true
docker volume create "$VOLUME" >/dev/null

# The Mimir image runs as an unprivileged uid; a fresh named volume is owned by
# root, so Mimir would fail to create its TSDB directories. Fix it up front
# with an image that is already on the host rather than pulling busybox.
MIMIR_UID="$(docker image inspect -f '{{.Config.User}}' "$MIMIR_IMAGE")"
MIMIR_UID="${MIMIR_UID:-0}"
docker run --rm -v "$VOLUME:/data" "$PY_IMAGE" \
  chown -R "$MIMIR_UID" /data >/dev/null

docker run -d --name demo-mimir --network "$NET" \
  -v "$HERE/mimir.yaml:/etc/mimir/mimir.yaml:ro" \
  -v "$VOLUME:/data" \
  -p "127.0.0.1:$MIMIR_PORT:8080" \
  "$MIMIR_IMAGE" -config.file=/etc/mimir/mimir.yaml >/dev/null

docker run -d --name demo-mimir-exporter --network "$NET" \
  -v "$HERE/exporter.py:/app/exporter.py:ro" \
  "$PY_IMAGE" python3 /app/exporter.py --duration "$DURATION" >/dev/null

printf '   waiting for Mimir to be ready ... '
for _ in $(seq 60); do
  if curl -fsS "http://127.0.0.1:$MIMIR_PORT/ready" >/dev/null 2>&1; then
    echo ok; break
  fi
  sleep 2
done
curl -fsS "http://127.0.0.1:$MIMIR_PORT/ready" >/dev/null 2>&1 || {
  echo "FAILED"; docker logs --tail 40 demo-mimir; exit 1; }

# Prometheus starts last so its first scrape cannot race Mimir's readiness and
# spend the initial minute retrying remote_write.
docker run -d --name demo-mimir-prom --network "$NET" \
  -v "$HERE/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  -p "127.0.0.1:$PROM_PORT:9090" \
  "$PROM_IMAGE" \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/prometheus \
  --storage.tsdb.retention.time=12h >/dev/null

printf '   waiting for Prometheus to be ready ... '
for _ in $(seq 45); do
  curl -fsS "http://127.0.0.1:$PROM_PORT/-/ready" >/dev/null 2>&1 && { echo ok; break; }
  sleep 2
done

# Proving data actually reaches Mimir is the point of the `up` step — a running
# container says nothing about whether remote_write is working.
echo "== waiting for the first samples to land in Mimir =="
for _ in $(seq 40); do
  n=$(curl -fsS -H "X-Scope-OrgID: $TENANT" \
        --data-urlencode 'query=count({__name__=~"demo_gauge_.+"})' \
        "http://127.0.0.1:$MIMIR_PORT/prometheus/api/v1/query" 2>/dev/null \
      | python3 -c "import json,sys; r=json.load(sys.stdin)['data']['result']; print(int(float(r[0]['value'][1])) if r else 0)" 2>/dev/null || echo 0)
  if [ "${n:-0}" -ge 1 ]; then
    echo "   Mimir holds $n demo_gauge series (expected 20)"
    break
  fi
  sleep 5
done

END_AT=$(python3 -c "import time; print(time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(time.time() + $DURATION)))")
cat <<SUMMARY

generator is up. exporter stops on its own at: $END_AT  (${HOURS} h)
  Mimir      http://127.0.0.1:$MIMIR_PORT/prometheus  (tenant: $TENANT)
  Prometheus http://127.0.0.1:$PROM_PORT

next: ansible-playbook playbook.yml -e @tests/demo/mimir/mimir-demo-extra-vars.yml
SUMMARY
