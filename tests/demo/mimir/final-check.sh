#!/usr/bin/env bash
# Final phase of the Mimir import test, designed to complete unattended.
#
# Waits for the exporter to hit its own deadline, waits for the importer tick
# that covers the last of the data, restarts the importer a few times, and runs
# the full verification — writing everything to final-check.log.
#
# Run it detached so it outlives the ssh session that started it:
#   setsid nohup ./final-check.sh >/dev/null 2>&1 &
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="$HERE/final-check.log"
STATUS_URL=http://127.0.0.1:9202/status
RESTARTS=3

say() { echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] $*" | tee -a "$LOG"; }

last_run_epoch() {
  curl -fsS -m 10 "$STATUS_URL" 2>/dev/null | python3 -c '
import json, sys
from datetime import datetime
try:
    s = json.load(sys.stdin)["sources"]["demo-mimir"]["last_run"]
    print(int(datetime.fromisoformat(s).timestamp()))
except Exception:
    print(0)' 2>/dev/null || echo 0
}

: > "$LOG"
say "final check armed; waiting for the exporter to reach its deadline"

# 1. The exporter exits on its own — that is the end of the data, and waiting
#    for the container rather than for a wall-clock time means this stays
#    correct no matter when the run actually started.
while [ "$(docker inspect -f '{{.State.Running}}' demo-mimir-exporter 2>/dev/null)" = "true" ]; do
  sleep 60
done
EXIT_AT=$(date +%s)
say "exporter stopped; generator produced its last samples"

# 2. Wait for an importer tick that started comfortably after the last sample,
#    so the final window genuinely covers the tail (end_lag is 2m).
say "waiting for the importer tick that covers the tail"
for _ in $(seq 120); do
  [ "$(last_run_epoch)" -gt $((EXIT_AT + 150)) ] && break
  sleep 30
done
say "importer tick done: $(curl -fsS -m 10 "$STATUS_URL" | python3 -m json.tool | tr -d '\n' | cut -c1-400)"

# 3. Restart the importer repeatedly. Each restart triggers an immediate
#    import (run_on_start), so if the watermark were not doing its job these
#    would re-import overlapping windows and the duplicate check would catch it.
say "restarting the importer ${RESTARTS}x to re-test the watermark"
for i in $(seq "$RESTARTS"); do
  docker restart importer >/dev/null 2>&1
  sleep 20
  say "  restart $i/$RESTARTS done"
done
sleep 20

# 4. Refresh the continuous aggregates so the metrics_1h check sees the tail
#    too (refresh_continuous_aggregate cannot run inside a transaction block).
docker exec timescaledb psql -U postgres -d metrics -v ON_ERROR_STOP=1 -c \
  "CALL refresh_continuous_aggregate('metrics_1h', NULL, NULL);" >>"$LOG" 2>&1
docker exec timescaledb psql -U postgres -d metrics -v ON_ERROR_STOP=1 -c \
  "CALL refresh_continuous_aggregate('metrics_4h', NULL, NULL);" >>"$LOG" 2>&1

say "running verify_mimir.py"
{
  echo
  echo "===== importer log (tail) ====="
  docker logs importer 2>&1 | tail -30
  echo
  echo "===== mimir-query.sh stats ====="
  "$HERE/mimir-query.sh" stats
  echo
  echo "===== verify_mimir.py ====="
} >>"$LOG" 2>&1
python3 "$HERE/verify_mimir.py" >>"$LOG" 2>&1
RC=$?
say "verification finished with exit code $RC"
exit $RC
