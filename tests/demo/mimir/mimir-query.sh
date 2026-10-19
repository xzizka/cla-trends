#!/usr/bin/env bash
# Look at what is actually inside the demo Mimir — the source side of the
# import test, as opposed to verify_mimir.py which inspects the destination.
#
# Mimir is published on the deployment host's loopback only, so run this there:
#
#   ssh xzizka@192.168.122.115
#   ~/CLA-grafana/tests/demo/mimir/mimir-query.sh <cmd>
#
# ...or tunnel it to your workstation and point MIMIR_URL at the tunnel:
#
#   ssh -N -L 18080:127.0.0.1:18080 xzizka@192.168.122.115 &
#   MIMIR_URL=http://127.0.0.1:18080 ./mimir-query.sh now
#
# Every request carries X-Scope-OrgID: without it Mimir answers 401 "no org id",
# and with the wrong tenant it answers 200 with an empty result — which is why
# an empty result here is not automatically "the data is missing".
set -euo pipefail

MIMIR_URL="${MIMIR_URL:-http://127.0.0.1:18080}"
TENANT="${TENANT:-demo}"
SEL="${SEL:-{__name__=~\"demo_gauge_.+\"}}"
API="$MIMIR_URL/prometheus/api/v1"

q() {  # q <endpoint> <urlencoded args...>
  local ep="$1"; shift
  curl -fsS -m 30 -H "X-Scope-OrgID: $TENANT" "$@" "$API/$ep"
}

py() { python3 -c "$1"; }

case "${1:-help}" in

health)
  printf 'ready:   '; curl -fsS -m 10 "$MIMIR_URL/ready" || echo -n UNREADY; echo
  printf 'tenant:  %s\n' "$TENANT"
  echo "-- auth is enforced (positive control) --"
  printf '  no header    -> HTTP %s\n' \
    "$(curl -s -o /dev/null -w '%{http_code}' -m 10 --data-urlencode 'query=up' "$API/query")"
  printf '  wrong tenant -> %s\n' \
    "$(curl -fsS -m 10 -H 'X-Scope-OrgID: nobody' --data-urlencode "query=count($SEL)" "$API/query" \
       | py 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1]+" series" if r else "empty result")')"
  printf '  tenant %-6s-> %s\n' "$TENANT" \
    "$(q query --data-urlencode "query=count($SEL)" \
       | py 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1]+" series" if r else "empty result")')"
  ;;

series)   # every series Mimir holds for this tenant, with its full label set
  q series --data-urlencode "match[]=$SEL" | py '
import json, sys
res = json.load(sys.stdin)["data"]
print("%d series" % len(res))
for m in sorted(res, key=lambda m: m.get("__name__", "")):
    name = m.pop("__name__", "?")
    print("  %-18s %s" % (name, ", ".join("%s=%s" % kv for kv in sorted(m.items()))))'
  ;;

now)      # current value of every gauge, with the self-verification shown
  # NOTE: an instant query reports the *query evaluation* time, not the
  # sample's own timestamp — so the sample time is derived from the value
  # itself (value - idx), which is the identity the whole test rests on.
  q query --data-urlencode "query=${2:-$SEL}" | py '
import json, sys, time

def utc(t):
    return time.strftime("%H:%M:%S", time.gmtime(t))

res = json.load(sys.stdin)["data"]["result"]
if not res:
    print("empty result"); raise SystemExit
evaluated = float(res[0]["value"][0])
print("query evaluated at %s UTC\n" % utc(evaluated))
print("%-18s %-5s %-14s %-11s %s"
      % ("metric", "idx", "value", "value->time", "age at eval"))
for s in sorted(res, key=lambda s: s["metric"].get("__name__", "")):
    m = s["metric"]
    val = float(s["value"][1])
    idx = int(m.get("idx", 0))
    sample_t = val - idx
    print("%-18s %-5s %-14s %-11s %+.0fs"
          % (m.get("__name__", "?"), m.get("idx", "-"), "%d" % val,
             utc(sample_t), evaluated - sample_t))
'
  ;;

range)    # last N minutes of one metric at the 30s grid the importer reads on
  mins="${2:-10}"; metric="${3:-demo_gauge_00}"
  now=$(date +%s); start=$(( (now - mins*60) / 30 * 30 )); end=$(( now / 30 * 30 ))
  q query_range --data-urlencode "query=$metric" \
                --data-urlencode "start=$start" --data-urlencode "end=$end" \
                --data-urlencode "step=30s" | py '
import json, sys, time
res = json.load(sys.stdin)["data"]["result"]
if not res:
    print("empty result"); raise SystemExit
for s in res:
    idx = int(s["metric"].get("idx", 0))
    print("%s  (%d points)" % (s["metric"].get("__name__", "?"), len(s["values"])))
    print("  %-21s %-14s %s" % ("grid point (UTC)", "value", "value-idx vs grid"))
    for ts, val in s["values"]:
        ts = float(ts); d = ts - (float(val) - idx)
        print("  %-21s %-14s %+.0fs" % (
            time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)), val, d))'
  ;;

stats)    # volume + freshness, the two things worth watching during a run
  q query --data-urlencode "query=count($SEL)" | py '
import json,sys; r=json.load(sys.stdin)["data"]["result"]
print("series:        %s" % (r[0]["value"][1] if r else 0))'
  q query --data-urlencode "query=count_over_time(${SEL}[24h])" | py '
import json,sys
r=json.load(sys.stdin)["data"]["result"]
vals=[int(float(s["value"][1])) for s in r]
print("samples/series: min=%s max=%s  (total %s)" % (
    min(vals) if vals else 0, max(vals) if vals else 0, sum(vals)))'
  q query --data-urlencode "query=time() - timestamp(${SEL})" | py '
import json,sys
r=json.load(sys.stdin)["data"]["result"]
vals=[float(s["value"][1]) for s in r]
print("age of newest: %.1fs%s" % (max(vals) if vals else -1,
      "  (> 60s means the generator stopped)" if vals and max(vals) > 60 else ""))'
  ;;

raw)      # escape hatch: ./mimir-query.sh raw 'label/__name__/values'
  q "${2:?usage: $0 raw <api-path> [curl args...]}" "${@:3}"; echo
  ;;

*)
  sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
  cat <<USAGE

commands:
  health           readiness + proof the tenant header is enforced
  series           every series held for tenant '$TENANT', with labels
  now [promql]     current value of each gauge (default: all demo gauges)
  range [min] [m]  last N minutes of one metric on the 30s grid, with the
                   value-vs-timestamp identity the test relies on
  stats            series count, samples per series, age of the newest sample
  raw <path>       any Prometheus API path, tenant header added

env: MIMIR_URL (default $MIMIR_URL), TENANT (default $TENANT)
USAGE
  ;;
esac
