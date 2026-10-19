# Mimir import test

Tests whether the **deployed** stack's importer can pull data out of an
external Grafana Mimir — the real deployment on `ubuntu-docker`, its real
importer container, its real `metrics_raw`. Not a simulation on the side.

Separate from the deployment: nothing here is referenced by `playbook.yml` or
`group_vars`. The importer is pointed at Mimir with an `--extra-vars` overlay,
and running the playbook without it puts everything back.

## What it builds

| Container | Role |
|---|---|
| `demo-mimir` | Grafana Mimir 3.2.1, monolithic (`-target=all`), filesystem blocks storage, **multi-tenant** (tenant `demo`) |
| `demo-mimir-exporter` | 20 self-verifying dummy gauges (`demo_gauge_00`..`19`), stdlib-only Python, exits on its own deadline |
| `demo-mimir-prom` | Prometheus v3.13.3, scrapes every 30s, `remote_write` → Mimir |

All three join the stack's existing `metrics` network, which is how the
importer reaches Mimir by name — the same path it uses to reach `timescaledb`,
so this exercises the container's real DNS/network route, not just the code.

Prometheus is a full server rather than agent mode on purpose: it keeps its own
copy of the samples, giving `verify_mimir.py` an **independent source of truth**
to compare the imported rows against point for point.

## Running it

```bash
# on the deployment host
cd ~/CLA-grafana/tests/demo/mimir
./mimir-demo.sh up 4                  # hours; the exporter enforces this itself

cd ~/CLA-grafana
ansible-playbook playbook.yml -e @tests/demo/mimir/mimir-demo-extra-vars.yml

# after the run (or any time — checks are valid on a partial run too)
python3 tests/demo/mimir/verify_mimir.py

# back to production config, then tear the generator down
ansible-playbook playbook.yml
tests/demo/mimir/mimir-demo.sh down   # or `purge` to drop the images too
```

### Seeing Mimir in Grafana

Grafana is on the same `metrics` network, so it reaches Mimir directly — no
tunnel. Adding it as a second datasource lets you put the source (Mimir) and
the destination (TimescaleDB) side by side in one panel:

```bash
GFPW=$(ansible-vault view group_vars/all/vault.yml | sed -n 's/^vault_grafana_admin_password: *//p' | tr -d "\"'")
curl -su "admin:$GFPW" -H 'Content-Type: application/json' -X POST \
  -d '{"uid":"demo-mimir","name":"Demo Mimir","type":"prometheus","access":"proxy",
       "url":"http://demo-mimir:8080/prometheus","isDefault":false,
       "jsonData":{"httpMethod":"POST","prometheusType":"Mimir","timeInterval":"30s",
                   "httpHeaderName1":"X-Scope-OrgID"},
       "secureJsonData":{"httpHeaderValue1":"demo"}}' \
  http://192.168.122.115:3000/api/datasources
```

The tenant header goes in `httpHeaderName1` / `secureJsonData.httpHeaderValue1`
— without it every query returns `401 no org id`.

**This datasource is created through the API, not provisioned**, so it lives in
the `grafana-data` volume and running the playbook will *not* remove it. Delete
it explicitly as part of cleanup:

```bash
curl -su "admin:$GFPW" -X DELETE http://192.168.122.115:3000/api/datasources/uid/demo-mimir
```

Cleaning the imported rows out of the production table afterwards:

```sql
DELETE FROM metrics_raw   WHERE labels->>'source' = 'demo-mimir';
DELETE FROM importer_state WHERE source_name      = 'demo-mimir';
```

## Looking at the source side: mimir-query.sh

`verify_mimir.py` inspects the destination (`metrics_raw`). To see what is in
**Mimir itself** — i.e. whether a missing row is the importer's fault or was
never there — use `mimir-query.sh`. It adds the tenant header to every request,
which matters: without it Mimir answers `401 no org id`, and with the *wrong*
tenant it answers `200` with an empty result, so "no data" here is ambiguous
unless the tenant is right.

```bash
ssh xzizka@192.168.122.115
~/CLA-grafana/tests/demo/mimir/mimir-query.sh health   # readiness + auth proof
./mimir-query.sh stats                 # series count, samples/series, freshness
./mimir-query.sh series                # all 20 series with their label sets
./mimir-query.sh now                   # current value of each gauge
./mimir-query.sh range 10 demo_gauge_07  # last 10 min on the importer's grid
./mimir-query.sh raw 'label/__name__/values'
```

Mimir is published on the host's loopback only. To use it from a workstation,
tunnel it and point `MIMIR_URL` at the tunnel:

```bash
ssh -N -L 18080:127.0.0.1:18080 xzizka@192.168.122.115 &
MIMIR_URL=http://127.0.0.1:18080 ./mimir-query.sh now
```

`range` prints the `value-idx vs grid` column, which is the identity below made
visible — a constant offset in `[0, 30s)` across every point is what correct
looks like.

Careful with `now`: an **instant query reports the query evaluation time**, not
the sample's own timestamp. The subcommand therefore derives the sample time
from the value itself rather than displaying the returned timestamp, which
would look like a 3-second skew that is not there.

## How the data verifies itself

Each gauge reads `unix_seconds + idx`, so one identity checks a stored row end
to end: a wrong value, a shifted timestamp, a timezone error and a mis-joined
series all break it.

It is deliberately **not** an equality. The importer reads through
`query_range` at a 30s step, and a range query answers each grid point with the
last sample *at or before* it — so a row stamped at grid point `g` carries a
value from a scrape up to one interval earlier:

```
0 <= epoch(g) - (value - idx) < 30s        (plus ~1s of scrape jitter)
```

The aggregate stream checks independently: `sum(epoch + i for i in 0..19)` is
`20*epoch + 190`, so `(value - 190) / 20` must satisfy the same bound.

## Two settings that decide whether the test can lie

**`end_lag: 2m`** (in the overlay). Without it the importer would read right up
to `now`, where the newest grid point resolves to a stale sample whenever
remote_write has not delivered the latest scrape yet — and the watermark would
then advance past it, making the error permanent instead of self-healing on the
next tick.

**Multi-tenancy on.** With `multitenancy_enabled: false` the test would pass
whether or not the importer sent a tenant header, proving nothing about that
code path. Verified as a positive control:

```
no header          -> HTTP 401 "no org id"
X-Scope-OrgID: xxx -> 200, empty result
X-Scope-OrgID: demo-> 200, 20 series
```

Note the importer's auth is **one-of**: `httpclient._apply_auth` is an if/elif
chain, so a source cannot send a tenant header *and* a bearer token. A Mimir
behind an authenticating gateway would need a code change.

## Checks in verify_mimir.py

Runs on the deployment host, stdlib only — `psql` via `docker exec`, Prometheus
via `urllib`. No psycopg, no vault access.

- exactly the 20 gauges plus the aggregate, equal row counts across gauges
- every timestamp on the 30s epoch grid
- the value identity above, for both streams
- **no duplicates** on `(time, metric_name, labels)` — `metrics_raw` has no
  unique constraint, so this is the only thing proving the watermark actually
  prevents overlapping re-imports
- `labels` are jsonb objects, not encoded strings (regression guard for
  bug-021), carry `source`/`demo`/`demo_stack`/`job`, and no leftover `__name__`
- no gaps in the grid between the first and last row
- one watermark per stream, past the newest row
- data reached the `metrics_1h` continuous aggregate
- **cross-check**: every point matches the generator's own Prometheus TSDB

A tail of rows violating the value identity within the last 5 minutes is
reported as a note, not a failure: after the exporter exits, Prometheus
staleness semantics keep answering with the last sample until the staleness
horizon passes.
