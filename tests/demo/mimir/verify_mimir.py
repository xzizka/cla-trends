#!/usr/bin/env python3
"""Assert what the importer actually pulled out of Mimir into metrics_raw.

Runs ON the deployment host. Talks to the database through `docker exec ...
psql` and to the demo Prometheus over HTTP, so it needs nothing installed
beyond the stdlib — no psycopg, no vault access.

Every check is a claim about the stored data, not about the importer's logs.
The load-bearing ones:

  * VALUE IDENTITY. exporter.py makes each gauge read `unix_seconds + idx`, so
    `epoch(time) - (value - idx)` must land in [0, scrape_interval). It is not
    zero because a range query answers each grid point with the last sample at
    or before it — see exporter.py. This one identity catches a wrong value, a
    shifted timestamp, a timezone error and a mis-joined series at once.
  * NO DUPLICATES on (time, metric_name, labels). metrics_raw has no unique
    constraint, so this is the only thing proving the watermark actually stops
    overlapping re-imports — the run restarts the importer before verifying.
  * CROSS-CHECK against the demo Prometheus' own TSDB. The importer read the
    data through Mimir; Prometheus holds the same samples independently. If
    both agree point for point, the whole path scrape -> remote_write -> Mimir
    -> query_range -> metrics_raw preserved the data exactly.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.parse
import urllib.request

PASS, FAIL, WARN = "PASS", "FAIL", "WARN"

SOURCE = "demo-mimir"
GAUGE_COUNT = 20
STEP = 30
#: sum(epoch + i for i in 0..19) == 20*epoch + 190
SUM_OFFSET = sum(range(GAUGE_COUNT))
#: Tolerance on the value identity: [0, STEP) by construction, widened by a
#: second at each end for scrape/clock jitter.
IDENT_LO, IDENT_HI = -2, STEP + 2
#: A target that stops being scraped keeps answering range queries with its
#: last sample until Prometheus' staleness horizon passes. Violations inside
#: this window at the very end of the series are expected, not a defect.
STALENESS_S = 300


class Checks(object):
    def __init__(self, container, database):
        self.container = container
        self.database = database
        self.results = []

    # -- plumbing ----------------------------------------------------------

    def sql(self, query):
        out = subprocess.run(
            ["docker", "exec", "-i", self.container,
             "psql", "-U", "postgres", "-d", self.database,
             "-At", "-F", "|", "-c", query],
            capture_output=True, text=True,
        )
        if out.returncode != 0:
            raise RuntimeError("psql failed: %s" % out.stderr.strip())
        return [line.split("|") for line in out.stdout.strip().splitlines() if line]

    def one(self, query):
        rows = self.sql(query)
        return rows[0][0] if rows else None

    def check(self, name, ok, detail):
        self.results.append((PASS if ok else FAIL, name, detail))
        return ok

    def note(self, name, detail):
        self.results.append((WARN, name, detail))

    # -- checks ------------------------------------------------------------

    def rows_present(self):
        row = self.sql(
            "SELECT count(*), count(DISTINCT metric_name),"
            "       extract(epoch from min(time))::bigint,"
            "       extract(epoch from max(time))::bigint"
            "  FROM metrics_raw WHERE labels->>'source' = '%s'" % SOURCE
        )[0]
        total, metrics, tmin, tmax = int(row[0]), int(row[1]), row[2], row[3]
        self.check("rows imported", total > 0,
                   "%d rows, %d distinct metric_name" % (total, metrics))
        if total == 0:
            return None
        span = int(tmax) - int(tmin)
        self.note("time span",
                  "%s .. %s (%.2f h, %d grid points wide)"
                  % (tmin, tmax, span / 3600.0, span // STEP + 1))
        return int(tmin), int(tmax), total

    def metric_names(self):
        rows = self.sql(
            "SELECT metric_name, count(*) FROM metrics_raw"
            " WHERE labels->>'source' = '%s' GROUP BY 1 ORDER BY 1" % SOURCE
        )
        found = {r[0]: int(r[1]) for r in rows}
        expected = set("demo_gauge_%02d" % i for i in range(GAUGE_COUNT))
        expected.add("demo_gauge_sum")
        missing = sorted(expected - set(found))
        extra = sorted(set(found) - expected)
        self.check("metric names are exactly the %d gauges + the aggregate"
                   % GAUGE_COUNT,
                   not missing and not extra,
                   "missing=%s extra=%s" % (missing or "none", extra or "none"))
        counts = sorted(v for k, v in found.items() if k != "demo_gauge_sum")
        if counts:
            self.check("all %d gauges have the same row count" % GAUGE_COUNT,
                       counts[0] == counts[-1],
                       "min=%d max=%d" % (counts[0], counts[-1]))
        return found

    def on_grid(self):
        off = int(self.one(
            "SELECT count(*) FROM metrics_raw"
            " WHERE labels->>'source' = '%s'"
            "   AND extract(epoch from time)::bigint %% %d <> 0" % (SOURCE, STEP)
        ))
        self.check("every timestamp is on the %ds epoch grid" % STEP, off == 0,
                   "%d rows off-grid" % off)

    def value_identity(self, tmax):
        """`epoch(time) - (value - idx)` must sit in [0, step)."""
        for label, expr, where in (
            ("gauges",
             "extract(epoch from time)::bigint - (value - (labels->>'idx')::int)",
             "metric_name LIKE 'demo\\_gauge\\_%%' AND labels ? 'idx'"),
            ("aggregate",
             "extract(epoch from time)::bigint - ((value - %d) / %d)"
             % (SUM_OFFSET, GAUGE_COUNT),
             "metric_name = 'demo_gauge_sum'"),
        ):
            rows = self.sql(
                "SELECT count(*),"
                "       count(*) FILTER (WHERE d < %d OR d >= %d),"
                "       count(*) FILTER (WHERE (d < %d OR d >= %d)"
                "                          AND extract(epoch from time) < %d),"
                "       min(d)::numeric(12,3), max(d)::numeric(12,3),"
                "       avg(d)::numeric(12,3)"
                "  FROM (SELECT time, (%s) AS d FROM metrics_raw"
                "         WHERE labels->>'source' = '%s' AND %s) s"
                % (IDENT_LO, IDENT_HI, IDENT_LO, IDENT_HI, tmax - STALENESS_S,
                   expr, SOURCE, where)
            )[0]
            total, bad, bad_early = int(rows[0]), int(rows[1]), int(rows[2])
            detail = ("%d rows, %d outside [%d,%d), delta min=%s max=%s avg=%s"
                      % (total, bad, IDENT_LO, IDENT_HI, rows[3], rows[4], rows[5]))
            self.check("value identity holds for the %s" % label,
                       total > 0 and bad_early == 0, detail)
            if bad and not bad_early:
                self.note("%s staleness tail" % label,
                          "%d rows violate only within the last %ds, which is "
                          "Prometheus staleness semantics after the exporter "
                          "stopped, not lost fidelity" % (bad, STALENESS_S))

    def no_duplicates(self):
        row = self.sql(
            "SELECT count(*), count(DISTINCT (time, metric_name, labels))"
            "  FROM metrics_raw WHERE labels->>'source' = '%s'" % SOURCE
        )[0]
        total, distinct = int(row[0]), int(row[1])
        self.check("no duplicate (time, metric_name, labels)", total == distinct,
                   "%d rows, %d distinct -> %d duplicate(s)"
                   % (total, distinct, total - distinct))

    def labels_shape(self):
        bad = int(self.one(
            "SELECT count(*) FROM metrics_raw"
            " WHERE labels->>'source' = '%s' AND jsonb_typeof(labels) <> 'object'"
            % SOURCE))
        self.check("labels are jsonb objects, not encoded strings", bad == 0,
                   "%d rows with a non-object labels column" % bad)

        # `sum by (job) (...)` keeps only `job`, so the aggregate legitimately
        # has neither the exporter's own labels nor Prometheus' external ones.
        # Scoping this per stream is the difference between checking the data
        # and checking a wrong expectation.
        gauges_only = " AND metric_name <> 'demo_gauge_sum'"
        for key, value, scope, where in (
            ("source", SOURCE, "every row", ""),
            ("demo", "mimir", "every row", ""),
            ("job", "demo-exporter", "every row", ""),
            ("demo_stack", "mimir", "every gauge row", gauges_only),
            ("idx", None, "every gauge row", gauges_only),
        ):
            if value is None:       # presence only — idx differs per series
                cond = "labels->>'%s' IS NULL" % key
                label = "%s carries %s" % (scope, key)
            else:
                cond = "labels->>'%s' IS DISTINCT FROM '%s'" % (key, value)
                label = "%s carries %s=%s" % (scope, key, value)
            missing = int(self.one(
                "SELECT count(*) FROM metrics_raw"
                " WHERE labels->>'source' = '%s'%s AND %s" % (SOURCE, where, cond)))
            self.check(label, missing == 0, "%d rows without it" % missing)

        leaked = int(self.one(
            "SELECT count(*) FROM metrics_raw"
            " WHERE labels->>'source' = '%s' AND labels ? '__name__'" % SOURCE))
        self.check("__name__ is not left in the labels", leaked == 0,
                   "%d rows still carry it" % leaked)

    def continuity(self, tmin, tmax):
        """A gap would mean a window the importer skipped entirely."""
        expected = (tmax - tmin) // STEP + 1
        rows = self.sql(
            "SELECT metric_name, count(*) FROM metrics_raw"
            " WHERE labels->>'source' = '%s' AND metric_name <> 'demo_gauge_sum'"
            " GROUP BY 1 HAVING count(*) <> %d ORDER BY 1" % (SOURCE, expected))
        self.check("no gaps in the %ds grid for any gauge" % STEP, not rows,
                   "expected %d points per gauge; deviating: %s"
                   % (expected, [(r[0], r[1]) for r in rows] or "none"))

    def watermarks(self, tmax):
        rows = self.sql(
            "SELECT stream_key, extract(epoch from last_ts)::bigint, last_rows"
            "  FROM importer_state WHERE source_name = '%s' ORDER BY 1" % SOURCE)
        keys = [r[0] for r in rows]
        self.check("a watermark exists per stream", set(keys) ==
                   {"demo_gauges", "demo_gauges_sum"},
                   "streams: %s" % (keys or "none"))
        for key, last_ts, last_rows in ((r[0], int(r[1]), r[2]) for r in rows):
            self.check("watermark %s is past the newest row" % key,
                       last_ts > tmax,
                       "watermark=%d newest row=%d (+%ds), last_rows=%s"
                       % (last_ts, tmax, last_ts - tmax, last_rows))

    def aggregate_flow(self):
        n = int(self.one(
            "SELECT count(*) FROM metrics_1h"
            " WHERE metric_name LIKE 'demo\\_gauge\\_%'"))
        self.check("data reached the metrics_1h continuous aggregate", n > 0,
                   "%d rows in metrics_1h" % n)

    def cross_check_prometheus(self, prom_url, tmin, tmax):
        """Compare point for point against the generator's own TSDB."""
        params = urllib.parse.urlencode({
            "query": '{__name__=~"demo_gauge_.+"}',
            "start": tmin, "end": tmax, "step": "%ds" % STEP,
        })
        try:
            with urllib.request.urlopen(
                    "%s/api/v1/query_range?%s" % (prom_url.rstrip("/"), params),
                    timeout=120) as fh:
                payload = json.load(fh)
        except Exception as exc:                      # noqa: BLE001
            self.note("cross-check vs Prometheus",
                      "skipped: %s not reachable (%s)" % (prom_url, exc))
            return
        if payload.get("status") != "success":
            self.note("cross-check vs Prometheus", "query failed: %s" % payload)
            return

        prom = {}
        for series in payload["data"]["result"]:
            name = series["metric"].get("__name__")
            for ts, val in series["values"]:
                prom[(name, int(float(ts)))] = float(val)

        db = {}
        for name, ts, val in self.sql(
                "SELECT metric_name, extract(epoch from time)::bigint, value"
                "  FROM metrics_raw WHERE labels->>'source' = '%s'"
                "   AND metric_name <> 'demo_gauge_sum'" % SOURCE):
            db[(name, int(ts))] = float(val)

        only_prom = set(prom) - set(db)
        only_db = set(db) - set(prom)
        differing = [k for k in set(prom) & set(db) if prom[k] != db[k]]
        self.check("every point matches the generator's own Prometheus",
                   not only_prom and not only_db and not differing,
                   "%d points compared; only-in-prometheus=%d only-in-db=%d "
                   "value-mismatch=%d"
                   % (len(set(prom) & set(db)), len(only_prom), len(only_db),
                      len(differing)))

    # -- reporting ---------------------------------------------------------

    def report(self):
        width = max(len(n) for _, n, _ in self.results)
        failed = 0
        for status, name, detail in self.results:
            if status == FAIL:
                failed += 1
            print("%-4s %-*s  %s" % (status, width, name, detail))
        passed = sum(1 for s, _, _ in self.results if s == PASS)
        print("\n%d/%d checks passed%s"
              % (passed, passed + failed, "" if not failed else
                 " — %d FAILED" % failed))
        return failed == 0


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pg-container", default="timescaledb")
    ap.add_argument("--database", default="metrics")
    ap.add_argument("--prometheus", default="http://127.0.0.1:19090",
                    help="demo Prometheus, for the independent cross-check")
    args = ap.parse_args()

    c = Checks(args.pg_container, args.database)
    span = c.rows_present()
    if span is None:
        c.report()
        print("\nnothing imported yet — nothing else can be checked",
              file=sys.stderr)
        return 1
    tmin, tmax, _ = span

    c.metric_names()
    c.on_grid()
    c.value_identity(tmax)
    c.no_duplicates()
    c.labels_shape()
    c.continuity(tmin, tmax)
    c.watermarks(tmax)
    c.aggregate_flow()
    c.cross_check_prometheus(args.prometheus, tmin, tmax)
    return 0 if c.report() else 1


if __name__ == "__main__":
    sys.exit(main())
