#!/usr/bin/env python3
"""Assert what the importer actually put in the demo TimescaleDB.

Every check is a claim about the data, not about the importer's own logs — if
the importer reported success but wrote nothing, or wrote it twice, or shifted
it by an hour, these queries say so.

The load-bearing ones:

  * NO DUPLICATES on (time, metric_name, labels). `metrics_raw` has no unique
    constraint, so this is the only thing that proves the watermark actually
    prevents overlapping re-imports. The demo restarts the importer several
    times before running this, each restart triggering an immediate import.
  * value == floor(epoch(time)/60) for every seeded Influx row. seed.py made
    each point's value its own epoch-minute, so this single identity catches a
    wrong value, a shifted timestamp, a partially-aggregated bucket (its mean
    would fall between integers) and a timezone error.
  * every row carries the provenance label the runner injects.
"""
from __future__ import annotations

import argparse
import sys

import psycopg

PASS, FAIL, WARN = "PASS", "FAIL", "WARN"


class Checks(object):
    def __init__(self, conn):
        self.conn = conn
        self.results = []

    def q(self, sql, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()

    def one(self, sql, params=None):
        rows = self.q(sql, params)
        return rows[0][0] if rows else None

    def check(self, name, ok, detail):
        self.results.append((PASS if ok else FAIL, name, detail))
        return ok

    def note(self, name, detail):
        self.results.append((WARN, name, detail))

    # -- individual checks -------------------------------------------------

    def sources_present(self, expected):
        rows = self.q(
            "SELECT labels->>'source', count(*), min(time), max(time),"
            "       count(DISTINCT metric_name)"
            "  FROM metrics_raw WHERE labels ? 'source'"
            " GROUP BY 1 ORDER BY 1"
        )
        found = {r[0]: r for r in rows}
        for src in expected:
            r = found.get(src)
            self.check(
                "source %r wrote rows" % src,
                r is not None and r[1] > 0,
                "%d rows, %d metric(s), %s .. %s" % (r[1], r[4], r[2], r[3])
                if r else "NO ROWS - importer did not write anything for this source",
            )
        return found

    def labels_are_jsonb_objects(self):
        """Catches the double-encoding class of bug directly.

        If the writer hands psycopg an already-serialised JSON *string* for a
        jsonb column, it gets encoded a second time and the column holds a
        JSON string rather than an object. Nothing errors: rows land, values
        are right, but `labels ? 'source'` matches nothing and every check
        below reports "no rows" instead of the actual cause. Assert the shape
        first so the diagnosis is unambiguous. (bug-021)
        """
        rows = self.q("SELECT jsonb_typeof(labels), count(*)"
                      "  FROM metrics_raw GROUP BY 1 ORDER BY 2 DESC")
        kinds = {r[0]: r[1] for r in rows}
        total = sum(kinds.values())
        self.check(
            "labels stored as jsonb objects",
            total > 0 and set(kinds) == {"object"},
            "all %d rows are objects" % total if set(kinds) == {"object"}
            else "jsonb_typeof breakdown: %s — anything but 'object' means the "
                 "writer double-encoded the labels" % (kinds or "no rows"),
        )

    def no_duplicates(self):
        # The single most important assertion in this demo.
        dupes = self.q(
            "SELECT metric_name, labels, time, count(*) AS n"
            "  FROM metrics_raw WHERE labels ? 'source'"
            " GROUP BY metric_name, labels, time HAVING count(*) > 1"
            " ORDER BY n DESC LIMIT 5"
        )
        total_dupe_rows = self.one(
            "SELECT coalesce(sum(n - 1), 0) FROM ("
            "  SELECT count(*) AS n FROM metrics_raw WHERE labels ? 'source'"
            "  GROUP BY metric_name, labels, time HAVING count(*) > 1) d"
        )
        self.check(
            "no duplicate (time, metric_name, labels) rows",
            not dupes,
            "clean" if not dupes else
            "%d redundant row(s); worst: %s %s at %s x%d"
            % (total_dupe_rows, dupes[0][0], dupes[0][1], dupes[0][2], dupes[0][3]),
        )

    def seeded_values_exact(self, source, expect_rows=None):
        """value must equal the epoch-minute of its own timestamp."""
        total = self.one(
            "SELECT count(*) FROM metrics_raw WHERE labels->>'source' = %s",
            (source,))
        if not total:
            self.check("%s: seeded values correct" % source, False,
                       "no rows to check")
            return
        bad = self.one(
            "SELECT count(*) FROM metrics_raw"
            " WHERE labels->>'source' = %s"
            "   AND value <> floor(extract(epoch from time) / 60)",
            (source,))
        self.check(
            "%s: value == epoch-minute for all %d rows" % (source, total),
            bad == 0,
            "exact match" if bad == 0 else
            "%d/%d rows have a wrong value, shifted timestamp or partial bucket"
            % (bad, total),
        )
        # timestamps must sit exactly on the 1-minute grid the align: 1m asked for
        off_grid = self.one(
            "SELECT count(*) FROM metrics_raw"
            " WHERE labels->>'source' = %s"
            "   AND extract(epoch from time)::bigint %% 60 <> 0",
            (source,))
        self.check(
            "%s: all timestamps on the 1-minute align grid" % source,
            off_grid == 0,
            "aligned" if off_grid == 0 else "%d row(s) off-grid" % off_grid,
        )
        if expect_rows is not None:
            self.check(
                "%s: row count matches seeded buckets" % source,
                total == expect_rows,
                "%d rows (expected %d)" % (total, expect_rows),
            )

    def metric_names(self, source, expected_names):
        rows = self.q(
            "SELECT DISTINCT metric_name FROM metrics_raw"
            " WHERE labels->>'source' = %s ORDER BY 1", (source,))
        found = sorted(r[0] for r in rows)
        missing = [n for n in expected_names if n not in found]
        self.check(
            "%s: expected metric names present" % source,
            not missing,
            "found %s" % (", ".join(found) or "nothing")
            if not missing else "missing %s (found %s)"
                                % (", ".join(missing), ", ".join(found)),
        )

    def rate_query_renamed(self):
        """rate() carries no __name__; the metric_name override must be used."""
        n = self.one(
            "SELECT count(*) FROM metrics_raw"
            " WHERE metric_name = 'prometheus_http_requests_rate'")
        leaked = self.one(
            "SELECT count(*) FROM metrics_raw WHERE metric_name = 'prom_scrape_rate'")
        self.check(
            "prometheus: rate() stored under metric_name override",
            n > 0 and leaked == 0,
            "%d rows as prometheus_http_requests_rate, %d leaked as the query name"
            % (n, leaked),
        )

    def extra_labels(self):
        rows = self.q(
            "SELECT labels->>'source', labels->>'demo', count(*)"
            "  FROM metrics_raw WHERE labels ? 'source'"
            " GROUP BY 1, 2 ORDER BY 1")
        missing = [r for r in rows if not r[1]]
        self.check(
            "extra_labels merged into every row",
            not missing,
            "; ".join("%s -> demo=%s (%d)" % (r[0], r[1], r[2]) for r in rows)
            if not missing else "rows without the demo label: %s" % missing,
        )

    def no_internal_columns_leaked(self):
        """Flux bookkeeping columns must not become labels."""
        leaked = self.one(
            "SELECT count(*) FROM metrics_raw"
            " WHERE labels ?| array['result','table','_start','_stop',"
            "                       '_time','_value','_field','_measurement',"
            "                       '__name__']")
        self.check(
            "no internal source columns leaked into labels",
            leaked == 0,
            "clean" if leaked == 0 else "%d row(s) carry an internal column" % leaked,
        )

    def pg_rows_mapped(self):
        """The SQL source's result: columns mapped onto metric_name/labels."""
        rows = self.q(
            "SELECT DISTINCT labels->>'queue', labels->>'host'"
            "  FROM metrics_raw WHERE labels->>'source' = 'demo-pg'"
            " ORDER BY 1, 2")
        pairs = [(r[0], r[1]) for r in rows]
        expected = [("orders", "app-1"), ("orders", "app-2"),
                    ("payments", "app-1")]
        self.check(
            "demo-pg: queue/host columns became labels",
            pairs == expected,
            "found %s" % (pairs or "NO ROWS"),
        )

    def watermarks(self, expected_streams):
        rows = self.q(
            "SELECT source_name, stream_key, last_ts, last_rows"
            "  FROM importer_state ORDER BY 1, 2")
        found = {(r[0], r[1]) for r in rows}
        missing = [s for s in expected_streams if s not in found]
        self.check(
            "watermark row per stream",
            not missing,
            "%d stream(s) tracked" % len(rows) if not missing
            else "missing watermarks for %s" % missing,
        )
        for r in rows:
            self.note("  watermark", "%s/%s -> last_ts=%s (last run wrote %d)"
                      % (r[0], r[1], r[2], r[3]))

    def rollups_populated(self):
        """The whole point of sharing metrics_raw: the existing tiers just work."""
        # refresh_continuous_aggregate() manages its own transactions and
        # refuses to run inside one, so this needs autocommit for the call.
        self.conn.commit()
        prev = self.conn.autocommit
        self.conn.autocommit = True
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "CALL refresh_continuous_aggregate('metrics_1h', NULL, NULL)")
        finally:
            self.conn.autocommit = prev
        n = self.one(
            "SELECT count(*) FROM metrics_1h WHERE labels ? 'source'")
        self.check(
            "imported data flows into the metrics_1h rollup",
            n > 0,
            "%d aggregate row(s) after refresh" % n,
        )

    # -- reporting ---------------------------------------------------------

    def report(self):
        width = max(len(n) for _, n, _ in self.results) + 2
        print()
        for status, name, detail in self.results:
            if status == WARN:
                print("      %-*s %s" % (width, name, detail))
            else:
                print("[%s] %-*s %s" % (status, width, name, detail))
        failed = [r for r in self.results if r[0] == FAIL]
        passed = [r for r in self.results if r[0] == PASS]
        print()
        print("%d passed, %d failed" % (len(passed), len(failed)))
        return 1 if failed else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn",
                    default="postgresql://postgres:demo-super-pw@127.0.0.1:15432/metrics")
    ap.add_argument("--influx-buckets", type=int, required=True,
                    help="number of complete 1-minute buckets seed.py wrote")
    ap.add_argument("--pg-buckets", type=int, default=None,
                    help="also verify the demo-postgres SQL source; value is "
                         "the number of complete 1-minute buckets of "
                         "queue_stats rows seeded by target-demo-sources.sh")
    args = ap.parse_args()

    with psycopg.connect(args.dsn, autocommit=False) as conn:
        c = Checks(conn)

        print("=" * 72)
        print("Verifying what the importer wrote to metrics_raw")
        print("=" * 72)

        expected_sources = ["demo-prom", "demo-influx1", "demo-influx2"]
        if args.pg_buckets is not None:
            expected_sources.append("demo-pg")
        c.labels_are_jsonb_objects()
        c.sources_present(expected_sources)
        c.no_duplicates()

        # 2 hosts x 2 fields, and 2 hosts x 1 field
        c.seeded_values_exact("demo-influx1", args.influx_buckets * 4)
        c.seeded_values_exact("demo-influx2", args.influx_buckets * 2)

        if args.pg_buckets is not None:
            # 3 (queue, host) series seeded per minute, aggregated by
            # date_trunc('minute', ...) into one row each.
            c.seeded_values_exact("demo-pg", args.pg_buckets * 3)
            c.pg_rows_mapped()
            c.metric_names("demo-pg", ["queue_depth_1m"])

        c.metric_names("demo-influx1", ["demo_cpu_load_mean", "demo_cpu_temp_mean"])
        c.metric_names("demo-influx2", ["demo_mem_used_percent"])
        c.metric_names("demo-prom", ["up", "go_goroutines",
                                     "prometheus_http_requests_rate"])
        c.rate_query_renamed()

        c.extra_labels()
        c.no_internal_columns_leaked()
        watermarks = [
            ("demo-prom", "prom_up"),
            ("demo-prom", "prom_goroutines"),
            ("demo-prom", "prom_scrape_rate"),
            ("demo-influx1", "cpu_1m"),
            ("demo-influx2", "mem_1m"),
        ]
        if args.pg_buckets is not None:
            watermarks.append(("demo-pg", "queue_depth_1m"))
        c.watermarks(watermarks)
        c.rollups_populated()

        return c.report()


if __name__ == "__main__":
    sys.exit(main())
