#!/usr/bin/env python3
"""Offline unit tests for the importer's correctness-critical logic.

Runs with no TimescaleDB, no Prometheus and no InfluxDB: the source plugins
are driven through a fake HTTP client that replays captured response bodies,
and the database is stubbed. What it actually checks is the handful of things
that would silently corrupt the metrics if they were wrong:

  * windows are half-open and contiguous, so re-runs neither duplicate nor
    skip samples (`metrics_raw` has no unique constraint to catch it)
  * the watermark cap and bootstrap behave as documented
  * window boundaries land on a stable grid
  * each plugin maps its own response format onto (time, metric_name, labels,
    value) correctly, including the awkward cases: __name__-less PromQL
    results, InfluxQL multi-field series, Flux annotated CSV and its
    error-as-200 responses
  * provenance labels are always applied and cannot be forged by a plugin

Usage (no arguments):  python3 tests/importer_unit_test.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone

APP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "..", "roles", "importer", "files", "app")
sys.path.insert(0, os.path.abspath(APP))

UTC = timezone.utc

import timespec  # noqa: E402
from timespec import IntervalSchedule, align_down, parse_duration, plan_windows  # noqa: E402


def _try_import(name):
    try:
        __import__(name)
        return True
    except ImportError:
        return False


def T(*args):
    return datetime(*args, tzinfo=UTC)


class FakeResponse(object):
    def __init__(self, body, status=200):
        self.text = body if isinstance(body, str) else json.dumps(body)
        self.status_code = status

    def json(self):
        return json.loads(self.text)


class FakeHttp(object):
    """Stands in for HttpClient; records calls and replays canned bodies."""

    def __init__(self, body):
        self.body = body
        self.calls = []

    def get(self, url, params=None):
        self.calls.append(("GET", url, params))
        return FakeResponse(self.body)

    def post(self, url, params=None, data=None, headers=None):
        self.calls.append(("POST", url, params, data))
        return FakeResponse(self.body)


# ---------------------------------------------------------------------------
# timespec
# ---------------------------------------------------------------------------

class TestDurations(unittest.TestCase):
    def test_units(self):
        self.assertEqual(parse_duration("500ms"), timedelta(milliseconds=500))
        self.assertEqual(parse_duration("30s"), timedelta(seconds=30))
        self.assertEqual(parse_duration("5m"), timedelta(minutes=5))
        self.assertEqual(parse_duration("1h"), timedelta(hours=1))
        self.assertEqual(parse_duration("7d"), timedelta(days=7))
        self.assertEqual(parse_duration("2w"), timedelta(weeks=2))

    def test_bare_number_rejected(self):
        # `step: 60` is ambiguous to a reader and a silent bug if guessed.
        for bad in (60, "60", "1 fortnight", "", None):
            with self.assertRaises(ValueError):
                parse_duration(bad)

    def test_roundtrip_format(self):
        for spec in ("30s", "5m", "1h", "7d"):
            self.assertEqual(timespec.format_duration(parse_duration(spec)), spec)


class TestAlign(unittest.TestCase):
    def test_align_down_to_grid(self):
        self.assertEqual(align_down(T(2026, 9, 9, 10, 37, 42), timedelta(minutes=1)),
                         T(2026, 9, 9, 10, 37, 0))
        self.assertEqual(align_down(T(2026, 9, 9, 10, 37, 42), timedelta(hours=1)),
                         T(2026, 9, 9, 10, 0, 0))

    def test_already_aligned_is_unchanged(self):
        dt = T(2026, 9, 9, 10, 0, 0)
        self.assertEqual(align_down(dt, timedelta(hours=1)), dt)


class TestPlanWindows(unittest.TestCase):
    hour = timedelta(hours=1)
    day = timedelta(days=1)

    def plan(self, last_ts, now, lookback="7d", end_lag="0s", span="1d", step=None):
        return plan_windows(last_ts, now, parse_duration(lookback),
                            parse_duration(end_lag), parse_duration(span), step)

    def test_first_run_bootstraps_from_lookback(self):
        now = T(2026, 9, 9, 12, 0)
        w = self.plan(None, now, lookback="3d", span="7d")
        self.assertEqual(w, [(T(2026, 9, 6, 12, 0), now)])

    def test_windows_are_contiguous_and_half_open(self):
        # Contiguity is the no-gap guarantee; the shared boundary being
        # half-open is the no-duplicate guarantee.
        w = self.plan(T(2026, 9, 6, 0, 0), T(2026, 9, 9, 0, 0), span="1d")
        self.assertEqual(len(w), 3)
        for (_, prev_end), (next_start, _) in zip(w, w[1:]):
            self.assertEqual(prev_end, next_start)
        self.assertEqual(w[0][0], T(2026, 9, 6, 0, 0))
        self.assertEqual(w[-1][1], T(2026, 9, 9, 0, 0))

    def test_resume_starts_exactly_at_watermark(self):
        # The watermark is the exclusive upper bound of what is stored, so the
        # next window must begin *at* it — not after it (gap) and not before
        # it (duplicate).
        wm = T(2026, 9, 9, 10, 0)
        w = self.plan(wm, T(2026, 9, 9, 11, 0))
        self.assertEqual(w[0][0], wm)

    def test_nothing_to_do_when_caught_up(self):
        now = T(2026, 9, 9, 12, 0)
        self.assertEqual(self.plan(now, now), [])
        # watermark ahead of now (clock skew) must not produce a backwards window
        self.assertEqual(self.plan(now + self.hour, now), [])

    def test_end_lag_holds_back_the_upper_bound(self):
        now = T(2026, 9, 9, 12, 0)
        w = self.plan(T(2026, 9, 9, 10, 0), now, end_lag="10m")
        self.assertEqual(w[-1][1], T(2026, 9, 9, 11, 50))

    def test_lookback_caps_a_stale_watermark(self):
        # After a long outage the importer must not request data the raw
        # tier's retention would drop anyway; it reports the gap instead.
        now = T(2026, 9, 9, 12, 0)
        w = self.plan(T(2026, 1, 1, 0, 0), now, lookback="7d", span="7d")
        self.assertEqual(w[0][0], T(2026, 9, 2, 12, 0))

    def test_step_alignment_is_stable_across_runs(self):
        # Same watermark, two different tick times -> identical window start,
        # so stored sample timestamps stay on one grid instead of drifting.
        step = timedelta(minutes=1)
        a = self.plan(None, T(2026, 9, 9, 12, 0, 17), lookback="1h", step=step)
        b = self.plan(None, T(2026, 9, 9, 12, 0, 49), lookback="1h", step=step)
        self.assertEqual(a[0][0], b[0][0])
        self.assertEqual(a[0][0], T(2026, 9, 9, 11, 0, 0))

    def test_span_bounds_every_window(self):
        w = self.plan(T(2026, 9, 1, 0, 0), T(2026, 9, 9, 0, 0), lookback="30d",
                      span="1d")
        self.assertTrue(all(end - start <= self.day for start, end in w))


class TestSchedules(unittest.TestCase):
    def test_interval_is_grid_anchored(self):
        # An hourly source fires at :00, not at whatever minute the container
        # happened to start.
        s = IntervalSchedule(timedelta(hours=1))
        self.assertEqual(s.next_after(T(2026, 9, 9, 10, 37, 12)),
                         T(2026, 9, 9, 11, 0, 0))

    def test_interval_always_moves_forward(self):
        s = IntervalSchedule(timedelta(hours=1))
        now = T(2026, 9, 9, 10, 0, 0)
        self.assertGreater(s.next_after(now), now)

    def test_zero_interval_rejected(self):
        with self.assertRaises(ValueError):
            IntervalSchedule(timedelta(0))

    def test_schedule_requires_exactly_one_of_every_cron(self):
        from timespec import build_schedule
        with self.assertRaises(ValueError):
            build_schedule({}, "src")
        with self.assertRaises(ValueError):
            build_schedule({"every": "1h", "cron": "0 3 * * *"}, "src")
        self.assertEqual(build_schedule({"every": "1h"}, "src").every,
                         timedelta(hours=1))

    @unittest.skipUnless(_try_import("croniter"), "croniter not installed")
    def test_cron_daily(self):
        from timespec import CronSchedule
        s = CronSchedule("0 3 * * *")
        self.assertEqual(s.next_after(T(2026, 9, 9, 10, 0)), T(2026, 9, 10, 3, 0))


# ---------------------------------------------------------------------------
# source plugins
# ---------------------------------------------------------------------------

class TestPrometheus(unittest.TestCase):
    def build(self, cfg, body):
        from sources.prometheus import PrometheusSource
        PrometheusSource.validate(cfg)
        http = FakeHttp(body)
        return PrometheusSource(cfg, http), http

    BASE = {
        "name": "prom", "type": "prometheus", "url": "http://p:9090",
        "queries": [{"name": "up", "query": "up"}],
    }

    def test_parses_matrix_and_moves_name_out_of_labels(self):
        body = {"status": "success", "data": {"resultType": "matrix", "result": [
            {"metric": {"__name__": "up", "job": "node", "instance": "a:9100"},
             "values": [[1788940800, "1"], [1788940860, "0"]]},
        ]}}
        src, _ = self.build(self.BASE, body)
        stream = src.streams()[0]
        rows = list(src.fetch(stream, T(2026, 9, 9, 8, 0), T(2026, 9, 9, 9, 0)))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].metric_name, "up")
        self.assertNotIn("__name__", rows[0].labels)
        self.assertEqual(rows[0].labels, {"job": "node", "instance": "a:9100"})
        self.assertEqual(rows[0].value, 1.0)
        self.assertEqual(rows[1].value, 0.0)

    def test_non_finite_samples_dropped(self):
        # NaN/Inf are legal Prometheus values but poison avg()/sum() in the
        # continuous aggregates.
        body = {"status": "success", "data": {"resultType": "matrix", "result": [
            {"metric": {"__name__": "x"},
             "values": [[1788940800, "NaN"], [1788940860, "+Inf"],
                        [1788940920, "3.5"]]},
        ]}}
        src, _ = self.build(self.BASE, body)
        rows = list(src.fetch(src.streams()[0], T(2026, 9, 9), T(2026, 9, 10)))
        self.assertEqual([r.value for r in rows], [3.5])

    def test_expression_without_name_uses_configured_metric_name(self):
        cfg = {"name": "prom", "type": "prometheus", "url": "http://p:9090",
               "queries": [{"name": "cpu_rate",
                            "query": "rate(node_cpu_seconds_total[5m])",
                            "metric_name": "node_cpu_seconds_rate"}]}
        body = {"status": "success", "data": {"resultType": "matrix", "result": [
            {"metric": {"cpu": "0"}, "values": [[1788940800, "0.25"]]},
        ]}}
        src, _ = self.build(cfg, body)
        rows = list(src.fetch(src.streams()[0], T(2026, 9, 9), T(2026, 9, 10)))
        self.assertEqual(rows[0].metric_name, "node_cpu_seconds_rate")

    def test_name_falls_back_to_query_name(self):
        cfg = {"name": "prom", "type": "prometheus", "url": "http://p:9090",
               "queries": [{"name": "cpu_rate", "query": "rate(x[5m])"}]}
        body = {"status": "success", "data": {"resultType": "matrix", "result": [
            {"metric": {}, "values": [[1788940800, "1"]]}]}}
        src, _ = self.build(cfg, body)
        rows = list(src.fetch(src.streams()[0], T(2026, 9, 9), T(2026, 9, 10)))
        self.assertEqual(rows[0].metric_name, "cpu_rate")

    def test_api_error_raises_source_error(self):
        from sources.base import SourceError
        src, _ = self.build(self.BASE, {"status": "error",
                                        "errorType": "bad_data",
                                        "error": "parse error"})
        with self.assertRaises(SourceError):
            list(src.fetch(src.streams()[0], T(2026, 9, 9), T(2026, 9, 10)))

    def test_wrong_result_type_raises(self):
        from sources.base import SourceError
        src, _ = self.build(self.BASE, {"status": "success",
                                        "data": {"resultType": "scalar",
                                                 "result": []}})
        with self.assertRaises(SourceError):
            list(src.fetch(src.streams()[0], T(2026, 9, 9), T(2026, 9, 10)))

    def test_max_span_derives_from_step_and_point_limit(self):
        # Prometheus rejects range queries resolving to >11000 points, so the
        # window has to be bounded in points, not wall time.
        cfg = dict(self.BASE, step="15s", max_points_per_request=1000)
        src, _ = self.build(cfg, {})
        self.assertEqual(src.streams()[0].max_span, timedelta(seconds=15 * 1000))

    def test_request_params(self):
        src, http = self.build(self.BASE, {"status": "success",
                                           "data": {"resultType": "matrix",
                                                    "result": []}})
        list(src.fetch(src.streams()[0], T(2026, 9, 9, 8, 0), T(2026, 9, 9, 9, 0)))
        method, url, params = http.calls[0]
        self.assertEqual(method, "GET")
        self.assertTrue(url.endswith("/api/v1/query_range"))
        self.assertEqual(params["query"], "up")
        self.assertEqual(params["step"], "60s")

    def test_duplicate_query_names_rejected(self):
        from sources.prometheus import PrometheusSource
        cfg = {"name": "p", "type": "prometheus", "url": "u", "queries": [
            {"name": "a", "query": "up"}, {"name": "a", "query": "down"}]}
        with self.assertRaises(ValueError):
            PrometheusSource.validate(cfg)


class TestInfluxDB1(unittest.TestCase):
    BASE = {
        "name": "influx1", "type": "influxdb1", "url": "http://i:8086",
        "database": "telegraf",
        "queries": [{"name": "cpu",
                     "query": 'SELECT mean("usage_idle") FROM "cpu" WHERE $timeFilter GROUP BY time(1m), "host"'}],
    }

    def build(self, cfg, body):
        from sources.influxdb1 import InfluxDB1Source
        InfluxDB1Source.validate(cfg)
        http = FakeHttp(body)
        return InfluxDB1Source(cfg, http), http

    def test_measurement_and_field_become_metric_name(self):
        body = {"results": [{"series": [{
            "name": "cpu", "tags": {"host": "a"},
            "columns": ["time", "mean"],
            "values": [[1788940800000000000, 91.5]],
        }]}]}
        src, _ = self.build(self.BASE, body)
        rows = list(src.fetch(src.streams()[0], T(2026, 9, 9), T(2026, 9, 10)))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].metric_name, "cpu_mean")
        self.assertEqual(rows[0].labels, {"host": "a"})
        self.assertEqual(rows[0].value, 91.5)
        self.assertEqual(rows[0].ts, datetime(2026, 9, 9, 8, 0, tzinfo=UTC))

    def test_nanosecond_timestamps_are_exact(self):
        # Converting ns via float seconds loses precision at epoch scale; the
        # plugin must go through integer microseconds.
        ns = 1788940800123456789
        body = {"results": [{"series": [{
            "name": "m", "columns": ["time", "v"], "values": [[ns, 1.0]]}]}]}
        src, _ = self.build(self.BASE, body)
        rows = list(src.fetch(src.streams()[0], T(2026, 9, 9), T(2026, 9, 10)))
        self.assertEqual(rows[0].ts.microsecond, 123456)

    def test_multiple_fields_and_nulls(self):
        body = {"results": [{"series": [{
            "name": "mem", "tags": {},
            "columns": ["time", "used", "free", "note"],
            "values": [[1788940800000000000, 10.0, None, "text"]],
        }]}]}
        src, _ = self.build(self.BASE, body)
        rows = list(src.fetch(src.streams()[0], T(2026, 9, 9), T(2026, 9, 10)))
        # null field skipped, non-numeric field skipped, one row for `used`
        self.assertEqual([(r.metric_name, r.value) for r in rows],
                         [("mem_used", 10.0)])

    def test_time_filter_is_half_open(self):
        src, http = self.build(self.BASE, {"results": []})
        list(src.fetch(src.streams()[0], T(2026, 9, 9, 8, 0), T(2026, 9, 9, 9, 0)))
        q = http.calls[0][2]["q"]
        self.assertIn("time >= '2026-09-09T08:00:00.000000Z'", q)
        self.assertIn("time < '2026-09-09T09:00:00.000000Z'", q)
        self.assertNotIn("$timeFilter", q)

    def test_query_without_time_placeholder_rejected(self):
        from sources.influxdb1 import InfluxDB1Source
        cfg = dict(self.BASE, queries=[{"name": "x",
                                        "query": 'SELECT * FROM "cpu"'}])
        with self.assertRaises(ValueError) as ctx:
            InfluxDB1Source.validate(cfg)
        self.assertIn("re-imports", str(ctx.exception))

    def test_result_error_raises(self):
        from sources.base import SourceError
        src, _ = self.build(self.BASE, {"results": [{"error": "database not found"}]})
        with self.assertRaises(SourceError):
            list(src.fetch(src.streams()[0], T(2026, 9, 9), T(2026, 9, 10)))

    def test_align_becomes_stream_step(self):
        cfg = dict(self.BASE, queries=[dict(self.BASE["queries"][0], align="1m")])
        src, _ = self.build(cfg, {})
        self.assertEqual(src.streams()[0].step, timedelta(minutes=1))


class TestInfluxDB2(unittest.TestCase):
    BASE = {
        "name": "influx2", "type": "influxdb2", "url": "http://i:8086",
        "org": "acme",
        "queries": [{"name": "mem", "query":
                     'from(bucket:"m") |> range(start: $__start, stop: $__stop)'}],
    }

    CSV = (
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string\r\n"
        "#group,false,false,true,true,false,false,true,true,true\r\n"
        "#default,_result,,,,,,,,\r\n"
        ",result,table,_start,_stop,_time,_value,_field,_measurement,host\r\n"
        ",,0,2026-09-09T08:00:00Z,2026-09-09T09:00:00Z,2026-09-09T08:00:00Z,42.5,used_percent,mem,srv1\r\n"
        ",,0,2026-09-09T08:00:00Z,2026-09-09T09:00:00Z,2026-09-09T08:01:00.5Z,43.5,used_percent,mem,srv1\r\n"
    )

    def build(self, cfg, body):
        from sources.influxdb2 import InfluxDB2Source
        InfluxDB2Source.validate(cfg)
        http = FakeHttp(body)
        return InfluxDB2Source(cfg, http), http

    def test_parses_annotated_csv(self):
        src, _ = self.build(self.BASE, self.CSV)
        rows = list(src.fetch(src.streams()[0], T(2026, 9, 9, 8, 0),
                              T(2026, 9, 9, 9, 0)))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].metric_name, "mem_used_percent")
        # Flux internals (result/table/_start/_stop) must not leak into labels
        self.assertEqual(rows[0].labels, {"host": "srv1"})
        self.assertEqual(rows[0].value, 42.5)
        self.assertEqual(rows[0].ts, datetime(2026, 9, 9, 8, 0, tzinfo=UTC))
        self.assertEqual(rows[1].ts, datetime(2026, 9, 9, 8, 1, 0, 500000, tzinfo=UTC))

    def test_nanosecond_precision_truncated_not_rejected(self):
        csv_body = self.CSV.replace("2026-09-09T08:00:00Z,42.5",
                                    "2026-09-09T08:00:00.123456789Z,42.5")
        src, _ = self.build(self.BASE, csv_body)
        rows = list(src.fetch(src.streams()[0], T(2026, 9, 9), T(2026, 9, 10)))
        self.assertEqual(rows[0].ts.microsecond, 123456)

    def test_error_reported_as_200_csv_raises(self):
        # InfluxDB answers a bad Flux query with HTTP 200 and an error table,
        # so a 2xx status is not proof of success.
        from sources.base import SourceError
        body = ("#datatype,string,string\r\n"
                "#group,true,true\r\n"
                "#default,,\r\n"
                ",error,reference\r\n"
                ",failed to parse query,897\r\n")
        src, _ = self.build(self.BASE, body)
        with self.assertRaises(SourceError) as ctx:
            list(src.fetch(src.streams()[0], T(2026, 9, 9), T(2026, 9, 10)))
        self.assertIn("failed to parse query", str(ctx.exception))

    def test_window_substituted_as_bare_flux_literals(self):
        src, http = self.build(self.BASE, ",result\r\n")
        list(src.fetch(src.streams()[0], T(2026, 9, 9, 8, 0), T(2026, 9, 9, 9, 0)))
        _method, url, params, data = http.calls[0]
        self.assertTrue(url.endswith("/api/v2/query"))
        self.assertEqual(params["org"], "acme")
        flux = data.decode("utf-8")
        self.assertIn("range(start: 2026-09-09T08:00:00.000000Z", flux)
        # unquoted: a quoted value would be a string, not a Flux time literal
        self.assertNotIn("'2026-09-09", flux)
        self.assertNotIn('"2026-09-09', flux)

    def test_fixed_range_rejected(self):
        from sources.influxdb2 import InfluxDB2Source
        cfg = dict(self.BASE, queries=[{"name": "x", "query":
                                        'from(bucket:"m") |> range(start: -1h)'}])
        with self.assertRaises(ValueError):
            InfluxDB2Source.validate(cfg)

    def test_non_numeric_values_skipped(self):
        body = self.CSV.replace(",42.5,", ",some-string,")
        src, _ = self.build(self.BASE, body)
        rows = list(src.fetch(src.streams()[0], T(2026, 9, 9), T(2026, 9, 10)))
        self.assertEqual([r.value for r in rows], [43.5])


class TestLogScrubbing(unittest.TestCase):
    """Credentials must not reach the container log.

    InfluxDB 1.x accepts u=/p= as query parameters and a url: with embedded
    userinfo is legal, so anything that gets logged has to be scrubbed —
    including the text `requests` builds for its own exceptions, which is
    assembled from the full URL.
    """

    def scrub(self, text, **kw):
        from httpclient import _scrub
        return _scrub(text, **kw)

    def test_query_string_elided_from_absolute_url(self):
        out = self.scrub("http://h:8086/query?db=demo&u=admin&p=s3cr3t")
        self.assertNotIn("s3cr3t", out)
        self.assertIn("http://h:8086/query", out)

    def test_query_string_elided_from_bare_path(self):
        # This is the shape requests' own exception text uses.
        out = self.scrub("Max retries exceeded with url: /query?db=d&p=s3cr3t "
                         "(Caused by NameResolutionError)")
        self.assertNotIn("s3cr3t", out)
        self.assertIn("NameResolutionError", out)

    def test_userinfo_elided(self):
        out = self.scrub("http://admin:s3cr3t@influx:8086/query")
        self.assertNotIn("s3cr3t", out)
        self.assertIn("influx:8086", out)

    def test_plain_text_survives(self):
        msg = "database not found: demo"
        self.assertEqual(self.scrub(msg), msg)

    def test_truncated(self):
        out = self.scrub("x" * 500, limit=100)
        self.assertTrue(out.endswith("..."))
        self.assertLessEqual(len(out), 103)


# ---------------------------------------------------------------------------
# registry + config
# ---------------------------------------------------------------------------

class TestRegistry(unittest.TestCase):
    def test_known_types(self):
        import sources
        self.assertEqual(sorted(sources.REGISTRY),
                         ["influxdb1", "influxdb2", "prometheus"])

    def test_unknown_type_names_the_alternatives(self):
        import sources
        with self.assertRaises(ValueError) as ctx:
            sources.build({"name": "x", "type": "graphite"}, FakeHttp(""))
        self.assertIn("prometheus", str(ctx.exception))

class TestConfig(unittest.TestCase):
    def write(self, text):
        fh = tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False)
        fh.write(text)
        fh.close()
        self.addCleanup(os.unlink, fh.name)
        return fh.name

    def test_env_expansion(self):
        import config
        out = config.expand_env({"auth": {"token": "${TOK}"}}, env={"TOK": "s3cr3t"})
        self.assertEqual(out["auth"]["token"], "s3cr3t")

    def test_missing_env_var_is_a_hard_error(self):
        # Expanding to "" would turn a config mistake into a confusing 401.
        import config
        with self.assertRaises(ValueError) as ctx:
            config.expand_env({"token": "${NOPE}"}, env={})
        self.assertIn("vault_importer_env", str(ctx.exception))

    def test_loads_prometheus_source(self):
        import config
        path = self.write(
            "defaults:\n  max_lookback: 2d\n"
            "sources:\n"
            "  - name: prom\n    type: prometheus\n    url: http://p:9090\n"
            "    schedule: {every: 1h}\n"
            "    extra_labels: {env: prod}\n"
            "    queries:\n      - name: up\n        query: up\n"
        )
        jobs = config.load(path)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].max_lookback, timedelta(days=2))
        self.assertEqual(jobs[0].extra_labels, {"env": "prod"})
        self.assertEqual(len(jobs[0].streams), 1)

    def test_disabled_source_skipped(self):
        import config
        path = self.write(
            "sources:\n  - name: prom\n    type: prometheus\n    enabled: false\n"
            "    url: http://p\n    schedule: {every: 1h}\n"
            "    queries: [{name: up, query: up}]\n"
        )
        self.assertEqual(config.load(path), [])

    def test_duplicate_source_names_rejected(self):
        import config
        path = self.write(
            "sources:\n"
            "  - {name: a, type: prometheus, url: 'u', schedule: {every: 1h},"
            " queries: [{name: up, query: up}]}\n"
            "  - {name: a, type: prometheus, url: 'u', schedule: {every: 1h},"
            " queries: [{name: up, query: up}]}\n"
        )
        with self.assertRaises(ValueError):
            config.load(path)

    def test_reserved_source_label_rejected(self):
        import config
        path = self.write(
            "sources:\n  - name: a\n    type: prometheus\n    url: u\n"
            "    schedule: {every: 1h}\n    extra_labels: {source: fake}\n"
            "    queries: [{name: up, query: up}]\n"
        )
        with self.assertRaises(ValueError) as ctx:
            config.load(path)
        self.assertIn("reserved", str(ctx.exception))

    def test_unknown_defaults_key_rejected(self):
        import config
        path = self.write("defaults:\n  max_lookbcak: 7d\nsources: []\n")
        with self.assertRaises(ValueError) as ctx:
            config.load(path)
        self.assertIn("max_lookbcak", str(ctx.exception))

    def test_empty_config_is_valid(self):
        import config
        self.assertEqual(config.load(self.write("sources: []\n")), [])


# ---------------------------------------------------------------------------
# runner: the window filter and provenance labels
# ---------------------------------------------------------------------------

class FakeStore(object):
    def __init__(self, watermarks=None):
        self.wm = dict(watermarks or {})
        self.commits = []

    def get_watermark(self, source, stream):
        return self.wm.get((source, stream))

    def commit_window(self, source, stream, rows, watermark):
        # `rows` is a generator in the real code path (streamed into COPY), so
        # consume it exactly once — mirroring the real Store, which cannot
        # rewind it either.
        materialised = list(rows)
        self.commits.append((source, stream, materialised, watermark))
        self.wm[(source, stream)] = watermark
        return len(materialised)


class TestRunner(unittest.TestCase):
    """Exercises importer._run_window / run_stream with the DB stubbed out."""

    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("IMPORTER_PG_DSN", "postgresql://stub")
        import importer
        cls.importer = importer

    def make_job(self, rows, lookback="7d", span="1d", step=None):
        from sources.base import Stream
        import config as cfgmod

        class StubSource(object):
            type_name = "stub"
            name = "stub-src"

            def fetch(self, stream, start, end):
                return iter(rows)

        stream = Stream(key="s1", step=step or timedelta(0),
                        max_span=parse_duration(span), query={})
        job = cfgmod.Job(source=StubSource(), schedule=IntervalSchedule(timedelta(hours=1)),
                         streams=[stream], max_lookback=parse_duration(lookback),
                         end_lag=timedelta(0), run_on_start=True,
                         extra_labels={"env": "test"})
        job.name = "stub-src"
        return job, stream

    def rows_for(self, *specs):
        from sources.base import Row
        return [Row(ts=ts, metric_name="m", labels=dict(lbl), value=v)
                for ts, v, lbl in specs]

    def test_samples_outside_the_window_are_dropped(self):
        # A source returning a wider range than asked would otherwise write
        # samples the watermark is about to declare already-imported.
        start, end = T(2026, 9, 9, 8, 0), T(2026, 9, 9, 9, 0)
        rows = self.rows_for(
            (start - timedelta(seconds=1), 1.0, {}),   # before window
            (start, 2.0, {}),                          # inclusive lower bound
            (T(2026, 9, 9, 8, 30), 3.0, {}),
            (end, 4.0, {}),                            # exclusive upper bound
            (end + timedelta(seconds=1), 5.0, {}),     # after window
        )
        job, stream = self.make_job(rows)
        store = FakeStore()
        self.importer._run_window(job, stream, store, start, end)
        kept = [r.value for r in store.commits[0][2]]
        self.assertEqual(kept, [2.0, 3.0])

    def test_provenance_labels_applied_and_not_forgeable(self):
        start, end = T(2026, 9, 9, 8, 0), T(2026, 9, 9, 9, 0)
        rows = self.rows_for((start, 1.0, {"host": "a", "source": "spoofed"}))
        job, stream = self.make_job(rows)
        store = FakeStore()
        self.importer._run_window(job, stream, store, start, end)
        labels = store.commits[0][2][0].labels
        self.assertEqual(labels["source"], "stub-src")
        self.assertEqual(labels["env"], "test")
        self.assertEqual(labels["host"], "a")

    def test_rows_are_streamed_not_buffered(self):
        # The runner must hand the store a lazy iterable so a large window
        # costs constant memory; a list here would silently reintroduce the
        # buffering this was changed away from.
        start, end = T(2026, 9, 9, 8, 0), T(2026, 9, 9, 9, 0)
        job, stream = self.make_job(self.rows_for((start, 1.0, {})))
        captured = {}

        class CapturingStore(FakeStore):
            def commit_window(self, source, stream_key, rows, watermark):
                captured["is_iterator"] = iter(rows) is rows
                return FakeStore.commit_window(self, source, stream_key, rows,
                                               watermark)

        self.importer._run_window(job, stream, CapturingStore(), start, end)
        self.assertTrue(captured["is_iterator"])

    def test_row_cap_aborts_the_window_without_moving_the_watermark(self):
        from sources.base import SourceError
        start, end = T(2026, 9, 9, 8, 0), T(2026, 9, 9, 9, 0)
        many = self.rows_for(*[(start, float(i), {}) for i in range(5)])
        job, stream = self.make_job(many)
        store = FakeStore()
        original = self.importer.MAX_ROWS_PER_WINDOW
        self.importer.MAX_ROWS_PER_WINDOW = 3
        try:
            with self.assertRaises(SourceError):
                # the generator is consumed by commit_window, so the guard
                # fires mid-COPY and must propagate rather than truncate
                list(self.importer._prepare_rows(job, stream, start, end))
        finally:
            self.importer.MAX_ROWS_PER_WINDOW = original
        self.assertEqual(store.commits, [])

    def test_watermark_moves_to_window_end(self):
        start, end = T(2026, 9, 9, 8, 0), T(2026, 9, 9, 9, 0)
        job, stream = self.make_job(self.rows_for((start, 1.0, {})))
        store = FakeStore()
        self.importer._run_window(job, stream, store, start, end)
        self.assertEqual(store.commits[0][3], end)

    def test_empty_window_still_advances_the_watermark(self):
        # Otherwise a source with an hour of no data would re-request that
        # hour forever.
        start, end = T(2026, 9, 9, 8, 0), T(2026, 9, 9, 9, 0)
        job, stream = self.make_job([])
        store = FakeStore()
        self.importer._run_window(job, stream, store, start, end)
        self.assertEqual(store.commits, [("stub-src", "s1", [], end)])

    def test_run_stream_covers_every_window_once(self):
        job, stream = self.make_job([], lookback="3d", span="1d")
        store = FakeStore()
        now = T(2026, 9, 9, 0, 0)
        self.importer.run_stream(job, stream, store, now)
        ends = [c[3] for c in store.commits]
        self.assertEqual(ends, [T(2026, 9, 7), T(2026, 9, 8), T(2026, 9, 9)])
        self.assertEqual(store.wm[("stub-src", "s1")], now)

    def test_failed_stream_leaves_watermark_untouched(self):
        from sources.base import SourceError

        class Boom(object):
            type_name = "stub"
            name = "stub-src"

            def fetch(self, stream, start, end):
                raise SourceError("endpoint down")

        job, stream = self.make_job([])
        job.source = Boom()
        store = FakeStore({("stub-src", "s1"): T(2026, 9, 8, 0, 0)})
        job.streams = [stream]
        self.importer.run_job(job, store)
        # nothing committed, so the same window is retried next tick
        self.assertEqual(store.commits, [])
        self.assertEqual(store.wm[("stub-src", "s1")], T(2026, 9, 8, 0, 0))


if __name__ == "__main__":
    unittest.main(verbosity=2)
