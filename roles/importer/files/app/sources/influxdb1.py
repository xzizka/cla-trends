"""InfluxDB 1.x source plugin (InfluxQL over /query).

Config:

    - name: influx-legacy
      type: influxdb1
      url: http://influx.example:8086
      database: telegraf
      schedule: {cron: "17 3 * * *"}
      auth: {type: basic, username: reader, password: "..."}
      max_span: 1d              # split longer backfills into 1-day requests
      queries:
        - name: cpu_idle
          # $timeFilter is expanded to a half-open `time >= .. AND time < ..`
          query: >-
            SELECT mean("usage_idle") FROM "cpu"
            WHERE $timeFilter GROUP BY time(1m), "host" fill(none)
          align: 1m             # MUST match GROUP BY time(1m) — see below
          metric_prefix: "influx_"

Why `align` matters: a `GROUP BY time(1m)` bucket that only partially overlaps
the requested window is still returned, aggregated over just the overlapping
seconds. Import windows starting at 10:00:37 would therefore store a "1-minute
average" computed from 23 seconds of data at every window edge. Setting
`align` to the GROUP BY interval snaps the window boundaries onto the same
grid InfluxDB buckets on, so every bucket imported is a whole one.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Iterator, List

from timespec import UTC, parse_duration

from . import register
from .base import Row, Source, SourceError, Stream, require

log = logging.getLogger("importer.influxdb1")

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

#: Columns that are never a metric value.
_NON_VALUE_COLUMNS = ("time",)


@register("influxdb1")
class InfluxDB1Source(Source):

    @staticmethod
    def validate(cfg):
        where = "source %r" % cfg.get("name", "?")
        require(cfg, "url", where)
        require(cfg, "database", where)
        queries = require(cfg, "queries", where)
        if not isinstance(queries, list) or not queries:
            raise ValueError("%s: `queries` must be a non-empty list" % where)
        seen = set()
        for i, q in enumerate(queries):
            qwhere = "%s query #%d" % (where, i + 1)
            if not isinstance(q, dict):
                raise ValueError("%s: must be a mapping, got %r" % (qwhere, q))
            text = require(q, "query", qwhere)
            name = require(q, "name", qwhere)
            if name in seen:
                raise ValueError(
                    "%s: duplicate query name %r — names are the watermark "
                    "keys and must be unique within a source" % (qwhere, name)
                )
            seen.add(name)
            # Without a time placeholder the query ignores the import window
            # entirely: every run would re-read the source's full retention
            # and duplicate it into metrics_raw. Refuse at startup instead.
            if "$timeFilter" not in text and "$__start" not in text:
                raise ValueError(
                    "%s: query must reference the import window via "
                    "$timeFilter (or $__start/$__stop), otherwise every run "
                    "re-imports the whole measurement" % qwhere
                )
            if "align" in q:
                parse_duration(q["align"], qwhere + ".align")

    def streams(self):
        # type: () -> List[Stream]
        default_span = parse_duration(self.cfg.get("max_span", "1d"),
                                      "source %r .max_span" % self.name)
        default_align = self.cfg.get("align")
        out = []
        for q in self.cfg["queries"]:
            align_spec = q.get("align", default_align)
            align = parse_duration(align_spec, "query %r .align" % q["name"]) \
                if align_spec else timedelta(0)
            span = parse_duration(q["max_span"], "query %r .max_span" % q["name"]) \
                if "max_span" in q else default_span
            out.append(Stream(key=q["name"], step=align, max_span=span, query=q))
        return out

    def fetch(self, stream, start, end):
        # type: (Stream, datetime, datetime) -> Iterator[Row]
        q = stream.query
        influxql = _expand_placeholders(q["query"], start, end)
        url = self.cfg["url"].rstrip("/") + "/query"
        params = {
            "db": self.cfg["database"],
            "q": influxql,
            # Integer nanoseconds: no RFC3339 parsing, no timezone ambiguity,
            # and no float rounding when converted below.
            "epoch": "ns",
        }
        if self.cfg.get("retention_policy"):
            params["rp"] = self.cfg["retention_policy"]

        payload = self.http.get(url, params=params).json()
        if payload.get("error"):
            raise SourceError("query %r rejected by %s: %s"
                              % (q["name"], self.name, payload["error"]))

        prefix = q.get("metric_prefix", self.cfg.get("metric_prefix", "")) or ""
        want_suffix = q.get("field_suffix", True)

        for result in payload.get("results") or []:
            if result.get("error"):
                raise SourceError("query %r rejected by %s: %s"
                                  % (q["name"], self.name, result["error"]))
            for series in result.get("series") or []:
                for row in self._rows_from_series(series, q, prefix, want_suffix):
                    yield row

    def _rows_from_series(self, series, q, prefix, want_suffix):
        columns = series.get("columns") or []
        if "time" not in columns:
            raise SourceError(
                "query %r returned a series without a `time` column (%r) — "
                "the importer cannot place those samples in time"
                % (q["name"], columns)
            )
        time_idx = columns.index("time")
        value_idx = [i for i, c in enumerate(columns) if c not in _NON_VALUE_COLUMNS]

        base = q.get("metric_name") or series.get("name") or q["name"]
        # Dropping the field suffix is only unambiguous with a single value
        # column; with several it would collapse different fields into one
        # metric name. Keep the suffix and say so rather than silently merging.
        suffix = want_suffix
        if not suffix and len(value_idx) > 1:
            log.warning(
                "source %s query %s: field_suffix=false but the result has %d "
                "value columns (%s) — keeping the suffix to avoid merging "
                "distinct fields into one metric name",
                self.name, q["name"], len(value_idx),
                ", ".join(columns[i] for i in value_idx),
            )
            suffix = True

        tags = {k: ("" if v is None else str(v))
                for k, v in (series.get("tags") or {}).items()}

        for values in series.get("values") or []:
            ns = values[time_idx]
            if ns is None:
                continue
            ts = _EPOCH + timedelta(microseconds=int(ns) // 1000)
            for i in value_idx:
                raw = values[i]
                if raw is None or isinstance(raw, bool):
                    continue
                try:
                    value = float(raw)
                except (TypeError, ValueError):
                    # String fields (a log message, a status word) have no
                    # place in a DOUBLE PRECISION column; skip quietly rather
                    # than failing the whole window over one column.
                    continue
                name = prefix + base + ("_" + columns[i] if suffix else "")
                yield Row(ts=ts, metric_name=name, labels=dict(tags), value=value)


def _expand_placeholders(text, start, end):
    """Substitute the import window into the InfluxQL text.

    Half-open on purpose (`>= start`, `< end`) to match the importer's
    watermark contract: the sample at exactly `end` belongs to the next
    window, so it is imported there and never twice.
    """
    start_lit = _influx_literal(start)
    end_lit = _influx_literal(end)
    time_filter = "time >= '%s' AND time < '%s'" % (start_lit, end_lit)
    return (text
            .replace("$timeFilter", time_filter)
            .replace("$__start", "'%s'" % start_lit)
            .replace("$__stop", "'%s'" % end_lit))


def _influx_literal(dt):
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
