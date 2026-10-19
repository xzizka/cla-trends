"""InfluxDB 2.x source plugin (Flux over /api/v2/query, annotated CSV).

Config:

    - name: influx-v2
      type: influxdb2
      url: http://influx2.example:8086
      org: my-org
      schedule: {every: 1h}
      auth: {type: token, token: "..."}
      max_span: 1d
      queries:
        - name: mem_used
          align: 1m                  # match aggregateWindow(every: 1m)
          query: |
            from(bucket: "metrics")
              |> range(start: $__start, stop: $__stop)
              |> filter(fn: (r) => r._measurement == "mem")
              |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)

`$__start` / `$__stop` are substituted with bare RFC3339 time literals, which
is what Flux's `range()` expects (unquoted, no `time()` wrapper needed).

**Pass `timeSrc: "_start"` to `aggregateWindow`.** Flux defaults to
`timeSrc: "_stop"`, i.e. it stamps each window with its END time, whereas
InfluxQL's `GROUP BY time()` and TimescaleDB's own `time_bucket()` both stamp
the START. Left at the default, the very same measurement imported through
this plugin lands one whole interval later than it would through the
influxdb1 plugin — and since both write into one `metrics_raw`, that is a
silent one-bucket skew between sources rather than an error. Nothing here can
detect it (a stop-stamped bucket is indistinguishable from a legitimately
later one), so it has to be right in the query.

The annotated-CSV response is parsed with the stdlib `csv` module rather than
pulling in `influxdb-client`: the format is short, stable and fully specified,
and the official client would add a transitive dependency tree (and its own
Rx/reactive machinery) for what amounts to sixty lines of parsing. Same
reasoning as the receiver hand-decoding remote_write protobuf.
"""
from __future__ import annotations

import csv
import io
import logging
import math
import re
from datetime import datetime, timedelta
from typing import Iterator, List

from timespec import UTC, parse_duration

from . import register
from .base import Row, Source, SourceError, Stream, require

log = logging.getLogger("importer.influxdb2")

#: Flux bookkeeping columns — everything else on a record becomes a label.
_INTERNAL_COLUMNS = frozenset(
    ("", "result", "table", "_start", "_stop", "_time", "_value",
     "_field", "_measurement")
)

_RFC3339_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[Tt](\d{2}:\d{2}:\d{2})(?:\.(\d+))?"
    r"([Zz]|[+-]\d{2}:?\d{2})$"
)


@register("influxdb2")
class InfluxDB2Source(Source):

    @staticmethod
    def validate(cfg):
        where = "source %r" % cfg.get("name", "?")
        require(cfg, "url", where)
        require(cfg, "org", where)
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
            # A hardcoded `range(start: -1h)` would ignore the import window
            # and re-import the same hour on every tick, duplicating rows.
            if "$__start" not in text:
                raise ValueError(
                    "%s: Flux query must use `range(start: $__start, stop: "
                    "$__stop)` so it follows the import window; a fixed "
                    "range re-imports the same data every run" % qwhere
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
        flux = (q["query"]
                .replace("$__start", _flux_time(start))
                .replace("$__stop", _flux_time(end)))
        url = self.cfg["url"].rstrip("/") + "/api/v2/query"
        resp = self.http.post(
            url,
            params={"org": self.cfg["org"]},
            data=flux.encode("utf-8"),
            headers={
                "Content-Type": "application/vnd.flux",
                "Accept": "application/csv",
            },
        )
        prefix = q.get("metric_prefix", self.cfg.get("metric_prefix", "")) or ""
        want_suffix = q.get("field_suffix", True)
        for row in self._parse_csv(resp.text, q, prefix, want_suffix):
            yield row

    def _parse_csv(self, text, q, prefix, want_suffix):
        header = None
        in_error_table = False
        for cells in csv.reader(io.StringIO(text)):
            if not cells or all(c == "" for c in cells):
                header = None          # blank line separates result tables
                in_error_table = False
                continue
            if cells[0].startswith("#"):
                # #datatype / #group / #default annotations. Not needed: every
                # column this plugin cares about is identified by name and
                # converted explicitly.
                continue
            if header is None:
                header = _strip_annotation_column(cells)
                # InfluxDB reports query errors as a CSV table rather than a
                # non-2xx status, so a 200 response is not proof of success.
                in_error_table = "error" in header and "_value" not in header
                continue

            values = _strip_annotation_column(cells)
            record = dict(zip(header, values))

            if in_error_table:
                raise SourceError(
                    "query %r rejected by %s: %s (reference %s)"
                    % (q["name"], self.name,
                       record.get("error") or "unknown error",
                       record.get("reference") or "-")
                )

            raw_value = record.get("_value")
            if raw_value in (None, ""):
                continue
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                # Non-numeric Flux column (a string field, a boolean) — no
                # representation in a DOUBLE PRECISION column.
                continue
            if not math.isfinite(value):
                continue

            ts = _parse_rfc3339(record.get("_time"))
            if ts is None:
                continue

            base = q.get("metric_name") or record.get("_measurement") or q["name"]
            field = record.get("_field") or ""
            name = prefix + base + ("_" + field if (want_suffix and field) else "")

            labels = {k: v for k, v in record.items()
                      if k not in _INTERNAL_COLUMNS and v not in (None, "")}
            yield Row(ts=ts, metric_name=name, labels=labels, value=value)


def _strip_annotation_column(cells):
    """Annotated CSV reserves the first column for annotations, leaving it
    empty on header/data rows. Drop it when present."""
    return cells[1:] if cells and cells[0] == "" else cells


def _flux_time(dt):
    """Bare RFC3339 literal — Flux `range(start: 2026-09-09T10:00:00Z)` takes
    a time literal directly, quoting it would make it a string and fail."""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_rfc3339(text):
    """Parse Flux's RFC3339 timestamps, including nanosecond precision.

    `datetime.fromisoformat` cannot take 9 fractional digits (or a bare `Z`)
    on every Python it might run under, and nanoseconds have to be truncated
    to microseconds anyway to fit TIMESTAMPTZ — so do both explicitly.
    """
    if not text:
        return None
    m = _RFC3339_RE.match(text.strip())
    if not m:
        return None
    date_part, time_part, frac, offset = m.groups()
    micros = int((frac or "").ljust(6, "0")[:6]) if frac else 0
    base = datetime.strptime(date_part + "T" + time_part, "%Y-%m-%dT%H:%M:%S")
    base = base.replace(microsecond=micros, tzinfo=UTC)
    if offset not in ("Z", "z"):
        sign = 1 if offset[0] == "+" else -1
        digits = offset[1:].replace(":", "")
        delta = timedelta(hours=int(digits[:2]), minutes=int(digits[2:4]))
        base = base - sign * delta
    return base
