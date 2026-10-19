"""Prometheus (and any /api/v1-compatible endpoint: Thanos, Mimir, Cortex,
VictoriaMetrics) source plugin.

Uses `/api/v1/query_range`, not `/federate` or an instant query: a range
query returns every sample in the window at the configured resolution, so
importing once an hour still yields a full-resolution hour of data rather
than a single point per hour. That is the whole reason this importer can
share `metrics_raw` with the push-based receiver — the shape of what lands
in the table is the same either way.

Config (one entry of `sources:` in sources.yml):

    - name: prom-main
      type: prometheus
      url: http://prometheus.example:9090
      schedule: {every: 1h}
      auth: {type: bearer, token: "..."}
      step: 60s                     # default for every query below
      queries:
        - name: cpu_rate            # watermark key — keep it stable
          query: 'rate(node_cpu_seconds_total[5m])'
          metric_name: node_cpu_rate   # required: rate() has no __name__
        - name: up
          query: 'up'
          step: 30s                 # per-query override
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta
from typing import Iterator, List

from timespec import UTC, parse_duration

from . import register
from .base import Row, Source, SourceError, Stream, require

log = logging.getLogger("importer.prometheus")

#: Prometheus refuses a range query resolving to more than 11000 points per
#: series. Staying meaningfully under it leaves headroom and keeps individual
#: responses a sane size, and the runner chunks longer windows automatically.
DEFAULT_MAX_POINTS = 9000


@register("prometheus")
class PrometheusSource(Source):

    @staticmethod
    def validate(cfg):
        where = "source %r" % cfg.get("name", "?")
        require(cfg, "url", where)
        queries = require(cfg, "queries", where)
        if not isinstance(queries, list) or not queries:
            raise ValueError("%s: `queries` must be a non-empty list" % where)
        seen = set()
        for i, q in enumerate(queries):
            qwhere = "%s query #%d" % (where, i + 1)
            if not isinstance(q, dict):
                raise ValueError("%s: must be a mapping, got %r" % (qwhere, q))
            require(q, "query", qwhere)
            name = require(q, "name", qwhere)
            if name in seen:
                raise ValueError(
                    "%s: duplicate query name %r — names are the watermark "
                    "keys and must be unique within a source" % (qwhere, name)
                )
            seen.add(name)
            if "step" in q:
                parse_duration(q["step"], qwhere + ".step")

    def streams(self):
        # type: () -> List[Stream]
        default_step = parse_duration(self.cfg.get("step", "60s"),
                                      "source %r .step" % self.name)
        max_points = int(self.cfg.get("max_points_per_request", DEFAULT_MAX_POINTS))
        out = []
        for q in self.cfg["queries"]:
            step = parse_duration(q["step"], "query %r .step" % q["name"]) \
                if "step" in q else default_step
            out.append(Stream(
                key=q["name"],
                step=step,
                # A window is bounded by how many points it would resolve to,
                # which is what the endpoint actually limits — not by wall time.
                max_span=step * max_points,
                query=q,
            ))
        return out

    def fetch(self, stream, start, end):
        # type: (Stream, datetime, datetime) -> Iterator[Row]
        q = stream.query
        params = {
            "query": q["query"],
            "start": "%.3f" % start.timestamp(),
            "end": "%.3f" % end.timestamp(),
            "step": "%gs" % stream.step.total_seconds(),
        }
        url = self.cfg["url"].rstrip("/") + "/api/v1/query_range"
        payload = self.http.get(url, params=params).json()

        if payload.get("status") != "success":
            raise SourceError(
                "query %r rejected by %s: %s"
                % (q["name"], self.name,
                   payload.get("error") or payload.get("errorType") or payload)
            )

        data = payload.get("data") or {}
        result_type = data.get("resultType")
        if result_type != "matrix":
            # query_range always answers with a matrix; anything else means the
            # expression is not a range vector and would silently import nothing.
            raise SourceError(
                "query %r returned resultType=%r, expected 'matrix' — a "
                "range query needs an instant-vector expression"
                % (q["name"], result_type)
            )

        override = q.get("metric_name")
        for series in data.get("result") or []:
            labels = dict(series.get("metric") or {})
            # __name__ always comes out of the label set: it is metric_name's
            # own column in metrics_raw, exactly as the receiver stores it.
            series_name = labels.pop("__name__", None)
            # Expressions like rate(...) / sum(...) drop __name__ entirely, so
            # an explicit override is the only way to name those; the query
            # name is the last resort so samples are never stored unnamed.
            metric_name = override or series_name or q["name"]
            for point in series.get("values") or []:
                try:
                    ts_s, raw = point[0], point[1]
                    value = float(raw)
                except (TypeError, ValueError, IndexError):
                    continue
                # NaN/±Inf are real Prometheus values (a gauge with no data, a
                # division by zero) but DOUBLE PRECISION can store them and
                # they poison avg()/sum() in the continuous aggregates, so they
                # are dropped rather than persisted.
                if not math.isfinite(value):
                    continue
                yield Row(
                    ts=datetime.fromtimestamp(ts_s, tz=UTC),
                    metric_name=metric_name,
                    labels=labels,
                    value=value,
                )
