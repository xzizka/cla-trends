"""PostgreSQL source plugin (plain SQL over psycopg).

The only plugin that does not speak HTTP: the connection is libpq's, the
"query language" is SQL, and `auth` means database credentials rather than
an HTTP auth scheme. Everything else — schedule, watermark, half-open
windows, provenance labels, the write into `metrics_raw` — is exactly the
same as for the HTTP plugins, because that all lives in the runner.

Config (one entry of `sources:` in sources.yml):

    - name: pg-app
      type: postgres
      host: pg.example.internal
      port: 5432                       # default
      database: appdb
      sslmode: require                 # optional; libpq default is 'prefer'
      schedule: {every: 1h}
      auth: {type: basic, username: app_reader}
      credentials_id: pg_app           # -> IMPORTER_CRED_PG_APP_PASSWORD
      queries:
        - name: queue_depth
          time_column: ts              # default 'time'
          value_column: depth          # default 'value'
          label_columns: [queue, host] # default: every other column
          metric_name: app_queue_depth # default: the query name
          align: 1m                    # MUST match time_bucket('1m', ...) —
                                       # same reason as influxdb1's align
          query: |
            SELECT time_bucket('1 minute', created_at) AS ts,
                   queue, host, avg(depth) AS depth
              FROM queue_stats
             WHERE created_at >= $__start AND created_at < $__stop
             GROUP BY 1, 2, 3

`$__start` / `$__stop` are bound as psycopg *parameters* (`%(start)s` /
`%(stop)s`), never interpolated into the SQL text — the window cannot leak
into (or break) the statement, and the driver sends it typed as timestamptz.
Consequence: a literal `%` anywhere in the SQL (LIKE pattern, `mod()`) must
be written `%%`, exactly as everywhere else in psycopg. Both placeholders
are mandatory: without an upper bound the query would re-read everything
newer than the watermark on every tick.

The example above aggregates with `time_bucket()` because that is what the
rest of this stack (TimescaleDB) uses. A plain PostgreSQL server does not
have that function — use `date_trunc('minute', ts)` there and keep `align`
equal to the bucket width either way.

Time zone: the time column should be `timestamptz`. A plain `timestamp`
column is accepted and interpreted as UTC — the whole stack runs in UTC and
assuming the server's zone for such rows would be a silent shift.

Timeouts: `timeout_seconds` (per source, or `importer_defaults`) bounds the
connection attempt and, via `statement_timeout`, the statement itself — one
runaway SQL cannot wedge its stream forever; it fails, the watermark stays
put and the window is retried, like any other SourceError.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta
from typing import Iterator, List

import psycopg

from timespec import UTC, parse_duration

from . import register
from .base import Row, Source, SourceError, Stream, require

log = logging.getLogger("importer.postgres")

DEFAULT_PORT = 5432

#: Rows fetched per round trip from a server-side cursor. The runner streams
#: them into COPY as they arrive, so this bounds network batching, not memory.
_ITERSIZE = 10000


@register("postgres")
class PostgresSource(Source):

    #: No HTTP involved. config.py skips building an HttpClient for this
    #: source: its `auth` is libpq credentials and may legitimately carry a
    #: username without a password (peer/trust setups), which HttpClient
    #: would reject as an invalid basic-auth block.
    uses_http = False

    @staticmethod
    def validate(cfg):
        where = "source %r" % cfg.get("name", "?")
        require(cfg, "host", where)
        require(cfg, "database", where)
        if cfg.get("port") is not None:
            try:
                port = int(cfg["port"])
            except (TypeError, ValueError):
                port = -1
            if not 0 < port < 65536:
                raise ValueError("%s: port must be an integer in 1..65535, "
                                 "got %r" % (where, cfg["port"]))

        auth = cfg.get("auth") or {}
        kind = (auth.get("type") or "none").lower()
        if kind not in ("none", "basic"):
            raise ValueError(
                "%s: auth.type %r is not valid for a SQL source — use "
                "`basic` (username/password) or omit auth" % (where, kind))
        if kind == "basic" and not auth.get("username"):
            raise ValueError("%s: auth.type basic needs auth.username" % where)

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
            # An open-ended window still cannot duplicate (the runner drops
            # samples outside [start, end)), but the source pays for reading
            # them on every tick — refuse at startup, like the other plugins
            # refuse a hardcoded range.
            missing = [p for p in ("$__start", "$__stop") if p not in text]
            if missing:
                raise ValueError(
                    "%s: query must reference the import window via %s — "
                    "otherwise every run re-reads everything newer than the "
                    "watermark" % (qwhere,
                                   " and ".join("`%s`" % p for p in missing))
                )
            if "align" in q:
                parse_duration(q["align"], qwhere + ".align")

    def streams(self):
        # type: () -> List[Stream]
        # Same shape as influxdb1/influxdb2: `align` snaps window boundaries
        # onto the grid the SQL's own time_bucket() buckets on, `max_span`
        # bounds one request (here: one statement's result set).
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
        sql, params = _bind_window(q["query"], start, end)
        conn = self._connect()
        try:
            # Named (server-side) cursor: rows are fetched in batches as the
            # runner consumes them, so a wide window never lands in this
            # process's memory in full.
            with conn.cursor(name="metrics-importer") as cur:
                cur.itersize = _ITERSIZE
                cur.execute(sql, params)
                for row in self._rows(cur, q):
                    yield row
        except psycopg.Error as exc:
            # Connection failures land here too. SourceError is retryable:
            # the watermark stays put and the same window is attempted on the
            # next tick. The message carries no conninfo, so no credentials.
            raise SourceError("query %r failed on %s: %s"
                              % (q["name"], self.name, exc))
        finally:
            conn.close()

    def _connect(self):
        cfg = self.cfg
        auth = cfg.get("auth") or {}
        # config.py sets the resolved `timeout_seconds` attribute (defaults
        # merged); the cfg lookup is only the fallback for a source built
        # outside config.load, e.g. in tests.
        timeout = int(getattr(self, "timeout_seconds", None)
                      or cfg.get("timeout_seconds") or 60)
        params = {
            "host": cfg["host"],
            "port": int(cfg.get("port") or DEFAULT_PORT),
            "dbname": cfg["database"],
            "connect_timeout": max(1, timeout),
            "application_name": "metrics-importer/%s" % self.name,
            # A hung statement would otherwise hold the stream (and its
            # windows) indefinitely; turning it into an error keeps the
            # runner's "fail, don't advance, retry" contract intact.
            "options": "-c statement_timeout=%d" % (timeout * 1000),
        }
        if cfg.get("sslmode"):
            params["sslmode"] = cfg["sslmode"]
        if auth.get("username"):
            params["user"] = auth["username"]
        if auth.get("password"):
            params["password"] = auth["password"]
        return psycopg.connect(**params)

    def _rows(self, cur, q):
        """Map result rows onto Row tuples, raising on misconfiguration.

        A generator, like every other plugin's: rows flow straight into the
        COPY stream. Raising from in here aborts the COPY and rolls the
        window back, which is what we want — a query selecting a string as
        `value_column` should be retried after the config is fixed, not
        silently imported as nothing.
        """
        columns = [d.name for d in (cur.description or ())]
        if not columns:
            raise SourceError(
                "query %r returned no result columns — it must be a SELECT"
                % q["name"])

        time_col = q.get("time_column", "time")
        value_col = q.get("value_column", "value")
        try:
            ti = columns.index(time_col)
        except ValueError:
            raise SourceError(
                "query %r has no time column %r (columns: %s) — alias it, "
                "e.g. `SELECT ... AS %s`"
                % (q["name"], time_col, ", ".join(columns), time_col))
        try:
            vi = columns.index(value_col)
        except ValueError:
            raise SourceError(
                "query %r has no value column %r (columns: %s) — alias it "
                "or set `value_column`" % (q["name"], value_col,
                                           ", ".join(columns)))

        label_cols = q.get("label_columns")
        if label_cols:
            missing = [c for c in label_cols if c not in columns]
            if missing:
                raise SourceError(
                    "query %r: label_columns not in the result: %s "
                    "(columns: %s)"
                    % (q["name"], ", ".join(missing), ", ".join(columns)))
            label_idx = [(c, columns.index(c)) for c in label_cols]
        else:
            # Default: everything that is neither the timestamp nor the value
            # is a label — the natural shape of a metrics SQL result.
            label_idx = [(c, i) for i, c in enumerate(columns)
                         if i not in (ti, vi)]

        prefix = q.get("metric_prefix", self.cfg.get("metric_prefix", "")) or ""
        metric_name = prefix + (q.get("metric_name") or q["name"])

        for rec in cur:
            raw_ts = rec[ti]
            if raw_ts is None:
                continue
            if not isinstance(raw_ts, datetime):
                raise SourceError(
                    "time column %r of query %r returned %r (%s) — cast it "
                    "to timestamptz in the query"
                    % (time_col, q["name"], raw_ts, type(raw_ts).__name__))
            # A naive `timestamp` column is taken as UTC, by design (see the
            # module docstring): the whole stack is UTC, and guessing the
            # server's zone would shift rows silently.
            ts = raw_ts if raw_ts.tzinfo else raw_ts.replace(tzinfo=UTC)

            raw = rec[vi]
            if raw is None or isinstance(raw, bool):
                # NULL = no sample at that timestamp (e.g. a LEFT JOIN);
                # booleans have no representation in DOUBLE PRECISION.
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError):
                # Unlike influxdb1 (which auto-discovers value columns and so
                # skips quietly), value_column here is an explicit choice —
                # a string in it is a misconfiguration, not a log line.
                raise SourceError(
                    "value column %r of query %r is not numeric (%r: %r) — "
                    "pick a numeric column"
                    % (value_col, q["name"], type(raw).__name__, raw))
            if not math.isfinite(value):
                continue

            labels = {c: str(rec[i]) for c, i in label_idx
                      if rec[i] is not None}
            yield Row(ts=ts, metric_name=metric_name, labels=labels,
                      value=value)


def _bind_window(text, start, end):
    """Replace the window placeholders with psycopg bind parameters.

    Keys MUST match the `%(name)s` placeholders left in the SQL — psycopg
    raises "query parameter missing: stop" otherwise (the live demo caught
    exactly that: the dict was keyed start/end while the placeholder is stop).
    """
    return (text.replace("$__start", "%(start)s").replace("$__stop", "%(stop)s"),
            {"start": start, "stop": end})
