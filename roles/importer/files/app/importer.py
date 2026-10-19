"""Active metrics importer — the pull counterpart to the remote_write receiver.

Where the receiver sits and waits for a Prometheus to push into it, this
service reaches out to configured endpoints on a schedule and pulls whatever
appeared since it last looked. Both write into the same `metrics_raw`
hypertable, so the rollup tiers, retention policies and Grafana dashboards
are shared and unchanged.

Shape of the thing:

    sources.yml ──► config.load() ──► one Job per source
                                          │
                       one thread per Job │ (sources are independent: a dead
                                          │  Influx must not delay Prometheus)
                                          ▼
              for each stream: watermark ──► plan_windows() ──► source.fetch()
                                                                    │
                                          rows + new watermark, one transaction
                                                                    ▼
                                                              metrics_raw
                                                            importer_state

Correctness rests on three things, in order of importance:

  1. Windows are half-open `[start, end)` everywhere, and the watermark is the
     exclusive upper bound. The sample at exactly `end` belongs to the next
     window. This is what keeps re-runs from double-inserting into a table
     that has no unique constraint to catch it.
  2. Rows and watermark commit together (`store.commit_window`).
  3. A failure never advances the watermark, so the window is retried; and a
     legitimately empty window does advance it, so it is not retried forever.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import config
from sources.base import SourceError
from store import Store
from timespec import UTC, plan_windows

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("importer")

CONFIG_PATH = os.environ.get("IMPORTER_CONFIG", "/etc/importer/sources.yml")
STATUS_PORT = int(os.environ.get("IMPORTER_STATUS_PORT", "9202"))
PG_DSN = os.environ["IMPORTER_PG_DSN"]

#: Runaway guard on a single window/transaction. Rows are streamed into COPY
#: so this is not a memory limit — it is there so a misjudged `step` against a
#: huge Prometheus fails loudly instead of committing an enormous transaction.
#: Legitimately large windows exist (a broad query at 60s over 9000 points and
#: a few hundred series is millions of samples), hence the generous default;
#: override with IMPORTER_MAX_ROWS_PER_WINDOW, or reduce max_span /
#: max_points_per_request for that query.
MAX_ROWS_PER_WINDOW = int(os.environ.get("IMPORTER_MAX_ROWS_PER_WINDOW", "5000000"))

_shutdown = threading.Event()


class Status(object):
    """Thread-safe snapshot of what every stream last did, for /status."""

    def __init__(self):
        self._lock = threading.Lock()
        self._streams = {}
        self._sources = {}

    def source(self, name, **fields):
        with self._lock:
            self._sources.setdefault(name, {}).update(fields)

    def stream(self, source, key, **fields):
        with self._lock:
            self._streams.setdefault("%s/%s" % (source, key), {}).update(fields)

    def snapshot(self):
        with self._lock:
            return {
                "sources": json.loads(json.dumps(self._sources, default=str)),
                "streams": json.loads(json.dumps(self._streams, default=str)),
            }


STATUS = Status()


def run_stream(job, stream, store, now):
    """Import every outstanding window for one stream. Returns rows written."""
    key = stream.key
    last_ts = store.get_watermark(job.name, key)
    windows = plan_windows(
        last_ts=last_ts,
        now=now,
        max_lookback=job.max_lookback,
        end_lag=job.end_lag,
        max_span=stream.max_span,
        step=stream.step or None,
    )

    if not windows:
        STATUS.stream(job.name, key, last_result="up-to-date",
                      last_run=now, watermark=last_ts)
        log.debug("source %s stream %s: nothing to do", job.name, key)
        return 0

    # The only situation in which the importer knowingly skips data: the
    # watermark is older than max_lookback, so whatever sits in between is
    # never requested. Say so explicitly — silently narrowing the window is
    # how a monitoring gap goes unnoticed.
    if last_ts is not None and windows[0][0] > last_ts:
        log.warning(
            "source %s stream %s: watermark %s is older than max_lookback, "
            "skipping to %s — %s of data will not be imported",
            job.name, key, last_ts.isoformat(), windows[0][0].isoformat(),
            windows[0][0] - last_ts,
        )
    elif last_ts is None:
        log.info("source %s stream %s: first run, bootstrapping from %s",
                 job.name, key, windows[0][0].isoformat())

    total = 0
    done = 0
    for start, end in windows:
        if _shutdown.is_set():
            log.info("source %s stream %s: shutting down, stopping after %d rows",
                     job.name, key, total)
            break
        total += _run_window(job, stream, store, start, end)
        done += 1

    # Report a shutdown mid-backfill as such rather than as "ok": the
    # watermark is honest either way, but /status is what an operator reads.
    STATUS.stream(job.name, key,
                  last_result="ok" if done == len(windows) else "interrupted",
                  last_run=now, last_rows=total,
                  windows="%d/%d" % (done, len(windows)),
                  watermark=store.get_watermark(job.name, key))
    return total


def _prepare_rows(job, stream, start, end):
    """Filter the plugin's output to the window and stamp provenance on it.

    A generator, not a list: it is handed straight to the COPY stream, so a
    window of millions of samples costs constant memory. Raising from in here
    aborts the COPY and rolls the transaction back, which is the behaviour we
    want — the watermark stays put and the window is retried.
    """
    provenance = dict(job.extra_labels)
    provenance[config.SOURCE_LABEL] = job.name

    seen = 0
    for row in job.source.fetch(stream, start, end):
        # Trust the window, not the plugin: a source that returns a wider
        # range than asked (an unaligned GROUP BY, a `range()` the query
        # widened itself) would otherwise write samples the watermark is
        # about to declare already-imported, i.e. permanent duplicates.
        if row.ts < start or row.ts >= end:
            continue
        seen += 1
        if seen > MAX_ROWS_PER_WINDOW:
            raise SourceError(
                "window %s..%s returned more than %d samples — lower the "
                "window size (max_span / max_points_per_request) or make the "
                "query more selective"
                % (start.isoformat(), end.isoformat(), MAX_ROWS_PER_WINDOW)
            )
        labels = dict(row.labels)
        # Applied after the plugin, not before, so a source cannot forge the
        # `source` label and pass its data off as another source's.
        labels.update(provenance)
        yield row._replace(labels=labels)


def _run_window(job, stream, store, start, end):
    written = store.commit_window(
        job.name, stream.key, _prepare_rows(job, stream, start, end), end)
    log.info("source %s stream %s: %s..%s -> %d samples",
             job.name, stream.key, start.isoformat(), end.isoformat(), written)
    return written


def run_job(job, store):
    """One tick of one source: every stream, sequentially.

    Streams within a source share its endpoint, so running them one after
    another keeps the load on that endpoint predictable; different sources
    run in parallel because they are unrelated.
    """
    now = datetime.now(UTC)
    total = 0
    failed = []
    for stream in job.streams:
        if _shutdown.is_set():
            break
        try:
            total += run_stream(job, stream, store, now)
        except SourceError as exc:
            # Retryable: watermark untouched, so the same window is attempted
            # again on the next tick. One bad query must not stop the others.
            failed.append(stream.key)
            log.error("source %s stream %s: %s", job.name, stream.key, exc)
            STATUS.stream(job.name, stream.key, last_result="error",
                          last_run=now, last_error=str(exc))
        except Exception as exc:  # noqa: BLE001 - a thread must not die silently
            failed.append(stream.key)
            log.exception("source %s stream %s: unexpected failure",
                          job.name, stream.key)
            STATUS.stream(job.name, stream.key, last_result="error",
                          last_run=now, last_error=repr(exc))

    STATUS.source(job.name, last_run=now, last_rows=total,
                  failed_streams=failed,
                  last_result="error" if failed else "ok")
    return total


def job_loop(job, store):
    log.info("source %s (%s): %d stream(s), %s",
             job.name, job.source.type_name, len(job.streams),
             job.schedule.describe())

    if job.run_on_start:
        run_job(job, store)

    while not _shutdown.is_set():
        now = datetime.now(UTC)
        nxt = job.schedule.next_after(now)
        STATUS.source(job.name, next_run=nxt)
        wait = (nxt - now).total_seconds()
        if wait > 0:
            # Event-based sleep so SIGTERM does not have to wait out a
            # 24h interval before the container can stop.
            if _shutdown.wait(wait):
                break
        run_job(job, store)

    log.info("source %s: loop stopped", job.name)


class StatusHandler(BaseHTTPRequestHandler):
    server_version = "metrics-importer/1.0"

    def log_message(self, fmt, *args):
        log.debug("%s - %s", self.address_string(), fmt % args)

    def do_GET(self):
        if self.path == "/healthz":
            self._send(200, b"ok", "text/plain")
        elif self.path == "/status":
            self._send(200,
                       json.dumps(STATUS.snapshot(), indent=2,
                                  default=str).encode("utf-8"),
                       "application/json")
        else:
            self._send(404, b"not found", "text/plain")

    def _send(self, code, body, content_type):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: _shutdown.set())

    try:
        jobs = config.load(CONFIG_PATH)
    except ValueError as exc:
        # Configuration errors are not retryable, and a restart loop would
        # bury the message. Fail once, loudly, with the reason.
        log.error("invalid configuration in %s: %s", CONFIG_PATH, exc)
        return 2

    status_server = ThreadingHTTPServer(("0.0.0.0", STATUS_PORT), StatusHandler)
    threading.Thread(target=status_server.serve_forever,
                     name="status", daemon=True).start()
    log.info("status endpoint on :%d (/healthz, /status)", STATUS_PORT)

    if not jobs:
        # Not an error: an empty `importer_sources` is the default state of a
        # fresh deploy. Stay up and healthy so the container is not in a
        # restart loop while someone configures the first source.
        log.warning("no enabled sources configured in %s — idling. Add entries "
                    "to importer_sources in group_vars/all/vars.yml and "
                    "re-run the playbook.", CONFIG_PATH)
        _shutdown.wait()
        return 0

    # One pool shared by all job threads; sized to cover them plus the
    # status endpoint's occasional read.
    store = Store(PG_DSN, max_size=min(len(jobs) + 2, 10))
    store.open()

    threads = []
    for job in jobs:
        t = threading.Thread(target=job_loop, args=(job, store),
                             name="src-" + job.name, daemon=True)
        t.start()
        threads.append(t)

    _shutdown.wait()
    log.info("shutting down, waiting for in-flight windows to finish")
    for t in threads:
        t.join(timeout=60)
    store.close()
    status_server.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
