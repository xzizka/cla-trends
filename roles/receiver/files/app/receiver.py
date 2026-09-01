"""Minimal Prometheus remote_write (v1, protobuf) receiver.

Decodes the WriteRequest wire format by hand (no `protobuf` package
dependency, no protoc codegen step) and batch-inserts samples into the
TimescaleDB `metrics_raw` hypertable. Deliberately narrow in scope:

  * Supports classic float Samples only (labels + samples) — this covers
    counters/gauges/histograms/summaries as Prometheus has always sent them
    over remote_write. Exemplars and native-histogram fields are present on
    the wire but are skipped (their bytes are read and discarded).
  * No retries/queueing beyond what the sender does — a non-2xx response
    tells Prometheus's remote_write queue to retry the batch.

Wire format (matches github.com/prometheus/prometheus/prompb, stable for
years): WriteRequest{timeseries=1}, TimeSeries{labels=1, samples=2},
Label{name=1, value=2}, Sample{value=1 (fixed64 double), timestamp=2 (varint,
ms since epoch)}.
"""
from __future__ import annotations

import logging
import os
import struct
import sys
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import cramjam
import psycopg
from psycopg_pool import ConnectionPool

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("receiver")

BEARER_TOKEN = os.environ["RECEIVER_BEARER_TOKEN"]
LISTEN_PORT = int(os.environ.get("RECEIVER_PORT", "9201"))
PG_DSN = os.environ["RECEIVER_PG_DSN"]

INSERT_SQL = "INSERT INTO metrics_raw (time, metric_name, labels, value) VALUES (%s, %s, %s, %s)"

pool = ConnectionPool(PG_DSN, min_size=1, max_size=8, kwargs={"autocommit": True})


# ---------------------------------------------------------------------------
# Minimal protobuf wire-format decoder (varint / fixed64 / length-delimited)
# ---------------------------------------------------------------------------

def _read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7


def _iter_fields(buf: bytes):
    """Yield (field_number, wire_type, value) for one embedded message."""
    pos = 0
    n = len(buf)
    while pos < n:
        tag, pos = _read_varint(buf, pos)
        field_no, wire_type = tag >> 3, tag & 0x7
        if wire_type == 0:  # varint
            val, pos = _read_varint(buf, pos)
        elif wire_type == 1:  # fixed64
            val = struct.unpack_from("<Q", buf, pos)[0]
            pos += 8
        elif wire_type == 2:  # length-delimited (string/bytes/embedded msg)
            length, pos = _read_varint(buf, pos)
            val = buf[pos:pos + length]
            pos += length
        elif wire_type == 5:  # fixed32
            val = struct.unpack_from("<I", buf, pos)[0]
            pos += 4
        else:
            raise ValueError(f"unsupported protobuf wire type {wire_type}")
        yield field_no, wire_type, val


def _parse_label(buf: bytes) -> tuple[str, str]:
    name = value = ""
    for field_no, _wt, val in _iter_fields(buf):
        if field_no == 1:
            name = val.decode("utf-8")
        elif field_no == 2:
            value = val.decode("utf-8")
    return name, value


def _parse_sample(buf: bytes) -> tuple[int | None, float | None]:
    value = ts_ms = None
    for field_no, _wt, val in _iter_fields(buf):
        if field_no == 1:  # double, carried as fixed64 bit pattern
            value = struct.unpack("<d", struct.pack("<Q", val))[0]
        elif field_no == 2:  # int64 ms, carried as varint
            ts_ms = val
    return ts_ms, value


def _parse_timeseries(buf: bytes):
    labels: dict[str, str] = {}
    samples: list[tuple[int, float]] = []
    for field_no, _wt, val in _iter_fields(buf):
        if field_no == 1:
            name, value = _parse_label(val)
            labels[name] = value
        elif field_no == 2:
            ts_ms, value = _parse_sample(val)
            if ts_ms is not None and value is not None:
                samples.append((ts_ms, value))
        # field 3 = exemplars, field 4 = native histograms — intentionally skipped
    return labels, samples


def parse_write_request(buf: bytes):
    """Return a flat list of (time_ms, metric_name, labels_dict, value) rows."""
    rows = []
    for field_no, _wt, val in _iter_fields(buf):
        if field_no != 1:
            continue
        labels, samples = _parse_timeseries(val)
        metric_name = labels.pop("__name__", "")
        if not metric_name or not samples:
            continue
        for ts_ms, value in samples:
            rows.append((ts_ms, metric_name, labels, value))
    return rows


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    server_version = "metrics-remote-write-receiver/1.0"

    def log_message(self, fmt, *args):  # route through logging, not stderr
        log.info("%s - %s", self.address_string(), fmt % args)

    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path != "/api/v1/write":
            self.send_response(404)
            self.end_headers()
            return

        auth = self.headers.get("Authorization", "")
        if auth != f"Bearer {BEARER_TOKEN}":
            self.send_response(401)
            self.end_headers()
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            compressed = self.rfile.read(length)
            raw = bytes(cramjam.snappy.decompress_raw(compressed))
            rows = parse_write_request(raw)
        except Exception:
            log.exception("failed to decode remote_write payload")
            self.send_response(400)
            self.end_headers()
            return

        if rows:
            try:
                self._insert(rows)
            except Exception:
                log.exception("failed to insert %d samples", len(rows))
                self.send_response(500)
                self.end_headers()
                return

        self.send_response(204)
        self.end_headers()

    def _insert(self, rows):
        params = [
            (
                datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc),
                metric_name,
                psycopg.types.json.Json(labels),
                value,
            )
            for ts_ms, metric_name, labels, value in rows
        ]
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(INSERT_SQL, params)
        log.info("inserted %d samples", len(params))


def main():
    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    log.info("listening on :%d", LISTEN_PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()
