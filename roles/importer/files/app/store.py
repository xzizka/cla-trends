"""TimescaleDB write path and watermark bookkeeping.

The one invariant this module exists to guarantee:

    the samples for a window and the watermark move in a SINGLE transaction

`metrics_raw` has no unique constraint (by design — it is an append-only
hypertable fed by the remote_write receiver at high rate, where a unique
index would be pure overhead). That means the database will happily accept
the same sample twice, so nothing downstream can clean up after a partial
import. Committing rows and watermark together is what makes the importer
safe to kill, restart, or crash mid-backfill: either the whole window landed
and the watermark moved, or neither did and the window is simply retried.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Iterable, Optional

import psycopg
from psycopg.types.json import Json
from psycopg_pool import ConnectionPool

from sources.base import Row

log = logging.getLogger("importer.store")

_COPY_SQL = "COPY metrics_raw (time, metric_name, labels, value) FROM STDIN"
# Declared explicitly so psycopg does not have to infer a type per value.
# NOTE the labels column: the value handed to write_row for it must be a
# `Json(dict)` wrapper, NOT a pre-serialised str. Passing an already-dumped
# string here makes psycopg's jsonb dumper encode it a *second* time, and the
# column ends up holding a JSON string like "{\"host\":\"a\"}" instead of an
# object — jsonb_typeof() says 'string', `labels ? 'key'` matches nothing, and
# every label-based query silently returns no rows. Cost us a demo run
# (bug-021); the receiver uses the same Json() wrapper for the same reason.
_COPY_TYPES = ["timestamptz", "text", "jsonb", "float8"]

_GET_WATERMARK_SQL = (
    "SELECT last_ts FROM importer_state WHERE source_name = %s AND stream_key = %s"
)
_SET_WATERMARK_SQL = """
INSERT INTO importer_state (source_name, stream_key, last_ts, updated_at, last_rows)
VALUES (%s, %s, %s, now(), %s)
ON CONFLICT (source_name, stream_key) DO UPDATE
   SET last_ts    = EXCLUDED.last_ts,
       updated_at = EXCLUDED.updated_at,
       last_rows  = EXCLUDED.last_rows
"""


class Store(object):
    def __init__(self, dsn, max_size=4):
        # autocommit is deliberately OFF (the receiver uses it, we must not):
        # every window needs an explicit transaction spanning the COPY and the
        # watermark update.
        self.pool = ConnectionPool(dsn, min_size=1, max_size=max_size, open=False)

    def open(self, timeout=120.0):
        """Open the pool, tolerating a TimescaleDB that is still starting up.

        On a fresh `ansible-playbook` run the importer container can come up
        while TimescaleDB is still replaying WAL, and a hard failure here
        would just mean a restart loop with a confusing first error.
        """
        self.pool.open()
        deadline = time.time() + timeout
        last_err = None
        while time.time() < deadline:
            try:
                with self.pool.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1 FROM importer_state LIMIT 0")
                    conn.rollback()
                return
            except Exception as exc:  # noqa: BLE001 - retried below
                last_err = exc
                log.warning("database not ready yet (%s); retrying", exc)
                time.sleep(3)
        raise RuntimeError(
            "TimescaleDB / importer_state unreachable after %.0fs: %s. Is the "
            "timescaledb container healthy and has roles/timescaledb applied "
            "01-schema.sql?" % (timeout, last_err)
        )

    def close(self):
        try:
            self.pool.close()
        except Exception:  # pragma: no cover
            pass

    def get_watermark(self, source_name, stream_key):
        # type: (str, str) -> Optional[datetime]
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_GET_WATERMARK_SQL, (source_name, stream_key))
                row = cur.fetchone()
            conn.rollback()
        return row[0] if row else None

    def commit_window(self, source_name, stream_key, rows, watermark):
        # type: (str, str, Iterable[Row], datetime) -> int
        """Insert `rows` and move the watermark to `watermark`, atomically.

        `rows` is consumed lazily, so it can be a generator straight off the
        source plugin: samples go down the COPY stream as they are parsed and
        are never all held in memory at once. That matters because a single
        window is legitimately large — one broad PromQL query at 60s over
        9000 points across a few hundred series is millions of samples, which
        would be hundreds of MB buffered but is nothing as a stream.

        Returns the number of rows written. An empty window still advances
        the watermark — a source legitimately having no data in an hour must
        not make the importer re-request that hour forever. (A zero-row
        `COPY FROM STDIN` is valid, so this needs no special case.)

        If iterating `rows` raises, the COPY is aborted and the transaction
        rolls back, leaving the watermark where it was: the window is retried
        rather than half-imported.
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                written = 0
                with cur.copy(_COPY_SQL) as cp:
                    cp.set_types(_COPY_TYPES)
                    for row in rows:
                        cp.write_row(
                            (
                                row.ts,
                                row.metric_name,
                                Json(row.labels),
                                row.value,
                            )
                        )
                        written += 1
                cur.execute(
                    _SET_WATERMARK_SQL,
                    (source_name, stream_key, watermark, written),
                )
            conn.commit()
        return written

    def watermarks(self):
        # type: () -> list
        """All bookkeeping rows, for the /status endpoint."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT source_name, stream_key, last_ts, updated_at, last_rows"
                    " FROM importer_state ORDER BY source_name, stream_key"
                )
                out = cur.fetchall()
            conn.rollback()
        return out
