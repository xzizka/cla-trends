CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Raw ingestion table for Prometheus remote_write samples.
-- One row per (time, metric_name, label set). Labels are stored as jsonb
-- without __name__ (which lives in its own column for fast filtering/indexing).
CREATE TABLE IF NOT EXISTS metrics_raw (
    time        TIMESTAMPTZ       NOT NULL,
    metric_name TEXT              NOT NULL,
    labels      JSONB             NOT NULL DEFAULT '{}'::jsonb,
    value       DOUBLE PRECISION  NOT NULL
);

SELECT create_hypertable(
    'metrics_raw', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS ix_metrics_raw_name_time
    ON metrics_raw (metric_name, time DESC);

CREATE INDEX IF NOT EXISTS ix_metrics_raw_labels_gin
    ON metrics_raw USING GIN (labels);

-- Native columnar compression for the raw tier: segment by metric_name so
-- same-series values stay adjacent, order by time for good compression ratio.
ALTER TABLE metrics_raw SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'metric_name',
    timescaledb.compress_orderby = 'time DESC'
);

-- ---------------------------------------------------------------------------
-- Importer bookkeeping (roles/importer). Not metrics data — one row per
-- (source, stream) telling the importer where it got to, so a restart or an
-- outage resumes exactly where it left off instead of re-importing (metrics_raw
-- has no unique constraint, so a duplicate import would silently double-count)
-- or skipping a window.
--
-- Contract: last_ts is an EXCLUSIVE upper bound — "everything with
-- time < last_ts has been imported for this stream". The importer only ever
-- writes samples in [start, end) and moves last_ts to `end` in the SAME
-- transaction as the inserts, so the table can never disagree with the data.
--
-- Deliberately a plain table, not a hypertable: it is tiny (one row per
-- configured query) and is updated in place, never appended to.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS importer_state (
    source_name TEXT        NOT NULL,
    stream_key  TEXT        NOT NULL,
    last_ts     TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_rows   BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (source_name, stream_key)
);
