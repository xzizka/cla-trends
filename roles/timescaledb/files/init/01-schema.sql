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
