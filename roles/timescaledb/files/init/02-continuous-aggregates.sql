-- Tier 2: 1-hour rollup, built directly from raw samples.
-- Real-time aggregation stays ON here so Grafana always sees the latest
-- (not-yet-materialized) bucket blended in when querying this view directly.
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    metric_name,
    labels,
    avg(value)   AS avg_value,
    min(value)   AS min_value,
    max(value)   AS max_value,
    last(value, time) AS last_value,
    count(*)     AS sample_count
FROM metrics_raw
GROUP BY bucket, metric_name, labels
WITH NO DATA;

-- Tier 3: 4-hour rollup, built on top of the 1h aggregate (hierarchical
-- continuous aggregate). TimescaleDB requires real-time aggregation to be
-- disabled on a continuous aggregate that is itself built on another one,
-- hence materialized_only = true here.
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_4h
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT
    time_bucket('4 hours', bucket) AS bucket,
    metric_name,
    labels,
    -- weighted average across the four 1h buckets that roll up into this one
    sum(avg_value * sample_count) / NULLIF(sum(sample_count), 0) AS avg_value,
    min(min_value) AS min_value,
    max(max_value) AS max_value,
    last(last_value, bucket) AS last_value,
    sum(sample_count) AS sample_count
FROM metrics_1h
GROUP BY time_bucket('4 hours', bucket), metric_name, labels
WITH NO DATA;
