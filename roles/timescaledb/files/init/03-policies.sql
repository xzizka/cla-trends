-- Continuous aggregate refresh policies: keep each rollup tier up to date
-- automatically in the background.
-- if_not_exists everywhere in this file: it is re-applied on every deploy
-- (see roles/timescaledb/tasks/main.yml), not just on first volume init.
SELECT add_continuous_aggregate_policy('metrics_1h',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval  => INTERVAL '1 hour',
    if_not_exists      => true
);

SELECT add_continuous_aggregate_policy('metrics_4h',
    start_offset      => INTERVAL '10 days',
    end_offset        => INTERVAL '4 hours',
    schedule_interval  => INTERVAL '4 hours',
    if_not_exists      => true
);

-- Retention: drop raw chunks after 7 days (tier 1 -> tier 2 handoff),
-- drop 1h chunks after 90 days / ~3 months (tier 2 -> tier 3 handoff).
-- The 4h tier has no retention policy and is kept indefinitely; add one
-- later with add_retention_policy('metrics_4h', INTERVAL '...') if needed.
SELECT add_retention_policy('metrics_raw', INTERVAL '7 days', if_not_exists => true);
SELECT add_retention_policy('metrics_1h', INTERVAL '90 days', if_not_exists => true);

-- Compression: raw chunks older than 1 day get columnar-compressed (segmented
-- by metric_name) to keep disk usage down on this small volume. The 1h
-- aggregate is also compressed once chunks age past its own refresh window.
SELECT add_compression_policy('metrics_raw', INTERVAL '1 day', if_not_exists => true);

ALTER MATERIALIZED VIEW metrics_1h SET (
    timescaledb.compress = true,
    timescaledb.compress_segmentby = 'metric_name',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('metrics_1h', INTERVAL '30 days', if_not_exists => true);

-- Prime both continuous aggregates so they aren't empty until their first
-- scheduled refresh runs.
CALL refresh_continuous_aggregate('metrics_1h', NULL, NULL);
CALL refresh_continuous_aggregate('metrics_4h', NULL, NULL);
