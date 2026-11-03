CREATE DATABASE IF NOT EXISTS logs;

CREATE TABLE logs.events (
    ts DateTime64(3),
    service LowCardinality(String),
    level Enum8('TRACE'=0, 'DEBUG'=1, 'INFO'=2, 'WARN'=3, 'ERROR'=4, 'FATAL'=5),
    template_id UInt32,
    seq UInt64,
    message String CODEC(ZSTD(3))
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (service, ts)
TTL toDateTime(ts) + INTERVAL 7 DAY;

CREATE TABLE logs.templates (
    template_id UInt32,
    service LowCardinality(String),
    template String,
    first_seen DateTime
) ENGINE = ReplacingMergeTree
ORDER BY template_id;

CREATE MATERIALIZED VIEW logs.errors_per_minute_mv
ENGINE = SummingMergeTree
ORDER BY (service, template_id, minute)
AS
SELECT
    service,
    template_id,
    toStartOfMinute(ts) AS minute,
    count() AS errors
FROM logs.events
WHERE level IN ('ERROR', 'FATAL')
GROUP BY service, template_id, minute;
