CREATE DATABASE IF NOT EXISTS cinema;

CREATE TABLE IF NOT EXISTS cinema.kafka_events
(
    event_id String,
    event_time String,
    user_id Int64,
    event_type String,
    content_id Nullable(Int64),
    watch_time_seconds Nullable(Int64),
    query Nullable(String),
    available_at String
)
ENGINE = Kafka SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'cinema.events',
    kafka_group_name = 'cinema_clickhouse',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS cinema.live_views
(
    event_id String,
    event_time DateTime('UTC'),
    content_id Int64,
    watch_time_seconds Int64,
    loaded_at DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_id, event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.live_mv TO cinema.live_views AS
SELECT event_id, parseDateTimeBestEffort(event_time, 'UTC') AS event_time, assumeNotNull(content_id) AS content_id,
       assumeNotNull(watch_time_seconds) AS watch_time_seconds
FROM cinema.kafka_events
WHERE event_type = 'view' AND content_id IS NOT NULL AND watch_time_seconds IS NOT NULL;

CREATE VIEW IF NOT EXISTS cinema.live_minute AS
SELECT toStartOfMinute(event_time) AS minute, content_id,
       count() AS views, sum(watch_time_seconds) AS watch_time_seconds
FROM
(
    SELECT event_id, any(event_time) AS event_time, any(content_id) AS content_id,
           any(watch_time_seconds) AS watch_time_seconds
    FROM cinema.live_views GROUP BY event_id
)
GROUP BY minute, content_id;

CREATE TABLE IF NOT EXISTS cinema.batch_minute
(
    minute DateTime('UTC'), content_id Int64, views UInt64, watch_time_seconds Int64,
    run_id String, interval_start DateTime('UTC'), interval_end DateTime('UTC'),
    processed_at DateTime('UTC'), publication_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDate(minute))
ORDER BY (publication_id, minute, content_id);

CREATE TABLE IF NOT EXISTS cinema.batch_country
(
    day Date, country String, subscription_tier String, views UInt64, watch_time_seconds Int64,
    run_id String, interval_start DateTime('UTC'), interval_end DateTime('UTC'),
    processed_at DateTime('UTC'), publication_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (publication_id, day, country, subscription_tier);

CREATE TABLE IF NOT EXISTS cinema.batch_top
(
    day Date, content_id Int64, views UInt64, rank UInt32,
    run_id String, interval_start DateTime('UTC'), interval_end DateTime('UTC'),
    processed_at DateTime('UTC'), publication_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (publication_id, day, rank);

CREATE TABLE IF NOT EXISTS cinema.batch_words
(
    day Date, word String, frequency UInt64,
    run_id String, interval_start DateTime('UTC'), interval_end DateTime('UTC'),
    processed_at DateTime('UTC'), publication_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (publication_id, day, word);

CREATE TABLE IF NOT EXISTS cinema.publications
(
    publication_id String, run_id String,
    interval_start DateTime('UTC'), interval_end DateTime('UTC'),
    version UInt64, processed_at DateTime('UTC')
)
ENGINE = MergeTree ORDER BY version;

CREATE VIEW IF NOT EXISTS cinema.cutoff AS
SELECT if(count() = 0, toDateTime('2026-01-01 00:00:00', 'UTC'), max(interval_end)) AS T
FROM cinema.publications;

CREATE VIEW IF NOT EXISTS cinema.active_days AS
SELECT day, argMax(publication_id, version) AS publication_id
FROM
(
    SELECT publication_id, version,
           addDays(toDate(interval_start), arrayJoin(range(toUInt32(dateDiff('day', interval_start, interval_end))))) AS day
    FROM cinema.publications
)
GROUP BY day;

CREATE VIEW IF NOT EXISTS cinema.report_minute AS
SELECT b.* FROM cinema.batch_minute AS b
INNER JOIN cinema.active_days AS a ON toDate(b.minute) = a.day AND b.publication_id = a.publication_id;

CREATE VIEW IF NOT EXISTS cinema.report_country AS
SELECT b.* FROM cinema.batch_country AS b
INNER JOIN cinema.active_days AS a ON b.day = a.day AND b.publication_id = a.publication_id;

CREATE VIEW IF NOT EXISTS cinema.report_top AS
SELECT b.* FROM cinema.batch_top AS b
INNER JOIN cinema.active_days AS a ON b.day = a.day AND b.publication_id = a.publication_id;

CREATE VIEW IF NOT EXISTS cinema.report_words AS
SELECT b.* FROM cinema.batch_words AS b
INNER JOIN cinema.active_days AS a ON b.day = a.day AND b.publication_id = a.publication_id;

CREATE VIEW IF NOT EXISTS cinema.report_all AS
SELECT b.minute AS minute, b.content_id AS content_id, b.views AS views,
       b.watch_time_seconds AS watch_time_seconds, b.layer AS layer
FROM
(
    SELECT minute, content_id, views, watch_time_seconds, publication_id, 'batch' AS layer
    FROM cinema.batch_minute
    UNION ALL
    SELECT minute, content_id, views, watch_time_seconds, '' AS publication_id, 'live' AS layer
    FROM cinema.live_minute
    WHERE minute >= toDateTime('2026-01-01 00:00:00', 'UTC')
) AS b
LEFT JOIN cinema.active_days AS a ON toDate(b.minute) = a.day
WHERE (b.layer = 'batch' AND b.publication_id = a.publication_id)
   OR (b.layer = 'live' AND coalesce(a.publication_id, '') = '');
