-- Counterparty panel: the latest row per fund, plus every row for point-in-time questions.
--
-- Create order matters. A Kafka table starts consuming as soon as a materialized view
-- is attached to it, so the destinations must exist first.

-- Latest row per fund. Merges keep the row with the highest kafka_offset.
-- All rows for a fund land on one partition, so a higher offset always means newer.
CREATE TABLE serving.counterparty_current (
  counterparty_id String,
  counterparty_name Nullable(String),
  exposure Decimal(38,2),
  credit_limit Nullable(Decimal(18,2)),
  breached Nullable(Bool),
  kafka_offset UInt64,
  kafka_time DateTime64(3)
) ENGINE = ReplacingMergeTree(kafka_offset)
ORDER BY counterparty_id;

-- Every row ever received. Old values stay, so "exposure at 14:05" can be answered.
CREATE TABLE serving.counterparty_history (
  counterparty_id String,
  counterparty_name Nullable(String),
  exposure Decimal(38,2),
  credit_limit Nullable(Decimal(18,2)),
  breached Nullable(Bool),
  kafka_time DateTime64(3)
) ENGINE = MergeTree
ORDER BY (counterparty_id, kafka_time)
TTL toDateTime(kafka_time) + INTERVAL 7 DAY;

-- Holds no data. It is ClickHouse's Kafka consumer, shaped like the topic's JSON.
CREATE TABLE serving.counterparty_view_kafka (
  counterparty_id String,
  counterparty_name Nullable(String),
  exposure Decimal(38,2),
  credit_limit Nullable(Decimal(18,2)),
  breached Nullable(Bool)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'counterparty_view',
         kafka_group_name = 'clickhouse-counterparty-view',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

-- History first: if the first batch reaches only one view, it must be the one that keeps everything.
CREATE MATERIALIZED VIEW serving.counterparty_history_mv TO serving.counterparty_history AS
SELECT *, _timestamp_ms AS kafka_time
FROM serving.counterparty_view_kafka;

CREATE MATERIALIZED VIEW serving.counterparty_current_mv TO serving.counterparty_current AS
SELECT *, _offset AS kafka_offset, _timestamp_ms AS kafka_time
FROM serving.counterparty_view_kafka;
