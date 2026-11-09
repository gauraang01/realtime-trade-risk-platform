-- Blotter: the latest row per trade. No history table, because trade_view is too
-- busy to keep every row (thousands of messages a second).

-- kafka_offset, not the trade's version: MTM changes every second while version stays the same.
CREATE TABLE serving.trade_current (
  trade_id String,
  version Int32,
  status String,
  instrument_id String,
  instrument_name Nullable(String),
  counterparty_id String,
  counterparty_name Nullable(String),
  book_id String,
  book_name Nullable(String),
  side String,
  qty Int32,
  trade_price Decimal(18,2),
  live_price Decimal(18,2),
  mtm Decimal(38,2),
  risk Nullable(Decimal(18,2)),
  risk_as_of Nullable(Int64),
  risk_trade_version Nullable(Int32),
  risk_stale Bool,
  risk_silent Bool,
  kafka_offset UInt64,
  kafka_time DateTime64(3)
) ENGINE = ReplacingMergeTree(kafka_offset)
ORDER BY trade_id;

-- ClickHouse skips tombstones, which is what we want: closed trades arrive as
-- CANCELLED rows, so a tombstone here is only half of a price update.
CREATE TABLE serving.trade_view_kafka (
  trade_id String,
  version Int32,
  status String,
  instrument_id String,
  instrument_name Nullable(String),
  counterparty_id String,
  counterparty_name Nullable(String),
  book_id String,
  book_name Nullable(String),
  side String,
  qty Int32,
  trade_price Decimal(18,2),
  live_price Decimal(18,2),
  mtm Decimal(38,2),
  risk Nullable(Decimal(18,2)),
  risk_as_of Nullable(Int64),
  risk_trade_version Nullable(Int32),
  risk_stale Bool,
  risk_silent Bool
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'trade_view',
         kafka_group_name = 'clickhouse-trade-view',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW serving.trade_current_mv TO serving.trade_current AS
SELECT *, _offset AS kafka_offset, _timestamp_ms AS kafka_time
FROM serving.trade_view_kafka;
