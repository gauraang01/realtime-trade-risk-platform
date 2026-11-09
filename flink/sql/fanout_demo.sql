-- Replays every tick against every live trade and prints the changelog,
-- to measure how many output rows the price join produces.
-- Bounded: it stops at the current end of both topics and frees its slot.

SET 'pipeline.name' = 'fanout_demo';
SET 'parallelism.default' = '1';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE TABLE trades_enriched (
  trade_id STRING,
  version INT,
  status STRING,
  instrument_id STRING,
  instrument_name STRING,
  daily_vol DECIMAL(6,4),
  counterparty_id STRING,
  counterparty_name STRING,
  credit_limit DECIMAL(18,2),
  book_id STRING,
  book_name STRING,
  side STRING,
  qty INT,
  price DECIMAL(18,2),
  trade_time BIGINT,
  event_time BIGINT,
  PRIMARY KEY (trade_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'trades_enriched',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'value.format' = 'json',
  'scan.bounded.mode' = 'latest-offset'
);

-- The same `prices` topic, read as "the current price per instrument".
CREATE TABLE prices_latest (
  instrument_id STRING,
  price DECIMAL(18,2),
  event_time BIGINT,
  PRIMARY KEY (instrument_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'prices',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'value.format' = 'json',
  'scan.bounded.mode' = 'latest-offset'
);

-- LEFT JOIN, so a trade booked before its first tick still appears, with no P&L yet.
SELECT t.trade_id, t.counterparty_id, t.book_id, t.instrument_id, t.side, t.qty,
       t.price AS trade_price,
       p.price AS live_price,
       CASE WHEN t.side = 'BUY' THEN t.qty ELSE -t.qty END * (p.price - t.price) AS mtm
FROM trades_enriched t
LEFT JOIN prices_latest p ON t.instrument_id = p.instrument_id
WHERE t.status <> 'CANCELLED';
