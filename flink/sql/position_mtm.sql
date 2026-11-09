-- Works out what the desk holds per book and instrument, values it at the latest
-- price, and publishes one current row per position to the compacted `position_mtm`.

SET 'pipeline.name' = 'position_mtm';
SET 'parallelism.default' = '1';

-- Flink only recovers from a crash if checkpoints are on.
SET 'execution.checkpointing.interval' = '10s';

-- Conflation: operators buffer input and process only the newest row per key.
-- This also merges an amend's "remove old" and "add new" into one change.
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '5000';

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
  'value.format' = 'json'
);

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
  'value.format' = 'json'
);

CREATE TABLE position_mtm (
  book_id STRING,
  instrument_id STRING,
  net_qty INT,
  cost DECIMAL(38,2),
  trades BIGINT,
  live_price DECIMAL(18,2),
  mtm DECIMAL(38,2),
  PRIMARY KEY (book_id, instrument_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'position_mtm',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.json.encode.decimal-as-plain-number' = 'true',
  'sink.buffer-flush.max-rows' = '5000',
  'sink.buffer-flush.interval' = '1s'
);

-- `cost` is what the position cost to build: negative when the desk has taken in cash.
CREATE TEMPORARY VIEW positions AS
SELECT book_id, instrument_id,
       SUM(CASE WHEN side = 'BUY' THEN qty ELSE -qty END) AS net_qty,
       SUM(CASE WHEN side = 'BUY' THEN qty ELSE -qty END * price) AS cost,
       COUNT(*) AS trades
FROM trades_enriched
WHERE status <> 'CANCELLED'
GROUP BY book_id, instrument_id;

INSERT INTO position_mtm
SELECT po.book_id, po.instrument_id, po.net_qty, po.cost, po.trades,
       p.price,
       po.net_qty * p.price - po.cost
FROM positions po
JOIN prices_latest p ON po.instrument_id = p.instrument_id;
