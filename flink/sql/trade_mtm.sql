-- Values every trade at the latest price and publishes one current row per trade
-- to the compacted `trade_mtm`.

SET 'pipeline.name' = 'trade_mtm';
SET 'parallelism.default' = '1';

-- Flink only recovers from a crash if checkpoints are on.
SET 'execution.checkpointing.interval' = '10s';

-- Conflation: operators buffer input and process only the newest row per key.
-- P&L can be up to about 1 second old. The size limit stops a replay filling memory.
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

-- The same `prices` topic the candles read, but as "the current price per instrument".
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

CREATE TABLE trade_mtm (
  trade_id STRING,
  version INT,
  status STRING,
  instrument_id STRING,
  instrument_name STRING,
  counterparty_id STRING,
  counterparty_name STRING,
  book_id STRING,
  book_name STRING,
  side STRING,
  qty INT,
  trade_price DECIMAL(18,2),
  live_price DECIMAL(18,2),
  mtm DECIMAL(38,2),
  PRIMARY KEY (trade_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'trade_mtm',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'value.format' = 'json',
  'value.json.encode.decimal-as-plain-number' = 'true',
  -- A price change leaves the join as a delete then an insert. Holding messages
  -- for up to 1 second keeps only the newest per key, so the delete is not written.
  'sink.buffer-flush.max-rows' = '5000',
  'sink.buffer-flush.interval' = '1s'
);

-- INNER JOIN: a LEFT JOIN writes a row with no price between the old and new price.
-- Cancelled trades stay, with zero P&L, so readers can see the cancel.
INSERT INTO trade_mtm
SELECT t.trade_id, t.version, t.status,
       t.instrument_id, t.instrument_name,
       t.counterparty_id, t.counterparty_name,
       t.book_id, t.book_name,
       t.side, t.qty,
       t.price,
       p.price,
       CASE WHEN t.status = 'CANCELLED' THEN 0
            ELSE CASE WHEN t.side = 'BUY' THEN t.qty ELSE -t.qty END * (p.price - t.price)
       END
FROM trades_enriched t
JOIN prices_latest p ON t.instrument_id = p.instrument_id;
