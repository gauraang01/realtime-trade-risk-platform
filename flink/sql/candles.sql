-- Builds 1-minute open/high/low/close candles per instrument from `prices`.

SET 'pipeline.name' = 'candles';

SET 'execution.checkpointing.interval' = '10s';

CREATE TABLE prices (
  instrument_id STRING,
  price DECIMAL(18,2),
  event_time BIGINT,
  -- event_time is epoch milliseconds, so the precision is 3.
  ts AS TO_TIMESTAMP_LTZ(event_time, 3),
  -- A window closes once a tick stamped 5 seconds past its end has been seen.
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'prices',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Keyed by instrument and minute, so a replay overwrites the same candle
-- instead of adding a duplicate.
CREATE TABLE candles_1m (
  instrument_id STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  `open` DECIMAL(18,2),
  high DECIMAL(18,2),
  low DECIMAL(18,2),
  `close` DECIMAL(18,2),
  ticks BIGINT,
  PRIMARY KEY (instrument_id, window_start) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'candles_1m',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.json.encode.decimal-as-plain-number' = 'true'
);

INSERT INTO candles_1m
SELECT instrument_id, window_start, window_end,
       FIRST_VALUE(price),
       MAX(price),
       MIN(price),
       LAST_VALUE(price),
       COUNT(*)
FROM TABLE(TUMBLE(TABLE prices, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
GROUP BY instrument_id, window_start, window_end;
