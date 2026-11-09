-- Keeps one current row per trade: reads every version from `trades` and
-- publishes the highest version of each trade to the compacted `trades_latest`.

SET 'pipeline.name' = 'latest_trades';

-- Flink only recovers from a crash if checkpoints are on.
SET 'execution.checkpointing.interval' = '10s';

CREATE TABLE trades (
  trade_id STRING,
  version INT,
  status STRING,
  instrument_id STRING,
  counterparty_id STRING,
  book_id STRING,
  side STRING,
  qty INT,
  price DECIMAL(18,2),
  trade_time BIGINT,
  event_time BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'trades',
  'properties.bootstrap.servers' = 'kafka:9092',
  -- Only used on a fresh start. After a crash, positions come from the checkpoint.
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE trades_latest (
  trade_id STRING,
  version INT,
  status STRING,
  instrument_id STRING,
  counterparty_id STRING,
  book_id STRING,
  side STRING,
  qty INT,
  price DECIMAL(18,2),
  trade_time BIGINT,
  event_time BIGINT,
  PRIMARY KEY (trade_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'trades_latest',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'value.format' = 'json',
  -- Writes a plain number rather than 1.9E+2, which downstream readers might not parse.
  'value.json.encode.decimal-as-plain-number' = 'true'
);

-- Cancelled trades are kept as rows on purpose. Filtering them out here would
-- make every amend write a delete followed by the new value.
INSERT INTO trades_latest
SELECT trade_id, version, status, instrument_id, counterparty_id,
       book_id, side, qty, price, trade_time, event_time
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY version DESC) AS rn
  FROM trades
)
WHERE rn = 1;
