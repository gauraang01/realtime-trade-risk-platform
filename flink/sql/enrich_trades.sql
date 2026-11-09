-- Adds instrument, counterparty and book details to the latest row of each trade,
-- and publishes the result to the compacted `trades_enriched`.

SET 'pipeline.name' = 'enrich_trades';
SET 'parallelism.default' = '1';

-- Flink only recovers from a crash if checkpoints are on.
SET 'execution.checkpointing.interval' = '10s';

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
  'value.format' = 'json'
);

CREATE TABLE instruments (
  instrument_id STRING,
  name STRING,
  daily_vol DECIMAL(6,4),
  PRIMARY KEY (instrument_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'refdata.public.instruments',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TABLE counterparties (
  counterparty_id STRING,
  name STRING,
  credit_limit DECIMAL(18,2),
  PRIMARY KEY (counterparty_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'refdata.public.counterparties',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TABLE books (
  book_id STRING,
  name STRING,
  PRIMARY KEY (book_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'refdata.public.books',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

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
  'value.json.encode.decimal-as-plain-number' = 'true'
);

-- Regular joins, because the limit check needs the current limit, not the one
-- in force when the trade was booked. LEFT JOIN so a trade with missing
-- reference data still reaches the blotter, with NULL details, instead of vanishing.
INSERT INTO trades_enriched
SELECT t.trade_id, t.version, t.status,
       t.instrument_id, i.name, i.daily_vol,
       t.counterparty_id, c.name, c.credit_limit,
       t.book_id, b.name,
       t.side, t.qty, t.price, t.trade_time, t.event_time
FROM trades_latest t
LEFT JOIN instruments i ON t.instrument_id = i.instrument_id
LEFT JOIN counterparties c ON t.counterparty_id = c.counterparty_id
LEFT JOIN books b ON t.book_id = b.book_id;
