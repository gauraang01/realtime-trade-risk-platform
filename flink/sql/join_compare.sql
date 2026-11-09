-- Experiment: runs a regular join and a temporal join side by side on the same
-- trades, so a reference-data change shows how differently they behave.
-- Output goes to the TaskManager log, with lines starting JOIN-REGULAR / JOIN-TEMPORAL.

SET 'pipeline.name' = 'join_compare';

-- One slot is all that's free next to latest_trades.
SET 'parallelism.default' = '1';

-- A source silent for 5 s stops holding back the job's watermark. Without this,
-- the temporal join waits forever on the quiet reference topic.
SET 'table.exec.source.idle-timeout' = '5s';

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
  event_time BIGINT,
  ts AS TO_TIMESTAMP_LTZ(event_time, 3),
  WATERMARK FOR ts AS ts
) WITH (
  'connector' = 'kafka',
  'topic' = 'trades',
  'properties.bootstrap.servers' = 'kafka:9092',
  -- Only trades sent after the job starts, to keep the output readable.
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

CREATE TABLE counterparties (
  counterparty_id STRING,
  name STRING,
  credit_limit DECIMAL(18,2),
  updated_at TIMESTAMP_LTZ(3) METADATA FROM 'value.source.timestamp' VIRTUAL,
  WATERMARK FOR updated_at AS updated_at,
  PRIMARY KEY (counterparty_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'refdata.public.counterparties',
  'properties.bootstrap.servers' = 'kafka:9092',
  -- From the start, so every counterparty is known before the first trade.
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TABLE print_regular (
  trade_id STRING,
  counterparty_id STRING,
  credit_limit DECIMAL(18,2)
) WITH ('connector' = 'print', 'print-identifier' = 'JOIN-REGULAR');

CREATE TABLE print_temporal (
  trade_id STRING,
  counterparty_id STRING,
  credit_limit DECIMAL(18,2)
) WITH ('connector' = 'print', 'print-identifier' = 'JOIN-TEMPORAL');

-- A statement set runs both INSERTs as one job, so they share the one free slot.
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO print_regular
  SELECT t.trade_id, t.counterparty_id, c.credit_limit
  FROM trades t
  JOIN counterparties c ON t.counterparty_id = c.counterparty_id;

  INSERT INTO print_temporal
  SELECT t.trade_id, t.counterparty_id, c.credit_limit
  FROM trades t
  LEFT JOIN counterparties FOR SYSTEM_TIME AS OF t.ts AS c
    ON t.counterparty_id = c.counterparty_id;
END;
