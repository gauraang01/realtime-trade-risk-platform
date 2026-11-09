-- Total exposure per counterparty against its credit limit.
-- Publishes one current row per counterparty to the compacted `counterparty_view`.

SET 'pipeline.name' = 'counterparty_view';
SET 'parallelism.default' = '1';

-- Flink only recovers from a crash if checkpoints are on.
SET 'execution.checkpointing.interval' = '10s';

-- Conflation: process only the newest row per key each second.
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '5000';

-- trades_latest, not trades_enriched: a limit change rewrites every enriched
-- trade, and totals built on that rewrite briefly dip.
CREATE TABLE trades_latest (
  trade_id STRING,
  status STRING,
  instrument_id STRING,
  counterparty_id STRING,
  qty INT,
  PRIMARY KEY (trade_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'trades_latest',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'value.format' = 'json'
);

CREATE TABLE prices_latest (
  instrument_id STRING,
  price DECIMAL(18,2),
  PRIMARY KEY (instrument_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'prices',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'value.format' = 'json'
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

CREATE TABLE counterparty_view (
  counterparty_id STRING,
  counterparty_name STRING,
  exposure DECIMAL(38,2),
  credit_limit DECIMAL(18,2),
  breached BOOLEAN,
  PRIMARY KEY (counterparty_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'counterparty_view',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'value.format' = 'json',
  'value.json.encode.decimal-as-plain-number' = 'true',
  -- Holding messages for up to 1 second keeps only the newest per key.
  'sink.buffer-flush.max-rows' = '5000',
  'sink.buffer-flush.interval' = '1s'
);

-- Total the quantity first, then price it: the price join touches at most
-- (funds x instruments) rows per tick, not one row per trade.
-- LEFT JOIN on the limit: a fund with no limit still shows, with breached NULL.
INSERT INTO counterparty_view
WITH gross AS (
  SELECT counterparty_id, instrument_id, SUM(CAST(ABS(qty) AS BIGINT)) AS gross_qty
  FROM trades_latest
  WHERE status <> 'CANCELLED'
  GROUP BY counterparty_id, instrument_id
),
exposure AS (
  SELECT g.counterparty_id, SUM(g.gross_qty * p.price) AS exposure
  FROM gross g
  JOIN prices_latest p ON g.instrument_id = p.instrument_id
  GROUP BY g.counterparty_id
)
SELECT e.counterparty_id, c.name, e.exposure, c.credit_limit, e.exposure > c.credit_limit
FROM exposure e
LEFT JOIN counterparties c ON e.counterparty_id = c.counterparty_id;
