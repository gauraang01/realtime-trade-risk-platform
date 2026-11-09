-- Attaches the latest risk result to every trade and flags risk that no longer
-- matches the trade. Publishes one current row per trade to the compacted `trade_view`.

SET 'pipeline.name' = 'trade_view';
SET 'parallelism.default' = '1';

-- Flink only recovers from a crash if checkpoints are on.
SET 'execution.checkpointing.interval' = '10s';

-- Conflation, as in trade_mtm: process only the newest row per key each second.
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '5000';

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
  'value.format' = 'json'
);

-- upsert-kafka: the newest report for a trade replaces the one before it.
CREATE TABLE risk_results (
  trade_id STRING,
  trade_version INT,
  risk DECIMAL(18,2),
  as_of BIGINT,
  PRIMARY KEY (trade_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'risk_results',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'value.format' = 'json'
);

-- One row per trade whose reports stopped. See risk_silent.sql.
CREATE TABLE risk_silent (
  trade_id STRING,
  last_as_of BIGINT,
  PRIMARY KEY (trade_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'risk_silent',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'value.format' = 'json'
);

CREATE TABLE trade_view (
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
  risk DECIMAL(18,2),
  risk_as_of BIGINT,
  risk_trade_version INT,
  risk_stale BOOLEAN,
  risk_silent BOOLEAN,
  PRIMARY KEY (trade_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'trade_view',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'value.format' = 'json',
  'value.json.encode.decimal-as-plain-number' = 'true',
  -- Holding messages for up to 1 second keeps only the newest per key.
  'sink.buffer-flush.max-rows' = '5000',
  'sink.buffer-flush.interval' = '1s'
);

-- LEFT JOIN: a trade with no risk yet must still reach the blotter.
-- `<` and not `<>`: a report can overtake its trade through the pipeline,
-- and a report based on a newer version is not stale.
-- A cancelled trade has no risk, so its last report is shown as zero and never stale.
-- Silence only counts if it came after the latest report: a newer report clears it,
-- with no extra message needed.
INSERT INTO trade_view
SELECT m.trade_id, m.version, m.status,
       m.instrument_id, m.instrument_name,
       m.counterparty_id, m.counterparty_name,
       m.book_id, m.book_name,
       m.side, m.qty,
       m.trade_price, m.live_price, m.mtm,
       CASE WHEN m.status = 'CANCELLED' THEN 0 ELSE r.risk END,
       r.as_of,
       r.trade_version,
       CASE WHEN m.status = 'CANCELLED' THEN FALSE
            WHEN r.trade_version IS NULL THEN TRUE
            WHEN r.trade_version < m.version THEN TRUE
            ELSE COALESCE(s.last_as_of >= r.as_of, FALSE)
       END,
       m.status <> 'CANCELLED' AND COALESCE(s.last_as_of >= r.as_of, FALSE)
FROM trade_mtm m
LEFT JOIN risk_results r ON m.trade_id = r.trade_id
LEFT JOIN risk_silent s ON m.trade_id = s.trade_id;
