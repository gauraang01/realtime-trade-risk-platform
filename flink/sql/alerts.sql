-- Publishes one message to `alerts` when a counterparty goes over its limit,
-- and one when it comes back under. A single odd value never causes either.

SET 'pipeline.name' = 'alerts';
SET 'parallelism.default' = '1';

-- Flink only recovers from a crash if checkpoints are on.
SET 'execution.checkpointing.interval' = '10s';

-- Each message is treated as one sample of the panel, so this is the plain kafka
-- connector. latest-offset: an alert is about now, not about replayed history.
CREATE TABLE samples (
  counterparty_id STRING,
  counterparty_name STRING,
  exposure DECIMAL(38,2),
  credit_limit DECIMAL(18,2),
  arrived_at AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'counterparty_view',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

-- Plain kafka, not upsert-kafka: alerts are events, so each must be kept
-- rather than replaced by the next one for the same fund.
CREATE TABLE alerts (
  counterparty_id STRING,
  counterparty_name STRING,
  alert_state STRING,
  exposure DECIMAL(38,2),
  credit_limit DECIMAL(18,2),
  alerted_at TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'alerts',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'key.fields' = 'counterparty_id',
  'value.format' = 'json',
  'value.json.encode.decimal-as-plain-number' = 'true'
);

-- A window only counts if every sample agrees. Mixed windows give NULL and are
-- dropped, so the fund keeps its previous state.
-- LAG looks at the previous definite state, and only a change produces an alert.
INSERT INTO alerts
WITH checked AS (
  SELECT counterparty_id,
         LAST_VALUE(counterparty_name) AS counterparty_name,
         LAST_VALUE(exposure) AS exposure,
         LAST_VALUE(credit_limit) AS credit_limit,
         window_end,
         window_time,
         CASE WHEN MIN(exposure - credit_limit) > 0 THEN 'BREACH'
              WHEN MAX(exposure - credit_limit) <= 0 THEN 'OK'
         END AS alert_state
  FROM TABLE(TUMBLE(TABLE samples, DESCRIPTOR(arrived_at), INTERVAL '5' SECOND))
  GROUP BY counterparty_id, window_start, window_end, window_time
),
changes AS (
  SELECT *, LAG(alert_state) OVER (PARTITION BY counterparty_id ORDER BY window_time) AS previous_state
  FROM checked
  WHERE alert_state IS NOT NULL
)
SELECT counterparty_id, counterparty_name, alert_state, exposure, credit_limit, window_end
FROM changes
WHERE alert_state <> COALESCE(previous_state, 'OK');
