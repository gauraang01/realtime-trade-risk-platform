-- Notices when risk reports stop arriving for a trade. Publishes one row per
-- silent trade to the compacted `risk_silent`, and writes nothing while reports flow.

SET 'pipeline.name' = 'risk_silent';
SET 'parallelism.default' = '1';

-- Flink only recovers from a crash if checkpoints are on.
SET 'execution.checkpointing.interval' = '10s';

-- The same topic trade_view reads as "the latest report". Here the plain kafka
-- connector treats each message as an event, because arrivals are what we count.
-- arrived_at is the wall clock: event time would freeze when the engine stops.
CREATE TABLE risk_reports (
  trade_id STRING,
  as_of BIGINT,
  arrived_at AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'risk_results',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE risk_silent (
  trade_id STRING,
  last_as_of BIGINT,
  silent_at TIMESTAMP(3),
  PRIMARY KEY (trade_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'risk_silent',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'value.format' = 'json'
);

-- The window for a trade closes once no report has arrived for 2 minutes.
-- last_as_of records which report the silence came after.
-- Not INTERVAL '120' SECOND: a seconds field holds 2 digits by default.
INSERT INTO risk_silent
SELECT trade_id, MAX(as_of), window_end
FROM TABLE(
  SESSION(TABLE risk_reports PARTITION BY trade_id, DESCRIPTOR(arrived_at), INTERVAL '2' MINUTE)
)
GROUP BY trade_id, window_start, window_end;
