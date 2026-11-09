# Real-time trade & risk dashboard

A teaching project: a toy bank trading desk's live trade blotter and counterparty credit panel, built locally with Docker Compose, one sprint at a time.

## Running it

The `kafka` profile runs Kafka and Kafka UI. The `cdc` profile adds Postgres and Debezium. The `flink` profile adds a Flink cluster to Kafka and Kafka UI (see [Stream processing](#stream-processing)):

```bash
docker compose --profile cdc up -d
scripts/k kafka-topics --list
```

Kafka UI is at http://localhost:8080.

Create the topics once (topic auto-creation is off):

```bash
scripts/k kafka-topics --create --topic trades --partitions 3 --replication-factor 1 \
  --config cleanup.policy=delete --config retention.ms=604800000
scripts/k kafka-topics --create --topic prices --partitions 3 --replication-factor 1 \
  --config cleanup.policy=delete --config retention.ms=86400000
```

Run the simulators, each in its own terminal, with prices first:

```bash
python3 -m venv .venv
.venv/bin/pip install -r simulators/requirements.txt
.venv/bin/python simulators/prices.py
.venv/bin/python simulators/trades.py
```

### Reference data

Create the compacted topics once, then register the connector. It takes a snapshot of the tables and then follows every change:

```bash
for t in instruments counterparties books; do
  scripts/k kafka-topics --create --topic refdata.public.$t \
    --partitions 1 --replication-factor 1 \
    --config cleanup.policy=compact \
    --config segment.ms=60000 \
    --config min.cleanable.dirty.ratio=0.01
done

curl -s -X POST -H 'Content-Type: application/json' \
  localhost:8083/connectors -d @debezium/refdata-connector.json
curl -s localhost:8083/connectors/refdata/status
```

To view a topic's change events, one per line:

```bash
.venv/bin/python scripts/cdc_tail.py counterparties
```

### Stream processing

Start everything, then open the Flink SQL client, which runs one statement at a time:

```bash
docker compose --profile cdc --profile flink up -d --build
curl -s localhost:8081/overview
docker exec -it jobmanager ./bin/sql-client.sh
```

The Flink UI is at http://localhost:8081.

The saved job in `flink/sql/latest_trades.sql` publishes one current row per trade. Create its output topic once, then submit the job. Submit it again after restarting the stack:

```bash
scripts/k kafka-topics --create --topic trades_latest --partitions 3 --replication-factor 1 \
  --config cleanup.policy=compact --config segment.ms=60000 --config min.cleanable.dirty.ratio=0.01
docker exec jobmanager ./bin/sql-client.sh -f /opt/flink/sql/latest_trades.sql 2>&1 | grep -E 'Job ID|ERROR'
```

To test late and duplicate messages, run `.venv/bin/python simulators/break_versions.py`.

The saved job in `flink/sql/enrich_trades.sql` adds instrument, counterparty and book details to each trade. Create its output topic once, then submit it after `latest_trades.sql`:

```bash
scripts/k kafka-topics --create --topic trades_enriched --partitions 3 --replication-factor 1 \
  --config cleanup.policy=compact --config segment.ms=60000 --config min.cleanable.dirty.ratio=0.01
docker exec jobmanager ./bin/sql-client.sh -f /opt/flink/sql/enrich_trades.sql 2>&1 | grep -E 'Job ID|ERROR'
```

To compare a regular join with a temporal join, submit `flink/sql/join_compare.sql`, follow `docker logs -f --tail 0 taskmanager`, and send trades with `.venv/bin/python simulators/test_trade.py` while changing a row in `counterparties`. It needs a free task slot.

The saved job in `flink/sql/candles.sql` builds 1-minute open/high/low/close candles per instrument. Create its output topic once, then submit the job:

```bash
scripts/k kafka-topics --create --topic candles_1m --partitions 3 --replication-factor 1 \
  --config cleanup.policy=compact --config segment.ms=60000 --config min.cleanable.dirty.ratio=0.01
docker exec jobmanager ./bin/sql-client.sh -f /opt/flink/sql/candles.sql 2>&1 | grep -E 'Job ID|ERROR'
```

To test late data and a silent partition, create a `prices_test` topic with 3 partitions, start a windowed query that reads it from `latest-offset`, then run `.venv/bin/python simulators/break_time.py late` or `silent`.

The saved jobs in `flink/sql/trade_mtm.sql` and `flink/sql/position_mtm.sql` value each trade and each position at the latest price, updating about once a second. Create their output topics once, then submit each job:

```bash
for t in trade_mtm position_mtm; do
  scripts/k kafka-topics --create --topic $t --partitions 3 --replication-factor 1 \
    --config cleanup.policy=compact --config segment.ms=60000 --config min.cleanable.dirty.ratio=0.01
done
docker exec jobmanager ./bin/sql-client.sh -f /opt/flink/sql/trade_mtm.sql 2>&1 | grep -A2 -E 'Job ID|ERROR'
docker exec jobmanager ./bin/sql-client.sh -f /opt/flink/sql/position_mtm.sql 2>&1 | grep -A2 -E 'Job ID|ERROR'
```

`simulators/risk_engine.py` reports risk for every live trade every 30 seconds, and `flink/sql/trade_view.sql` attaches the latest report to each trade with a staleness flag. Create the two topics once, start the engine, then submit the job:

```bash
for t in risk_results trade_view; do
  scripts/k kafka-topics --create --topic $t --partitions 3 --replication-factor 1 \
    --config cleanup.policy=compact --config segment.ms=60000 --config min.cleanable.dirty.ratio=0.01
done
.venv/bin/python simulators/risk_engine.py
docker exec jobmanager ./bin/sql-client.sh -f /opt/flink/sql/trade_view.sql 2>&1 | grep -A2 -E 'Job ID|ERROR'
```

Run `.venv/bin/python scripts/stale_check.py` to see how many trades have stale risk and how old the latest reports are.

`flink/sql/risk_silent.sql` marks a trade when no risk report has arrived for 2 minutes, and `trade_view` shows that as stale. `flink/sql/counterparty_view.sql` compares each counterparty's exposure with its credit limit, and `flink/sql/alerts.sql` publishes one message when a counterparty goes over its limit and one when it comes back. Create the topics once, then submit the jobs:

```bash
for t in risk_silent counterparty_view; do
  scripts/k kafka-topics --create --topic $t --partitions 3 --replication-factor 1 \
    --config cleanup.policy=compact --config segment.ms=60000 --config min.cleanable.dirty.ratio=0.01
done
scripts/k kafka-topics --create --topic alerts --partitions 3 --replication-factor 1 \
  --config cleanup.policy=delete --config retention.ms=604800000
for j in risk_silent counterparty_view alerts; do
  docker exec jobmanager ./bin/sql-client.sh -f /opt/flink/sql/$j.sql 2>&1 | grep -A2 -E 'Job ID|ERROR'
done
```

Run `.venv/bin/python scripts/exposure_check.py` to see exposure against limits, and `scripts/k kafka-console-consumer --topic alerts` to follow alerts.

Each saved job uses one of the eight task slots. `flink/sql/fanout_demo.sql` measures the output of an unconflated price join, and `.venv/bin/python scripts/topic_rate.py <topic>` measures a topic's message rate.

## Architecture (target)

```
Postgres ─ Debezium ─┐ reference data
                     ▼
Simulators ──────► KAFKA ──► FLINK ──► KAFKA ─┬─► ClickHouse  (history)
trades, prices,    inputs    logic    outputs ├─► Redis       (latest snapshot)
risk_results                                  └─► FastAPI ──► Browser grid
                                                  (reads snapshot from Redis)
```

## Repo layout

| Path | What goes there | Sprint |
|---|---|---|
| `simulators/` | Python producers: prices, trades, risk engine | 2, 9 |
| `postgres/` | Reference data schema and seed | 3 |
| `debezium/` | CDC connector config | 3 |
| `flink/` | Flink SQL and PyFlink jobs | 4–9 |
| `clickhouse/` | Serving tables and views | 10 |
| `gateway/` | FastAPI WebSocket gateway | 11 |
| `ui/` | HTML/JS grid | 11 |
| `spark/` | Spark comparison and Iceberg sink | 12 |
| `scripts/` | Reconciliation and helper scripts | 13 |

## Where to look

- [GLOSSARY.md](GLOSSARY.md): business and technical terms
