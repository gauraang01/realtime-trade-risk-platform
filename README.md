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
