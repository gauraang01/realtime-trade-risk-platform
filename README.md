# Real-time trade & risk dashboard

A teaching project: a toy bank trading desk's live trade blotter and counterparty credit panel, built locally with Docker Compose, one sprint at a time.

## Running it

Kafka and Kafka UI are the only services so far:

```bash
docker compose --profile kafka up -d
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
