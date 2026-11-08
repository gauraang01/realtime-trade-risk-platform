# Real-time trade & risk dashboard

A teaching project: a toy bank trading desk's live trade blotter and counterparty credit panel, built locally with Docker Compose, one sprint at a time.

Nothing is runnable yet. The stack starts in Sprint 1.

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
- [LEARNING_LOG.md](LEARNING_LOG.md): lessons from each sprint
