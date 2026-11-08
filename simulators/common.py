"""Shared pieces for the simulators: instruments and the Kafka producer."""
import os
import time

from confluent_kafka import Producer

# ticker -> starting price and daily volatility (0.02 means a typical day moves about 2%).
INSTRUMENTS = {
    "AAPL": {"price": 190.0, "daily_vol": 0.016},
    "MSFT": {"price": 420.0, "daily_vol": 0.014},
    "NVDA": {"price": 130.0, "daily_vol": 0.032},
    "AMZN": {"price": 185.0, "daily_vol": 0.020},
    "GOOGL": {"price": 170.0, "daily_vol": 0.018},
    "META": {"price": 560.0, "daily_vol": 0.024},
    "TSLA": {"price": 250.0, "daily_vol": 0.038},
    "JPM": {"price": 225.0, "daily_vol": 0.015},
    "XOM": {"price": 115.0, "daily_vol": 0.016},
    "KO": {"price": 65.0, "daily_vol": 0.009},
}


COUNTERPARTIES = ["CP-1", "CP-2", "CP-3", "CP-4", "CP-5"]
BOOKS = ["BOOK-1", "BOOK-2", "BOOK-3"]

# The Mac-side listener. Containers would use kafka:9092 instead.
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")


def make_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            # Retries can't duplicate or reorder messages. Also turns on acks=all.
            "enable.idempotence": True,
            # Same key hashing as Java clients, so all writers agree on a key's partition.
            "partitioner": "murmur2_random",
            # Wait up to 20 ms to fill a batch: fewer requests, tiny added delay.
            "linger.ms": 20,
            # Repetitive JSON compresses well, and lz4 is cheap on CPU.
            "compression.type": "lz4",
        }
    )


def now_ms() -> int:
    """Event time: when this happened, as epoch milliseconds in UTC."""
    return int(time.time() * 1000)
