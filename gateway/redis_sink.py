"""Keep the latest row per key from the view topics in Redis.

Each topic becomes one Redis hash: field = Kafka key, value = the row's JSON.
Redis is a cache of Kafka, so every start rebuilds it from the beginning of the topics.
"""
import time

import redis
from confluent_kafka import OFFSET_BEGINNING, Consumer

BOOTSTRAP = "localhost:9094"
TOPICS = ["trade_view", "counterparty_view"]
FLUSH_SECONDS = 0.25
REPORT_SECONDS = 10


def from_beginning(consumer: Consumer, partitions: list) -> None:
    """Ignore any saved position: a rebuild always starts at the first message."""
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING
    consumer.assign(partitions)


def write(cache: redis.Redis, pending: dict) -> None:
    """Send a batch in one round trip. Sending one write per message would be far slower."""
    pipe = cache.pipeline(transaction=False)
    for (topic, key), value in pending.items():
        pipe.hset(topic, key, value)
    pipe.execute()


def main() -> None:
    cache = redis.Redis(host="localhost", port=6379)
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "redis-sink",
        "enable.auto.commit": False,  # nothing to resume from, see from_beginning
    })
    consumer.subscribe(TOPICS, on_assign=from_beginning)

    pending = {}  # newest value per key since the last write
    read, written = 0, 0
    next_flush = time.monotonic() + FLUSH_SECONDS
    next_report = time.monotonic() + REPORT_SECONDS

    try:
        while True:
            msg = consumer.poll(0.05)
            if msg is not None:
                if msg.error():
                    print(f"kafka error: {msg.error()}", flush=True)
                # A tombstone is the first half of a price update, not a closed trade.
                # Closed trades arrive as rows with status CANCELLED, so keep the last real row.
                elif msg.value() is not None:
                    pending[(msg.topic(), msg.key().decode())] = msg.value()
                    read += 1

            now = time.monotonic()
            if now >= next_flush:
                if pending:
                    write(cache, pending)
                    written += len(pending)
                    pending.clear()
                next_flush = now + FLUSH_SECONDS
            if now >= next_report:
                print(f"read {read} messages, wrote {written} rows", flush=True)
                read, written = 0, 0
                next_report = now + REPORT_SECONDS
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
