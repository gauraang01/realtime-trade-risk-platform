"""Break it on purpose: what happens when a trade's versions don't share a key?

Each trade is published as NEW (v1) then CANCELLED (v2), so every trade should end
up cancelled. A simple reader keeps the last message it read for each trade.
"""
import json
import random
import time

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from common import BOOTSTRAP, INSTRUMENTS, make_producer

TRADES = 300
PARTITIONS = 3

# Experiment name -> which field is the key for the NEW and for the CANCELLED message.
EXPERIMENTS = {
    "both keyed by trade_id": ("trade_id", "trade_id"),
    "both keyed by instrument_id": ("instrument_id", "instrument_id"),
    "cancel uses a different key": ("trade_id", "instrument_id"),
}


def publish(topic: str, new_key: str, cancel_key: str) -> None:
    producer = make_producer()
    for n in range(TRADES):
        trade = {"trade_id": f"T-{n}", "instrument_id": random.choice(list(INSTRUMENTS))}
        for version, status, key_field in ((1, "NEW", new_key), (2, "CANCELLED", cancel_key)):
            value = json.dumps({**trade, "version": version, "status": status})
            producer.produce(topic, key=trade[key_field], value=value)
    if producer.flush(10) != 0:
        raise RuntimeError("some messages were not delivered")


def read_all(topic: str) -> list:
    consumer = Consumer(
        {"bootstrap.servers": BOOTSTRAP, "group.id": "break-wrong-key", "enable.auto.commit": False}
    )
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in range(PARTITIONS)])
    messages = []
    while len(messages) < TRADES * 2:
        msg = consumer.poll(5)
        if msg is None:
            raise RuntimeError(f"timed out after reading {len(messages)} messages")
        if not msg.error():
            messages.append(json.loads(msg.value()))
    consumer.close()
    return messages


def report(name: str, messages: list) -> None:
    last_read, highest = {}, {}
    for message in messages:
        trade_id = message["trade_id"]
        last_read[trade_id] = message
        if trade_id not in highest or message["version"] > highest[trade_id]["version"]:
            highest[trade_id] = message

    def wrongly_live(view: dict) -> int:
        return sum(t["status"] != "CANCELLED" for t in view.values())

    print(f"{name:<30} wrongly live of {TRADES}:  last read wins: {wrongly_live(last_read):<5} "
          f"highest version wins: {wrongly_live(highest)}")


def main() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    for name, (new_key, cancel_key) in EXPERIMENTS.items():
        topic = f"break-wrong-key-{int(time.time() * 1000)}"
        admin.create_topics([NewTopic(topic, num_partitions=PARTITIONS, replication_factor=1)])[topic].result()
        try:
            publish(topic, new_key, cancel_key)
            report(name, read_all(topic))
        finally:
            admin.delete_topics([topic])[topic].result()


if __name__ == "__main__":
    main()
