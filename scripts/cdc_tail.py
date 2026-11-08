"""Show a reference topic one message per line, from an offset to the current end.

Usage: .venv/bin/python scripts/cdc_tail.py counterparties [start_offset]
"""
import json
import os
import sys

from confluent_kafka import Consumer, TopicPartition

table = sys.argv[1]
start = int(sys.argv[2]) if len(sys.argv) > 2 else 0
topic = f"refdata.public.{table}"

# assign() reads the partition directly: no consumer group, nothing committed.
consumer = Consumer(
    {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9094"),
        "group.id": "cdc-tail",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
)
_, end = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
consumer.assign([TopicPartition(topic, 0, start)])

print(f"{topic}: reading offsets {start} to {end - 1}")
while start < end:
    msg = consumer.poll(5)
    if msg is None:
        break
    if msg.error():
        print(f"error: {msg.error()}")
        continue
    key = list(json.loads(msg.key()).values())[0]
    if msg.value() is None:
        print(f"{msg.offset():>4}  TOMBSTONE  {key}")
    else:
        event = json.loads(msg.value())
        print(f"{msg.offset():>4}  op={event['op']}  {key}  before={event['before']}  after={event['after']}")
    if msg.offset() >= end - 1:
        break

consumer.close()
