"""Summarise risk staleness from the current contents of a trade view topic.

Usage: .venv/bin/python scripts/stale_check.py [topic]
"""
import json
import sys
import time

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

BOOTSTRAP = "localhost:9094"
ROWS_TO_SHOW = 3


def read_current(topic: str) -> dict:
    """Read up to the end of each partition as it is now, keeping the newest row per trade."""
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "stale-check", "enable.auto.commit": False})
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    ends = {p: consumer.get_watermark_offsets(TopicPartition(topic, p))[1] for p in partitions}
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions if ends[p] > 0])

    rows, waiting = {}, {p for p in partitions if ends[p] > 0}
    while waiting:
        msg = consumer.poll(1)
        if msg is None or msg.error():
            continue
        # A tombstone here is the first half of a price update, not a closed trade.
        # Closed trades arrive as rows with status CANCELLED, so keep the last real row.
        if msg.value() is not None:
            rows[msg.key().decode()] = json.loads(msg.value())
        if msg.offset() >= ends[msg.partition()] - 1:
            waiting.discard(msg.partition())
    consumer.close()
    return rows


def main() -> None:
    topic = sys.argv[1] if len(sys.argv) > 1 else "trade_view"
    rows = read_current(topic)
    now = time.time() * 1000

    live = [r for r in rows.values() if r["status"] != "CANCELLED"]
    stale = [r for r in live if r["risk_stale"]]
    no_risk = [r for r in stale if r["risk_trade_version"] is None]
    amended = [r for r in stale if r["risk_trade_version"] is not None]
    ages = [(now - r["risk_as_of"]) / 1000 for r in live if r["risk_as_of"] is not None]

    print(f"live trades: {len(live)}   cancelled: {len(rows) - len(live)}")
    print(f"stale: {len(stale)}   no risk yet: {len(no_risk)}   amended since last report: {len(amended)}")
    if ages:
        print(f"risk age in seconds: newest {min(ages):.0f}, oldest {max(ages):.0f}")
    for r in amended[:ROWS_TO_SHOW]:
        print(f"  {r['trade_id']}: trade is v{r['version']}, risk is from v{r['risk_trade_version']}")


if __name__ == "__main__":
    main()
