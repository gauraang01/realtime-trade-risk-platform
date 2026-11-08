"""Break it on purpose: send one trade's versions late and twice.

A correct "latest version" view must end on the highest version, whatever order
the messages arrive in. The trade finishes CANCELLED, so this test trade can't
count towards exposure in later sprints.
"""
import argparse
import json
import time

from common import make_producer, now_ms

# (version, status, qty, what this message is testing)
STEPS = [
    (1, "NEW", 100, "first version"),
    (3, "AMENDED", 300, "skips ahead: v2 hasn't arrived yet"),
    (2, "AMENDED", 200, "late: older than v3"),
    (3, "AMENDED", 300, "duplicate of v3"),
    (4, "CANCELLED", 300, "clean-up: takes the test trade out"),
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="trades")
    parser.add_argument("--pause", type=float, default=3, help="seconds between messages")
    args = parser.parse_args()

    trade_id = f"T-TEST-{int(time.time()) % 1_000_000}"
    trade_time = now_ms()
    producer = make_producer()

    for version, status, qty, note in STEPS:
        trade = {
            "trade_id": trade_id, "version": version, "status": status,
            "instrument_id": "AAPL", "counterparty_id": "CP-1", "book_id": "BOOK-1",
            "side": "BUY", "qty": qty, "price": 190.00,
            "trade_time": trade_time, "event_time": now_ms(),
        }
        producer.produce(args.topic, key=trade_id, value=json.dumps(trade))
        # Flush each one so the arrival order is exactly the order above.
        if producer.flush(10) != 0:
            raise RuntimeError("message was not delivered")
        print(f"sent {trade_id} v{version} {status} qty {qty}  ({note})")
        time.sleep(args.pause)


if __name__ == "__main__":
    main()
