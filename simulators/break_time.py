"""Send hand-timed ticks to `prices_test` to show late data and a silent partition.

Each tick is stamped a number of seconds after 10:00:00, whatever the real time
is. Instrument P0 goes to partition 0, P1 to partition 1 and P2 to partition 2.
"""
import argparse
import json
import time
from datetime import datetime, timezone

from common import make_producer

TOPIC = "prices_test"
START_MS = int(datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc).timestamp() * 1000)
ALL = (0, 1, 2)

producer = make_producer()


def tick(seconds: int, price: float, partitions=ALL, note: str = "") -> None:
    for p in partitions:
        message = {"instrument_id": f"P{p}", "price": price, "event_time": START_MS + seconds * 1000}
        producer.produce(TOPIC, key=f"P{p}", value=json.dumps(message), partition=p)
    producer.flush(5)
    minute, second = divmod(seconds, 60)
    print(f"sent 10:{minute:02d}:{second:02d} price {price} to {list(partitions)}  {note}", flush=True)
    # Flink publishes watermarks every 200 ms, so the pause lets each step take effect.
    time.sleep(1)


def late() -> None:
    tick(10, 100)
    tick(50, 110)
    tick(40, 999, [0], "out of order, but its window is still open")
    tick(66, 120, note="watermark reaches 10:01:01, so the 10:00 window closes")
    time.sleep(3)
    tick(58, 1, [0], "too late: the 10:00 window has closed")
    tick(126, 130, note="watermark reaches 10:02:01, so the 10:01 window closes")


def silent() -> None:
    tick(10, 100)
    tick(50, 110)
    print("partition 1 goes silent for 15 seconds", flush=True)
    for i in range(15):
        tick(66 + i, 120, [0, 2])
    tick(58, 500, [1], "partition 1 is back, with a tick from the 10:00 minute")
    tick(130, 130, note="every partition has moved on")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", choices=["late", "silent"])
    args = parser.parse_args()
    {"late": late, "silent": silent}[args.scenario]()
