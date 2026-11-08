"""Publish random-walk price ticks to the `prices` topic, keyed by instrument."""
import argparse
import json
import math
import random
import time
from collections import Counter

from common import INSTRUMENTS, make_producer, now_ms

TOPIC = "prices"

# Squeeze a full trading day's volatility into one hour, so P&L visibly moves.
SIM_DAY_SECONDS = 3600


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=20, help="total ticks per second")
    args = parser.parse_args()

    prices = {ticker: info["price"] for ticker, info in INSTRUMENTS.items()}

    # Volatility grows with the square root of time, so scale it down to one tick.
    seconds_per_tick = len(INSTRUMENTS) / args.rate
    tick_scale = math.sqrt(seconds_per_tick / SIM_DAY_SECONDS)

    producer = make_producer()
    per_partition = Counter()
    errors = []

    def on_delivery(err, msg):
        if err:
            errors.append(err)
        else:
            per_partition[msg.partition()] += 1

    last_report = time.monotonic()
    try:
        while True:
            ticker = random.choice(list(INSTRUMENTS))
            # Multiplicative step: the price can never go negative.
            shock = random.gauss(0, INSTRUMENTS[ticker]["daily_vol"] * tick_scale)
            prices[ticker] *= math.exp(shock)

            tick = {
                "instrument_id": ticker,
                "price": round(prices[ticker], 2),
                "event_time": now_ms(),
            }
            producer.produce(TOPIC, key=ticker, value=json.dumps(tick), on_delivery=on_delivery)
            producer.poll(0)  # Runs on_delivery for any finished sends. Never blocks.

            if time.monotonic() - last_report >= 5:
                print(f"delivered per partition: {dict(sorted(per_partition.items()))}  errors: {len(errors)}")
                last_report = time.monotonic()
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        print("stopping, sending what's still buffered...")
    finally:
        unsent = producer.flush(5)
        print(f"done. delivered: {sum(per_partition.values())}  errors: {len(errors)}  unsent: {unsent}")


if __name__ == "__main__":
    main()
