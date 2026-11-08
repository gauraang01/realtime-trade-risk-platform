"""Publish trades to the `trades` topic, keyed by trade_id, with amends and cancels."""
import argparse
import json
import random
import time

from confluent_kafka import OFFSET_END, Consumer, TopicPartition

from common import BOOKS, BOOTSTRAP, COUNTERPARTIES, INSTRUMENTS, make_producer, now_ms

TRADES_TOPIC = "trades"
PRICES_TOPIC = "prices"

# Each step is a cancel, an amend, or otherwise a new trade.
CANCEL_CHANCE = 0.05
AMEND_CHANCE = 0.15


def watch_prices() -> Consumer:
    """Follow live ticks from now on, so the desk trades near the market price."""
    # assign() picks partitions directly and joins no consumer group. The client
    # still insists on a group.id, and we never commit offsets to it.
    consumer = Consumer(
        {"bootstrap.servers": BOOTSTRAP, "group.id": "trade-sim-prices", "enable.auto.commit": False}
    )
    partitions = consumer.list_topics(PRICES_TOPIC, timeout=10).topics[PRICES_TOPIC].partitions
    consumer.assign([TopicPartition(PRICES_TOPIC, p, OFFSET_END) for p in partitions])
    return consumer


def read_latest(consumer: Consumer, latest: dict) -> None:
    """Drain whatever ticks have arrived. Each ticker has one partition, so order holds."""
    while (msg := consumer.poll(0)) is not None:
        if msg.error():
            print(f"price consumer error: {msg.error()}")
            continue
        tick = json.loads(msg.value())
        latest[tick["instrument_id"]] = tick["price"]


def random_qty() -> int:
    return random.randrange(100, 2001, 100)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=1, help="trade events per second")
    args = parser.parse_args()

    consumer = watch_prices()
    producer = make_producer()
    latest, live = {}, {}
    delivered, errors = [0], []

    def on_delivery(err, msg):
        if err:
            errors.append(err)
        else:
            delivered[0] += 1

    # Part of every trade_id, so a restart can't reuse the IDs of an earlier run.
    run_id = int(time.time()) % 1_000_000
    next_id = 1

    try:
        while True:
            time.sleep(1 / args.rate)
            read_latest(consumer, latest)
            producer.poll(0)

            roll = random.random()
            if live and roll < CANCEL_CHANCE:
                trade = live.pop(random.choice(list(live)))
                trade.update(version=trade["version"] + 1, status="CANCELLED")
            elif live and roll < CANCEL_CHANCE + AMEND_CHANCE:
                trade = live[random.choice(list(live))]
                trade.update(version=trade["version"] + 1, status="AMENDED", qty=random_qty())
            elif latest:
                ticker = random.choice(list(latest))
                trade = {
                    "trade_id": f"T-{run_id}-{next_id}",
                    "version": 1,
                    "status": "NEW",
                    "instrument_id": ticker,
                    "counterparty_id": random.choice(COUNTERPARTIES),
                    "book_id": random.choice(BOOKS),
                    "side": random.choice(["BUY", "SELL"]),  # from the desk's point of view
                    "qty": random_qty(),
                    # A small gap from the market, so P&L doesn't start at exactly zero.
                    "price": round(latest[ticker] * (1 + random.gauss(0, 0.0005)), 2),
                    "trade_time": now_ms(),  # when first booked; amends keep this
                }
                live[trade["trade_id"]] = trade
                next_id += 1
            else:
                print(f"waiting for ticks on '{PRICES_TOPIC}'. Is prices.py running?")
                continue

            trade["event_time"] = now_ms()  # when this version happened
            producer.produce(TRADES_TOPIC, key=trade["trade_id"], value=json.dumps(trade), on_delivery=on_delivery)
            print(f"{trade['status']:<9} {trade['trade_id']} v{trade['version']} "
                  f"{trade['side']} {trade['qty']} {trade['instrument_id']} @ {trade['price']}")
    except KeyboardInterrupt:
        print("stopping, sending what's still buffered...")
    finally:
        unsent = producer.flush(5)
        consumer.close()
        print(f"done. delivered: {delivered[0]}  errors: {len(errors)}  unsent: {unsent}")


if __name__ == "__main__":
    main()
