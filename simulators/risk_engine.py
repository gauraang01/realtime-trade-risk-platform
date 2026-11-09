"""Every --interval seconds, publish one risk result per live trade to `risk_results`."""
import argparse
import json
import time

from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, Consumer, TopicPartition

from common import BOOTSTRAP, INSTRUMENTS, make_producer, now_ms

TRADES_TOPIC = "trades_latest"
PRICES_TOPIC = "prices"
RISK_TOPIC = "risk_results"

# Roughly the 95% point of a normal distribution: a bad day, not the worst day.
CONFIDENCE_MULTIPLIER = 1.65


def follow(topic: str, start: int) -> Consumer:
    """Read every partition directly. No consumer group, because we commit nothing."""
    consumer = Consumer(
        {"bootstrap.servers": BOOTSTRAP, "group.id": "risk-engine", "enable.auto.commit": False}
    )
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    consumer.assign([TopicPartition(topic, p, start) for p in partitions])
    return consumer


def drain(consumer: Consumer) -> list:
    """Return whatever has arrived so far. Anything later waits for the next run."""
    messages = []
    while (msg := consumer.poll(0)) is not None:
        if msg.error():
            print(f"consumer error: {msg.error()}")
        else:
            messages.append(msg)
    return messages


def update_trades(trades: dict, messages: list) -> None:
    for msg in messages:
        trade_id = msg.key().decode()
        if msg.value() is None:  # a tombstone: this trade no longer exists
            trades.pop(trade_id, None)
        else:
            trades[trade_id] = json.loads(msg.value())


def update_prices(prices: dict, messages: list) -> None:
    for msg in messages:
        tick = json.loads(msg.value())
        prices[tick["instrument_id"]] = tick["price"]


def calculate(trades: dict, prices: dict, as_of: int) -> list:
    """Price every live trade from one snapshot, so a whole run shares one as_of."""
    results = []
    for trade_id, trade in trades.items():
        price = prices.get(trade["instrument_id"])
        if trade["status"] == "CANCELLED" or price is None:
            continue
        notional = abs(trade["qty"] * price)
        daily_vol = INSTRUMENTS[trade["instrument_id"]]["daily_vol"]
        results.append(
            {
                "trade_id": trade_id,
                "trade_version": trade["version"],  # which version this was calculated from
                "instrument_id": trade["instrument_id"],
                "notional": round(notional, 2),
                "risk": round(notional * daily_vol * CONFIDENCE_MULTIPLIER, 2),
                "as_of": as_of,
            }
        )
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=30, help="seconds between risk runs")
    args = parser.parse_args()

    # Trades from the start: the compacted topic is the full current book.
    # Prices from now: only the newest price matters.
    trades_consumer = follow(TRADES_TOPIC, OFFSET_BEGINNING)
    prices_consumer = follow(PRICES_TOPIC, OFFSET_END)
    producer = make_producer()
    trades, prices = {}, {}

    try:
        while True:
            time.sleep(args.interval)
            update_trades(trades, drain(trades_consumer))
            update_prices(prices, drain(prices_consumer))

            results = calculate(trades, prices, now_ms())
            for result in results:
                producer.produce(RISK_TOPIC, key=result["trade_id"], value=json.dumps(result))
                producer.poll(0)
            unsent = producer.flush(10)
            print(f"risk run: {len(results)} results from {len(trades)} trades, unsent: {unsent}")
    except KeyboardInterrupt:
        print("stopping")
    finally:
        trades_consumer.close()
        prices_consumer.close()


if __name__ == "__main__":
    main()
