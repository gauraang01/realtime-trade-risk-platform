"""Send one trade, to see what each join type does with it.

The trade is sent as CANCELLED so it can never count towards exposure in later
sprints. Joins ignore status, so it still shows up in their output.
"""
import argparse
import json
import time

from common import make_producer, now_ms


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--counterparty", default="CP-2")
    args = parser.parse_args()

    trade_id = f"T-JOIN-{int(time.time()) % 1_000_000}"
    now = now_ms()
    trade = {
        "trade_id": trade_id, "version": 1, "status": "CANCELLED",
        "instrument_id": "AAPL", "counterparty_id": args.counterparty, "book_id": "BOOK-1",
        "side": "BUY", "qty": 100, "price": 190.00,
        "trade_time": now, "event_time": now,
    }

    producer = make_producer()
    producer.produce("trades", key=trade_id, value=json.dumps(trade))
    if producer.flush(10) != 0:
        raise RuntimeError("message was not delivered")
    print(f"sent {trade_id} for {args.counterparty}")


if __name__ == "__main__":
    main()
