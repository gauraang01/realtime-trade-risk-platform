"""Connect to the gateway like a browser, then check the result against a fresh snapshot.

Usage: ws_check.py [--token desk] [--seconds 30] [--drop-after 10] [--drops 1]
                   [--no-resync] [--read-delay 0] [--show-row]
"""
import argparse
import asyncio
import json
import time

import websockets

URL = "ws://127.0.0.1:8000/ws?token={}"


class Screen:
    """The same rules as the browser: replace on a snapshot, apply deltas, watch for gaps."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.trades, self.counterparties = {}, {}
        self.last_seq, self.resyncing = 0, False
        self.frames = self.delta_rows = self.bytes = self.gaps = self.snapshots = self.dropped = 0
        self.ages = []
        self.drop_at = time.monotonic() + args.drop_after if args.drop_after else None

    def apply(self, frame: dict) -> None:
        for row in frame["trades"]:
            self.trades[row["trade_id"]] = row
        for row in frame["counterparties"]:
            self.counterparties[row["counterparty_id"]] = row

    async def on_frame(self, socket, raw: str) -> None:
        frame = json.loads(raw)
        self.frames += 1
        self.bytes += len(raw)
        if frame["type"] == "snapshot":
            self.snapshots += 1
            self.trades, self.counterparties = {}, {}
            self.apply(frame)
            self.last_seq, self.resyncing = frame["seq"], False
            return
        self.ages.append(time.time() * 1000 - frame["kafka_ts"])
        self.delta_rows += len(frame["trades"])
        if self.drop_at and time.monotonic() >= self.drop_at and self.dropped < self.args.drops:
            self.dropped += 1
            return
        if self.resyncing:
            return
        if frame["seq"] != self.last_seq + 1:
            self.gaps += 1
            if not self.args.no_resync:
                self.resyncing = True
                await socket.send("resync")
                return
        self.last_seq = frame["seq"]
        self.apply(frame)

    async def run(self, stop: asyncio.Event) -> None:
        # A slow reader holds only one frame here, so back-pressure reaches the gateway.
        queue = 1 if self.args.read_delay else 16
        async with websockets.connect(URL.format(self.args.token), max_size=None, compression=None, max_queue=queue) as socket:
            while not stop.is_set():
                try:
                    raw = await asyncio.wait_for(socket.recv(), 0.5)
                except asyncio.TimeoutError:
                    continue
                await self.on_frame(socket, raw)
                if self.args.read_delay:
                    await asyncio.sleep(self.args.read_delay)
                await socket.send("ack")


async def fresh_snapshot(token: str) -> dict:
    async with websockets.connect(URL.format(token), max_size=None, compression=None) as socket:
        return json.loads(await socket.recv())


async def main(args: argparse.Namespace) -> None:
    screen, stop = Screen(args), asyncio.Event()
    start = time.monotonic()
    task = asyncio.create_task(screen.run(stop))
    await asyncio.sleep(args.seconds)
    truth = await fresh_snapshot(args.token)
    await asyncio.sleep(3)  # let the screen receive changes the fresh snapshot already includes
    elapsed = time.monotonic() - start
    stop.set()
    await task

    wrong = [t["trade_id"] for t in truth["trades"]
             if t["trade_id"] not in screen.trades or screen.trades[t["trade_id"]]["version"] < t["version"]]
    ages = sorted(screen.ages) or [0]
    deltas = max(len(screen.ages), 1)
    print(f"frames {screen.frames} ({screen.snapshots} snapshots), gaps {screen.gaps}, dropped on purpose {screen.dropped}")
    print(f"trades on screen {len(screen.trades)}, counterparties {len(screen.counterparties)}, "
          f"rows per delta {screen.delta_rows / deltas:.0f}, {screen.bytes / elapsed / 1e6:.2f} MB/s")
    print(f"data age when a delta arrives: median {ages[len(ages) // 2]:.0f} ms, worst {ages[-1]:.0f} ms")
    print(f"against a fresh snapshot: {len(wrong)} trades missing or on an old version {wrong[:5]}")
    if args.show_row and screen.trades:
        print(json.dumps(next(iter(screen.trades.values()))))
        print(json.dumps(list(screen.counterparties.values())))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", default="desk")
    parser.add_argument("--seconds", type=float, default=30)
    parser.add_argument("--drop-after", type=float, default=0, help="start dropping deltas after this many seconds")
    parser.add_argument("--drops", type=int, default=1, help="how many deltas to drop")
    parser.add_argument("--no-resync", action="store_true", help="note gaps but carry on, as a client without resync would")
    parser.add_argument("--read-delay", type=float, default=0, help="sleep after each frame, to act like a slow client")
    parser.add_argument("--show-row", action="store_true", help="print one trade and the counterparties")
    asyncio.run(main(parser.parse_args()))
