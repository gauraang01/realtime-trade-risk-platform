"""WebSocket gateway: a snapshot from Redis, then numbered live changes from Kafka.

All clients are served by one event loop, so client state needs no locks.
"""
import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

import redis.asyncio as redis
import uvicorn
from confluent_kafka import OFFSET_END, Consumer, TopicPartition
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from entitlements import TOKENS, view

BOOTSTRAP = "localhost:9094"
TOPICS = {"trade_view": "trades", "counterparty_view": "counterparties"}  # topic -> name in a frame
INDEX_PAGE = Path(__file__).parent.parent / "ui" / "index.html"

CONFLATE_SECONDS = float(os.getenv("CONFLATE_SECONDS", "1"))
# Longer than redis_sink's 250 ms flush: a change read just before a client
# registers must have reached Redis by the time its snapshot is read.
REDIS_SETTLE_SECONDS = 0.5
REPORT_SECONDS = 10

cache = redis.Redis(host="localhost", port=6379, decode_responses=True)
clients: set["Client"] = set()


class Client:
    def __init__(self, socket: WebSocket, fund: str | None) -> None:
        self.socket = socket
        self.fund = fund
        self.seq = 0  # this connection's own counter; filtering makes Kafka offsets unusable
        self.pending = {topic: {} for topic in TOPICS}  # newest visible row per key: the conflation
        self.newest_ts = 0  # Kafka timestamp of the newest row waiting to be sent
        self.resync_wanted = False
        self.acked = asyncio.Event()  # has the client finished the last frame?
        self.acked.set()

    def offer(self, topic: str, key: str, row: dict, timestamp: int) -> None:
        visible = view(self.fund, topic, row)
        if visible is not None:
            self.pending[topic][key] = visible
            self.newest_ts = max(self.newest_ts, timestamp)

    async def send(self, frame: dict) -> None:
        self.seq += 1
        frame["seq"] = self.seq
        self.acked.clear()
        await self.socket.send_text(json.dumps(frame))


async def send_snapshot(client: Client) -> None:
    # The client is already registered, so anything changing from here on is in
    # pending and goes out after this frame. Rows are full state, so a row sent
    # twice is harmless.
    await asyncio.sleep(REDIS_SETTLE_SECONDS)
    frame = {"type": "snapshot", "scope": client.fund or "desk"}
    for topic, name in TOPICS.items():
        rows = (view(client.fund, topic, json.loads(raw)) for raw in (await cache.hgetall(topic)).values())
        frame[name] = [row for row in rows if row is not None]
    await client.send(frame)


async def send_delta(client: Client) -> None:
    frame = {name: list(client.pending[topic].values()) for topic, name in TOPICS.items()}
    frame |= {"type": "delta", "kafka_ts": client.newest_ts}
    client.pending = {topic: {} for topic in TOPICS}
    await client.send(frame)


async def send_loop(client: Client) -> None:
    """The only writer to this socket, so frames can't interleave or arrive out of order."""
    await send_snapshot(client)
    while True:
        await asyncio.sleep(CONFLATE_SECONDS)
        # Wait for the client, so a slow one doesn't pile up old frames in network
        # buffers. Meanwhile offer() keeps overwriting pending: the next frame is
        # fresh, and memory is capped at one row per key.
        await client.acked.wait()
        if client.resync_wanted:
            client.resync_wanted = False
            await send_snapshot(client)
        elif any(client.pending.values()):
            await send_delta(client)


async def receive_loop(client: Client) -> None:
    while True:
        message = await client.socket.receive_text()
        if message == "ack":
            client.acked.set()
        elif message == "resync":
            client.resync_wanted = True


def new_consumer() -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "gateway",  # required by the client, but assign() below never joins a group
        "enable.auto.commit": False,
    })
    # assign, not subscribe: with a shared group, a second gateway would take
    # partitions away from the first. Start at the end, because Redis has the past.
    partitions = []
    for topic in TOPICS:
        for partition in consumer.list_topics(topic, timeout=10).topics[topic].partitions:
            partitions.append(TopicPartition(topic, partition, OFFSET_END))
    consumer.assign(partitions)
    return consumer


async def pump_kafka() -> None:
    consumer = new_consumer()
    read, next_report = 0, time.monotonic() + REPORT_SECONDS
    try:
        while True:
            # In a thread, because consume() blocks and would freeze every client.
            for msg in await asyncio.to_thread(consumer.consume, 5000, 0.1):
                if msg.error():
                    print(f"kafka error: {msg.error()}", flush=True)
                # A tombstone is the first half of a revaluation, not a closed trade.
                elif msg.value() is not None and clients:
                    read += 1
                    key, row, timestamp = msg.key().decode(), json.loads(msg.value()), msg.timestamp()[1]
                    for client in clients:
                        client.offer(msg.topic(), key, row, timestamp)
            if time.monotonic() >= next_report:
                print(f"clients {len(clients)}, read {read} messages", flush=True)
                read, next_report = 0, time.monotonic() + REPORT_SECONDS
    finally:
        consumer.close()


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(pump_kafka())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(INDEX_PAGE)


@app.websocket("/ws")
async def websocket(socket: WebSocket) -> None:
    await socket.accept()
    token = socket.query_params.get("token")
    if token not in TOKENS:
        await socket.close(code=1008, reason="unknown token")  # 1008 = policy violation
        return

    client = Client(socket, TOKENS[token])
    clients.add(client)  # registered first: see send_snapshot
    tasks = [asyncio.create_task(send_loop(client)), asyncio.create_task(receive_loop(client))]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if not task.cancelled() and not isinstance(task.exception(), WebSocketDisconnect):
                print(f"client failed: {task.exception()!r}", flush=True)
    finally:
        clients.discard(client)
        for task in tasks:
            task.cancel()


if __name__ == "__main__":
    # Compression is off by default: on localhost, bandwidth is free, and
    # compressing a desk frame would hold up the event loop for every client.
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning",
                ws_per_message_deflate=os.getenv("WS_COMPRESSION") == "1")
