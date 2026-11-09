# Glossary

One line per term, in plain English. Added to at the end of every sprint.

## Business terms

### Who and what
- **Front office**: the people who deal directly with markets and clients (traders, sales), and the systems they use.
- **Trading desk**: a team of traders that specialises in one market or product.
- **Book**: a named bucket of trades that a desk manages and measures together, like a folder with its own profit and risk.
- **Counterparty**: the other side of a trade. In this project, a hedge fund.
- **Hedge fund**: a private investment firm; here, a client that trades stocks with our desk.
- **Instrument**: the thing being traded. In this project, a stock.
- **Reference data**: slow-changing facts that other data points to, such as instrument names and credit limits.

### Trades and positions
- **Trade**: an agreement to exchange a quantity of an instrument at a price.
- **Long / short**: long means you own it and gain when the price rises; short means you owe it and gain when the price falls.
- **Position**: your net holding, summed across live trades (here, per book and instrument). +500 is long, -500 is short.
- **Net quantity**: buys minus sells for a position. Positive is long, negative is short, zero is flat.
- **Position cost**: the sum of `signed quantity × trade price`. Negative when the desk has taken in more cash than it paid. Position P&L is `net quantity × live price - cost`.
- **Amendment**: a correction to an existing trade. It creates a new version that replaces the old one and does not add a second trade.
- **Cancellation**: the trade is void and drops out of positions, P&L and exposure.
- **Trade version**: a counter that goes up with each change to a trade, so systems can tell which state is newest.
- **Tick**: one price update for one instrument.
- **Blotter**: the live list of a desk's trades.

### Profit
- **Notional**: the size of a trade in money: `|quantity × price|`. It measures size, not profit or loss.
- **Mark-to-market (MTM)**: valuing a holding at today's market price instead of the price you paid.
- **P&L (profit and loss)**: how much money you have made or lost.
- **MTM P&L**: `signed quantity × (current price - trade price)`, from the desk's point of view.
- **Unrealised P&L**: gain or loss on paper, because the position is still open.
- **Realised P&L**: gain or loss locked in by closing the position.
- **Slippage**: the gap between the price you expected and the price you actually traded at. Comparing a trade's price with the market price at that moment is a simple check of it.
- **Flash P&L**: a fast, unverified estimate, available during or just after the trading day. This dashboard shows flash P&L.
- **Official P&L**: the checked, signed-off number, usually produced the next day. It goes into the books and regulatory reports.
- **Product control**: the finance team that verifies P&L independently of the traders.
- **P&L explain**: breaking a day's P&L into its causes, such as price moves and new trades, so the number can be justified.

### Risk
- **Daily volatility**: the typical size of a one-day percentage price move (one standard deviation). 2% means a typical day moves about 2%.
- **VaR (Value at Risk)**: a loss level you do not expect to exceed over a period at a stated confidence. "1-day 95% VaR of $3,300" means about 1 day in 20 should be worse than -$3,300.
- **1.65**: the multiplier that turns one standard deviation into a 95% one-sided confidence level, assuming normally distributed returns.
- **Risk engine**: the system that calculates risk. In real banks it is usually slow and runs separately from the trading systems.
- **As-of time**: the moment a calculated number was true.
- **Risk stale**: a risk number that can no longer be trusted. In this project: the trade changed after risk was calculated, or the risk is older than 2 minutes.
- **Risk run**: one pass of the risk engine over the whole book, with every result stamped with the same as-of time.
- **`trade_version` stamp**: the trade version a risk result was calculated from, compared with the current version to detect stale risk.

### Credit
- **Credit exposure**: how much we could lose if a counterparty fails. In this project, simplified to the sum of `|quantity × live price|` across its live trades.
- **Credit limit**: the maximum exposure the bank will accept to one counterparty.
- **Limit breach**: exposure has gone above the limit. It can happen with no new trade, purely because prices moved.
- **Pre-trade limit check**: a check that can block a trade before it is booked. It must be synchronous, unlike this project's after-the-fact alert.
- **Post-trade monitoring**: watching exposure after trades are booked and alerting people, which allows a few seconds of delay.
- **Valid-from date**: a business column saying when a reference value took effect, independent of when a system recorded it.
- **Audit trail**: a kept record of past values, so a bank can show regulators what it knew and when.
- **Trader blotter snapshot**: the full current list of trades shown when a screen first opens, before live updates begin.
- **Entitlements**: the rules for who may see which data. A client should see only its own trades.
- **Client view**: the same trade seen from the client's side, so the side and the P&L sign are reversed.
- **Credit line used**: how much of its credit limit a counterparty has used, shown as a percentage.

## Technical terms

### Architecture
- **Stream processing**: computing results continuously as events arrive, instead of on a schedule over stored data.
- **Kafka**: a durable, replayable log of events that producers write to and consumers read from independently.
- **Topic**: a named stream of events in Kafka.
- **Key**: the field that decides a message's partition. Messages with the same key stay in order relative to each other.
- **Compacted topic**: a topic that eventually keeps only the latest message per key, so it acts like a table.
- **Change data capture (CDC)**: turning every committed database change into an event.
- **WAL (write-ahead log)**: Postgres's own ordered record of every change, which CDC tools read.
- **Debezium**: an open-source CDC tool that publishes database changes to Kafka.
- **Dual-write problem**: when an app writes to two systems separately, one can succeed and the other fail, leaving them inconsistent.
- **Flink**: a stream processor that keeps state, handles event time and recovers from failures.
- **State**: what a streaming job remembers between events, such as the latest price per instrument.
- **Checkpoint**: a periodic consistent snapshot of a job's state and read positions, used to recover after a failure.
- **Upsert / changelog stream**: a stream where a new message updates or deletes the row with that key, instead of adding a row.
- **Fan-out**: one input event causing many outputs, such as one tick revaluing every trade in that stock.
- **Conflation**: keeping only the newest update when updates arrive faster than a consumer needs them.
- **OLTP**: database work made of many small reads and writes of individual rows, such as saving one order.
- **OLAP**: database work made of queries that scan and summarise many rows, such as total exposure per day.
- **Columnar storage**: storing each column together on disk, so a query reads only the columns it needs.
- **ClickHouse**: a columnar database built for fast analytical queries over large history.
- **Table format**: a set of rules for organising files as a table. It stores data but doesn't run queries itself.
- **Small files problem**: frequent writes create many tiny files, which slows reads until they are merged.
- **Redis**: an in-memory key-value store, used here to hold the latest snapshot.
- **DynamoDB**: AWS's managed key-value database; fast and durable for lookups by key, but not built for ad-hoc analytics.
- **Hot partition**: one partition receiving far more traffic than the others, which throttles it even when the system as a whole has spare capacity.
- **WebSocket**: a connection that stays open so a server can push updates to a browser.
- **Snapshot + deltas**: send the full current state once, then only the changes.
- **Sequence number**: an increasing counter on each update, so a client can spot a missed message.
- **Spark Structured Streaming**: Spark's streaming engine, which by default processes small batches repeatedly.
- **Iceberg**: an open table format that adds tables, snapshots and history on top of files in object storage.
- **MinIO**: an S3-compatible object store that runs locally.
- **REST catalog**: a service that tracks which Iceberg tables exist and their current state.

### Kafka
- **Partition**: one ordered log within a topic. Kafka guarantees order only within a partition.
- **Offset**: a message's position in its partition. Offsets are never reused or changed.
- **Broker**: the Kafka server that stores partitions and serves reads and writes.
- **Controller**: the Kafka role that manages cluster metadata, such as topics and partition leaders.
- **KRaft**: Kafka's built-in metadata system, which replaced ZooKeeper.
- **Bootstrap server**: the first address a client contacts, only to discover the cluster.
- **Advertised listener**: the address a broker tells clients to use for all later connections.
- **Replication factor**: how many copies of each partition the cluster keeps.
- **ISR (in-sync replicas)**: the copies of a partition that are fully caught up with its leader.
- **Consumer group**: a team of consumers sharing a topic's work. Each partition has one reader per group.
- **Committed offset**: a group's saved position in a partition, stored in `__consumer_offsets`.
- **Log-end offset**: the position where the next message in a partition will be written.
- **Consumer lag**: log-end offset minus committed offset, or how far behind a group is.
- **`auto.offset.reset`**: where a group starts when it has no saved offset. It's ignored once one exists.
- **Rebalance**: redistributing a group's partitions when consumers join or leave.
- **`session.timeout.ms`**: how long the broker waits without heartbeats before declaring a consumer dead (45 s by default).
- **At-least-once delivery**: no messages are lost, but some may be processed twice after a failure.
- **Retention**: deleting messages once they pass an age or size limit.
- **Segment**: one file of a partition's log on disk. Only closed segments get cleaned.
- **Active segment**: the segment currently being written to. It's never compacted.
- **Tombstone**: a message with a key and a null value, which tells compaction to delete that key.
- **Partitioner**: the client code that turns a key into a partition number. Java clients use murmur2 by default; librdkafka clients use CRC32 unless configured otherwise.
- **Key skew**: some partitions get more traffic than others because of how keys hash or how busy each key is.
- **`acks`**: how many replicas must have a message before the broker confirms it. `all` means every in-sync replica.
- **Idempotent producer**: a producer whose retries can't create duplicates or reorder messages, because the broker tracks sequence numbers.
- **Batching (`linger.ms`)**: the producer waits briefly to group messages into one request, trading a little latency for throughput.
- **Delivery callback**: a function that reports whether each message reached the broker. `produce()` alone only puts it in a local buffer.

### CDC
- **Logical decoding**: Postgres turning its WAL into row-level changes. It needs `wal_level=logical`.
- **`pgoutput`**: the logical decoding plugin built into Postgres.
- **Publication**: the list of tables whose changes Postgres sends to WAL readers.
- **Replication slot**: a bookmark Postgres keeps for a WAL reader. WAL after it can't be removed, so an abandoned slot can fill the disk.
- **LSN (log sequence number)**: a position in the WAL.
- **Replica identity**: how much of the old row Postgres writes to the WAL. `FULL` writes all of it.
- **CDC snapshot**: reading every existing row once (`op=r`) before following the WAL. It only runs when there's no saved position.
- **Debezium envelope**: the message shape holding `before`, `after`, `op` (r, c, u, d), `source` and `ts_ms`.
- **Kafka Connect**: a framework that moves data in and out of Kafka, using plugins such as Debezium.
- **Connect worker / connector / task**: the host process, the job configuration, and the thread doing the reading.
- **Connect offsets**: a source connector's saved position, stored in a Kafka topic (`connect-offsets` here).
- **`decimal.handling.mode`**: how Debezium writes numeric columns. `precise` is base64 bytes, `double` is floating point, `string` is exact and readable.
- **Retraction**: taking an old value back out of a result before adding the new one.
- **`delete.retention.ms`**: how long compaction keeps a tombstone, so slow consumers can still see the delete (24 hours by default).
- **`min.cleanable.dirty.ratio`**: how much of a log must be uncompacted before the cleaner runs.

### Flink
- **JobManager**: the Flink process that plans jobs, assigns work and coordinates recovery. It doesn't process records.
- **TaskManager**: the Flink worker process that processes records and holds running state.
- **Task slot**: one share of a TaskManager's capacity. A job can't start without enough free slots.
- **Slot sharing**: different operators of the same job running in one slot. Separate jobs never share a slot.
- **Parallelism**: how many copies of an operator run at once.
- **Subtask**: one of those parallel copies.
- **Operator chaining**: running consecutive operators in one thread so records don't need to be handed between them.
- **Hash exchange**: sending records between operators by key, so the same key always reaches the same subtask. `GROUP BY` needs it.
- **Connector**: the plugin that lets Flink read or write an outside system. It must be a JAR in `lib/`.
- **Dynamic table**: Flink's view of a stream as a table whose contents keep changing. It stores no data itself.
- **Metadata column**: a table column that exposes details of the source message, such as its Kafka partition or offset.
- **Continuous query**: a query that never finishes and keeps updating its result as data arrives.
- **`+I` / `-U` / `+U`**: changelog row types: a new row, the old value being withdrawn, and the new value replacing it.
- **`-D`**: changelog row type meaning this row is deleted.
- **Append stream**: a stream where every message is a new fact and nothing is taken back.
- **Changelog stream**: a stream of instructions (insert, withdraw, update, delete) for keeping a table correct.
- **Deduplication (`ROW_NUMBER`, `rn = 1`)**: Flink's pattern for keeping one best row per key, emitting a change only when a better row arrives.
- **`upsert-kafka`**: the connector that writes a changelog as key/value messages, and reads a topic as "the newest message for a key replaces the older ones".
- **`PRIMARY KEY ... NOT ENFORCED`**: names the key column for Flink without Flink checking that it's unique.
- **Bounded source**: a source that stops at a fixed point, so its job finishes. `scan.bounded.mode` makes a Kafka source bounded.
- **Late message**: a message that arrives after a newer one for the same key.
- **State backend**: where a running job keeps its state. The default `hashmap` uses the Java heap; `rocksdb` uses managed memory and disk.
- **RocksDB**: an embedded key-value store, used for state too large or too long-lived for the Java heap.
- **Task heap**: the Java heap memory for a job's own objects, including state under the default `hashmap` backend.
- **Managed memory**: memory Flink controls outside the Java heap, used by RocksDB and batch operators.
- **State TTL**: a time limit after which unused state is deleted, so state doesn't grow forever.
- **Garbage collection pause**: time when Java stops or slows a program to reclaim unused objects. Large heaps make pauses worse.
- **Checkpoint storage**: the durable location for checkpoints. It must outlive any single process.
- **Exactly-once (state)**: after a recovery, each record has affected Flink's state once. End-to-end exactly-once also needs a transactional or idempotent sink.
- **Heartbeat timeout**: how long the JobManager waits to hear from a TaskManager before declaring it dead (50 s by default).
- **Restart strategy**: what Flink does when a job fails. With checkpointing off, the default is not to restart.
- **Enrichment**: adding descriptive fields to an event by joining it with reference data.
- **Regular join**: a stream join that remembers both sides and re-emits results when either side changes.
- **Temporal join**: a join that uses the version of a table that was valid at each event's time, and never revises the result.
- **Versioned table**: a table with a primary key and an event-time column, so Flink can tell which version applied at a given moment.
- **Lookup join**: a join that queries an external database for each event as it's processed. It keeps no state, but replaying gives today's answers.
- **Broadcast state**: a DataStream pattern that gives every parallel task a full copy of a small reference table, so joins need no network shuffle.
- **`STATE_TTL` hint**: a per-query setting that gives each side of a join its own state TTL. `0` means never expire.
- **`ChangelogNormalize`**: the Flink operator that remembers each key's latest row, so an upsert source can produce `-U` retractions.
- **`SinkUpsertMaterializer`**: an operator Flink adds before an upsert sink when it can't prove updates arrive in order. It keeps state per key. Shown as `SinkMaterializer` in the Flink UI.
- **Statement set**: several `INSERT`s submitted as one job, which can share slots.
- **Mini-batch**: operators buffer input for a short time and process only the final state per key. It conflates updates and merges an amend's retract and add, at the cost of a little latency.
- **Sink buffer flush**: `sink.buffer-flush.*` on `upsert-kafka`, which holds messages briefly and writes only the newest per key. It removes most delete-then-insert tombstones.
- **Tombstone block**: when a flush falls between one tick's deletes and inserts, every trade on that instrument briefly reads as deleted.
- **Version staleness vs age staleness**: the first asks "has the trade changed since this result?", the second "how old is this result?". Neither check can replace the other.
- **`print` connector**: a sink that writes rows to the TaskManager's log, for experiments.
- **`blackhole` connector**: a sink that discards every row, used to measure a job's speed.
- **`JDBC` connector**: Flink's connector for querying databases such as Postgres, used here for lookup joins.
- **JobManager High Availability**: storing job definitions and a pointer to the latest checkpoint outside the JobManager, so a replacement can resume running jobs.

### Time
- **Event time**: when something actually happened, as opposed to when a system processed it.
- **Trade time**: when a trade was first booked. Later amendments keep it unchanged.
- **Processing time**: the clock of the machine handling a record. `PROCTIME()` in Flink SQL.
- **Watermark**: a marker saying "no events older than this are still expected", which lets event-time operators make progress.
- **Idle source**: a source sending nothing for a set time, which Flink then stops waiting for when working out watermarks.
- **Capture time**: when CDC recorded a row, which isn't the same as when that row became valid in business terms.
- **Candle (OHLC)**: one bar on a price chart, summarising a time period with its open (first), high, low and close (last) prices.
- **Tumbling window**: fixed-size, back-to-back time buckets that don't overlap, so each event lands in exactly one.
- **Window table function (TVF)**: Flink SQL's way to assign rows to windows, such as `TUMBLE(...)`. It adds `window_start` and `window_end`.
- **Watermark delay**: how far the watermark trails the newest event time seen. Longer catches more disorder but gives results later.
- **Out-of-order event**: an event that arrives after one with a later timestamp. It still counts if its window is open.
- **Late event**: an event whose window has already closed. By default it is dropped without any error.
- **Allowed lateness**: keeping a closed window's state for longer, so a late event can produce a corrected result.
- **Per-partition watermark**: each partition tracks its own watermark, and the source reports the lowest of them.
- **Session window**: a per-key window that stays open while events keep arriving and closes after a set gap of silence.
- **Timer**: a callback that fires at a set time with no incoming event, the only way to react to something not happening.
- **`KeyedProcessFunction`**: Flink's low-level API for per-key state and hand-registered timers, for logic SQL can't express.
- **Debounce**: acting on a signal only after it has held steady for a while, so brief glitches are ignored.
- **Level vs edge trigger**: a level trigger acts for as long as a condition holds; an edge trigger acts once, when it changes.
- **`LAG`**: a SQL function that returns a column's value from the previous row for the same key.
- **Aggregate before fan-out**: totalling slow-changing data first, so a fast-changing join touches a few rows instead of every record.

### Serving
- **Serving layer**: the stores that answer questions from screens and people, fed from the stream.
- **Part (ClickHouse)**: an immutable batch of rows on disk; every insert creates one.
- **Merge (ClickHouse)**: background work that combines parts into larger ones, at a time ClickHouse chooses.
- **`MergeTree`**: ClickHouse's basic table engine, which keeps every row inserted.
- **`ReplacingMergeTree`**: a table engine that keeps one row per sorting key during merges.
- **Version column**: the column that decides which row a `ReplacingMergeTree` keeps; the highest value wins.
- **`FINAL`**: a query modifier that removes outdated rows at read time, for merges that haven't happened yet.
- **`ORDER BY` (ClickHouse table)**: the on-disk sort order, which in a `ReplacingMergeTree` also defines what counts as the same row.
- **Kafka table engine**: a ClickHouse table that is really a Kafka consumer and stores nothing.
- **Materialized view (ClickHouse)**: an insert trigger that transforms each new batch and writes it to another table.
- **Virtual column**: metadata such as `_offset` that ClickHouse exposes for each Kafka message.
- **`argMax(a, b)`**: an aggregate that returns `a` from the row with the largest `b`.
- **Point-in-time query**: asking what a value was at a past moment, which needs every old value kept.
- **`TTL` (ClickHouse)**: a rule that deletes rows automatically once they reach a given age.
- **Redis hash**: one Redis key holding many field-value pairs, here one field per trade.
- **`HGETALL`**: a Redis command that returns every field in a hash, used here to read a full snapshot.
- **`noeviction`**: a Redis setting that rejects writes when memory is full instead of deleting keys.
- **Cache vs source of truth**: a cache can be wiped and rebuilt, and the source of truth, here Kafka, can't be lost.
- **Redis pipeline**: sending many commands in one network round trip, which makes bulk writes far faster.
- **`HSCAN`**: a Redis command that reads a hash in chunks, so a large read doesn't block other clients.
- **Idempotent write**: a write that leaves the same result however many times it's applied.
- **Page cache**: file data the operating system keeps in memory; it counts in `docker stats` but can be reclaimed.
- **Serving lag**: how far a serving store is behind the stream, here 1-8 seconds for ClickHouse because it writes in batches.

### Gateway and UI
- **Gateway**: the server between the data pipeline and the screens, which decides what each screen receives and how fast.
- **FastAPI**: a Python web framework, used here for the page and the WebSocket endpoint.
- **uvicorn**: the server process that runs a FastAPI app.
- **Event loop**: one thread that switches between many waiting tasks, so client state needs no locks but one slow step delays everyone.
- **Delta**: a message holding only the rows that changed since the last message.
- **Full-state delta**: a delta that carries whole rows, so applying one twice is harmless and a later one repairs an earlier loss.
- **Resync**: a client discarding its state and asking for a new snapshot after spotting a gap.
- **Self-healing stream**: a stream that re-sends every row often, so a lost update is overwritten soon even without a resync.
- **Slow consumer**: a client that can't read as fast as the server sends.
- **Back-pressure**: a slow receiver making the sender wait, instead of letting data pile up.
- **Application-level ack**: a client message saying it has finished a frame, so the server sends the next one only then.
- **Viewport subscription**: sending a client only the rows currently on its screen.
- **Register before snapshot**: start collecting a client's changes before reading its snapshot, so nothing falls between the two.
- **Allow-list**: naming exactly what may pass, so anything new stays blocked until someone adds it.
- **`permessage-deflate`**: WebSocket compression, which saves bandwidth but costs CPU per frame.
- **Close code 1008**: the WebSocket code for "policy violation", used here to refuse an unknown token.

### Simulation
- **Random walk**: a series where each value is the previous one plus a random step, a simple stand-in for price movement.

### Docker
- **Compose profile**: a label that lets you start only a chosen group of services.
