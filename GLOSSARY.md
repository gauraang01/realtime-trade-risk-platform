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
- **Flash P&L**: a fast, unverified estimate, available during or just after the trading day. This dashboard shows flash P&L.
- **Official P&L**: the checked, signed-off number, usually produced the next day. It goes into the books and regulatory reports.
- **Product control**: the finance team that verifies P&L independently of the traders.

### Risk
- **Daily volatility**: the typical size of a one-day percentage price move (one standard deviation). 2% means a typical day moves about 2%.
- **VaR (Value at Risk)**: a loss level you do not expect to exceed over a period at a stated confidence. "1-day 95% VaR of $3,300" means about 1 day in 20 should be worse than -$3,300.
- **1.65**: the multiplier that turns one standard deviation into a 95% one-sided confidence level, assuming normally distributed returns.
- **Risk engine**: the system that calculates risk. In real banks it is usually slow and runs separately from the trading systems.
- **As-of time**: the moment a calculated number was true.
- **Risk stale**: a risk number that can no longer be trusted. In this project: the trade changed after risk was calculated, or the risk is older than 2 minutes.

### Credit
- **Credit exposure**: how much we could lose if a counterparty fails. In this project, simplified to the sum of `|quantity × live price|` across its live trades.
- **Credit limit**: the maximum exposure the bank will accept to one counterparty.
- **Limit breach**: exposure has gone above the limit. It can happen with no new trade, purely because prices moved.

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

### Time
- **Event time**: when something actually happened, as opposed to when a system processed it.
- **Trade time**: when a trade was first booked. Later amendments keep it unchanged.

### Simulation
- **Random walk**: a series where each value is the previous one plus a random step, a simple stand-in for price movement.

### Docker
- **Compose profile**: a label that lets you start only a chosen group of services.
