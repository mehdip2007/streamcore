# StreamCore Kafka Interview Questions & Answers

> Comprehensive interview questions with detailed answers based on StreamCore's Kafka producer/consumer implementation

---

## Table of Contents

1. [Kafka Fundamentals](#1-kafka-fundamentals)
2. [Producer Deep Dive](#2-producer-deep-dive)
3. [Consumer Deep Dive](#3-consumer-deep-dive)
4. [Configuration & Settings](#4-configuration--settings)
5. [Architecture & Design Patterns](#5-architecture--design-patterns)
6. [Error Handling & Debugging](#6-error-handling--debugging)
7. [Performance & Scaling](#7-performance--scaling)
8. [Scenario-Based & Code Review](#8-scenario-based--code-review)
9. [Advanced Kafka Concepts](#9-advanced-kafka-concepts)

---

## 1. Kafka Fundamentals

### Q1.1: What is Apache Kafka and what problem does it solve?

**A:** Kafka is a **distributed event streaming platform** that provides:
- **Pub-sub**: Multiple consumers can subscribe to topics
- **Queue**: Messages persist until consumed (unlike traditional pub-sub)
- **Replayability**: Consumers can re-read past messages
- **Scalability**: Horizontally scalable through partitioning
- **Fault tolerance**: Data replicated across brokers

**Problems solved:** Decoupling producers/consumers, handling high-volume real-time data, ensuring data durability

---

### Q1.2: Difference between message queue and publish-subscribe?

**A:**

| Feature | Message Queue | Publish-Subscribe | Kafka |
|---------|--------------|-----------------|-------|
| Consumers | 1 (competing) | Many | Many |
| Retention | Deleted after consume | Deleted after consume | **Retained** |
| Coupling | Tight | Loose | **Decoupled** |
| Kafka fits | ❌ | ❌ | ✅ **Both** |

---

### Q1.3: Main components of Kafka architecture?

**A:**
1. **Broker**: Kafka server (stores data, serves requests)
2. **Topic**: Logical channel (feed of messages)
3. **Partition**: Ordered, immutable sequence within a topic
4. **Offset**: Position of message within partition
5. **Producer**: Publishes data to topics
6. **Consumer**: Subscribes to topics, processes data
7. **Consumer Group**: Set of consumers sharing work
8. **Zookeeper**: Coordination (in older versions)
9. **ISR**: In-Sync Replicas (replicas caught up with leader)

---

### Q1.4: StreamCore's Kafka topics?

**A:**
- `streamcore.video.views` → `VideoViewEvent` (video start)
- `streamcore.video.watch_progress` → `WatchProgressEvent` (every ~10s during playback)

---

### Q1.5: What is a Kafka partition and why partition topics?

**A:** A partition is an **ordered, immutable sequence** of messages within a topic.

**Why partition:**
- **Parallelism**: Multiple consumers read different partitions simultaneously
- **Scalability**: Distribute data across brokers
- **Throughput**: Parallel writes increase write capacity
- **Ordering**: Messages ordered within each partition

---

### Q1.6: How does Kafka achieve high throughput?

**A:**
1. **Sequential I/O**: Append-only log (10x faster than random I/O)
2. **Batching**: Group messages (reduces network round trips)
3. **Zero-copy**: Uses sendfile() system call
4. **Compression**: snappy/gzip/lz4/zstd
5. **Partitioning**: Parallel writes/reads
6. **Pull-based**: Consumers pull at their own rate
7. **Page cache**: Relies on OS cache

---

### Q1.7: Relationship between Kafka, Zookeeper, and Kafka UI in StreamCore?

**A:**
- **Zookeeper**: Manages cluster metadata (broker states, topic configs, offsets)
- **Kafka**: Depends on Zookeeper for coordination; stores data
- **Kafka UI**: Connects to Kafka for monitoring; depends on Kafka being healthy

**Flow:** Zookeeper ← Kafka ← Kafka UI (monitoring)

---

### Q1.8: What happens when a new consumer joins a consumer group?

**A:** **Rebalancing** occurs:
1. Consumer sends JoinGroup request to group coordinator
2. Coordinator signals all consumers to stop processing
3. All consumers commit offsets
4. Coordinator recalculates partition assignments
5. Each consumer receives new assignment
6. Consumers resume processing from committed offsets

**Cost:** Processing pause, potential duplicates if offsets not committed

---

### Q1.9: Difference between broker, topic, partition, offset?

**A:**

| Concept | Definition | Analogy |
|---------|------------|---------|
| Broker | Server instance | Library building |
| Topic | Logical channel | Book genre |
| Partition | Physical division (ordered sequence) | Book chapter |
| Offset | Message position within partition | Page number |

---

### Q1.10: Why two listeners in StreamCore's docker-compose (PLAINTEXT and PLAINTEXT_HOST)?

**A:**
- `PLAINTEXT://kafka:29092` → Internal Docker communication
- `PLAINTEXT_HOST://localhost:9092` → Host machine communication

Allows both containers and host to connect to the same Kafka instance.

---

### Q1.11: Kafka's log segmentation?

**A:** Kafka stores data in **log segments** (not single file). Each partition has:
- `.log` file: Actual message data (binary format)
- `.index` file: Offset → byte position mapping (O(1) lookups)
- `.timeindex` file: Timestamp → offset mapping

**Configs:**
- `log.segment.bytes`: Max segment size (default 1GB)
- `log.retention.ms`: Retention time (default 7 days)

---

### Q1.12: Kafka's ISR mechanism and relation to acks?

**A:** ISR (In-Sync Replica) = set of replicas caught up with the leader.

**How it works:**
- Followers pull data from leader
- A follower is in-sync if:
  - Replicated all messages up to leader's last stable offset
  - Sent fetch request within `replica.lag.time.max.ms` (10s)
- Leader maintains ISR list in Zookeeper

**Relation to acks:**
- `acks=0`: No ack (fire-and-forget)
- `acks=1`: Leader ack only
- `acks=all`: **All ISR members must ack** → full durability

---

### Q1.13: auto.offset.reset=earliest vs latest?

**A:**

| Value | Behavior | Use Case |
|-------|----------|----------|
| `earliest` | Read from beginning | Replay all data, new consumer groups |
| `latest` | Read only new messages | Real-time consumers, skip history |
| `none` | Throw exception if no offset | Strict mode |

**StreamCore uses `earliest`** to process all events from startup.

---

---

## 2. Producer Deep Dive

### Q2.1: Why key by user_id in StreamCore?

**A:** Ensures **ordered delivery per user**.

Without keying: Same user's events could go to different partitions → processed out of order.

With `user_id` key: All events for a user go to same partition → ordered processing.

**Tradeoff:** Can cause data skew if some users generate many more events.

---

### Q2.2: Why acks=all in StreamCore?

**A:** Provides **strongest durability guarantee**.

- `acks=all`: Waits for all ISR members to replicate
- **No data loss** if at least one ISR survives
- Tradeoff: Higher latency, lower throughput

**Appropriate for StreamCore** because video streaming events are critical for analytics.

---

### Q2.3: Purpose of each producer config in StreamCore?

**A:**

| Config | Value | Purpose |
|--------|-------|---------|
| `bootstrap.servers` | localhost:9092 | Broker addresses |
| `client.id` | streamcore-producer | Producer identifier |
| `acks` | all | Durability: wait for all ISR |
| `retries` | 5 | Retry transient failures |
| `retry.backoff.ms` | 300 | Delay between retries |
| `linger.ms` | 10 | Batch wait time (10ms) |
| `compression.type` | snappy | Compress batches |

---

### Q2.4: Why model_dump(mode="json") vs dict()?

**A:** Handles Pydantic-specific types:
- **Datetime** → ISO 8601 string (not JSON serializable as-is)
- **Enum** → Enum value (not JSON serializable)
- **UUID, Decimal, Path** → Proper serialization

`dict()` would leave these as Python objects → JSON serialization fails.

---

### Q2.5: Purpose of delivery callback?

**A:** Kafka producer is **asynchronous**. The callback is invoked when:
- ✅ Message successfully delivered (err=None)
- ❌ Delivery failed (err=KafkaError)

Allows tracking delivery status, logging, metrics, dead letter queue routing.

---

### Q2.6: Why poll(0) after each produce?

**A:** Triggers **non-blocking** callback execution and buffer flushing.

Without `poll(0)`:
- Callbacks only execute during `flush()` or when buffer is full
- Messages may sit in buffer for up to `linger.ms` (10ms)

`poll(0)` processes pending callbacks **without blocking**.

---

### Q2.7: What if sleep removed from run_producer.py?

**A:**
1. Producer runs at **max speed** (limited by CPU/network)
2. **BufferError** when producer buffer fills up (32MB default)
3. Kafka broker **overload** (Docker single broker: ~100-200K msg/sec)
4. **Consumer lag** grows infinitely (consumer processes ~100 msg/sec)
5. **Network saturation** on host machine

**Result:** System instability, message loss, crashes.

---

### Q2.8: Why separate TopicRegistry from producer?

**A:** **Single Source of Truth (SSOT)** / **Don't Repeat Yourself (DRY)**

**Benefits:**
- Consistency: All components use same topic names
- Maintainability: Change topic name in one place
- Discoverability: Easy to find all topics
- Extensibility: Easy to add new topics
- Type safety: Maps event types (not strings) to topics
- Runtime validation: Fail fast if event type not registered

---

### Q2.9: Three values for acks and tradeoffs?

**A:**

| acks | Behavior | Durability | Latency | Throughput | Use Case |
|------|----------|------------|---------|------------|----------|
| 0 | No wait | ❌ None | ✅ Lowest | ✅ Highest | Metrics, logs |
| 1 | Leader ack | ⚠️ Leader only | ⚠️ Low | ✅ High | General |
| all | All ISR ack | ✅ Full | ❌ Highest | ⚠️ Lower | **Critical data** |

---

### Q2.10: How does producer batch messages?

**A:** Messages are grouped into **batches** before sending.

**Configs:**
- `batch.size`: Max bytes per batch (16KB default)
- `linger.ms`: Max wait time (10ms in StreamCore)
- `buffer.memory`: Total buffer size (32MB default)

**When batch is sent:**
- Batch reaches `batch.size`
- `linger.ms` timeout elapsed
- Buffer is getting full

**Benefits:** Network efficiency, higher throughput, better compression.

---

### Q2.11: What is idempotent production?

**A:** Sending same message multiple times → written **exactly once**.

**How Kafka achieves it:**
- Each message gets a **sequence number** (per partition)
- Broker tracks last sequence number per partition
- Duplicate (same producer, partition, seq#) → ignored

**Enable:** `enable.idempotence=true`

**Requirements:** `max.in.flight.requests.per.connection <= 5` (auto-set)

---

### Q2.12: Difference between send() and flush()?

**A:**

| Method | Behavior | Blocking? | Use Case |
|--------|----------|-----------|----------|
| `send()` | Add to buffer, return immediately | ❌ No | Normal production |
| `flush()` | Wait for all buffered messages | ✅ Yes | Graceful shutdown |

---

### Q2.13: Handling retryable vs non-retryable errors?

**A:**

**Retryable (transient):**
- `LEADER_NOT_AVAILABLE`, `NOT_ENOUGH_REPLICAS`, `TIMED_OUT`, `BROKER_NOT_AVAILABLE`
- **Strategy:** Retry with exponential backoff (100ms, 200ms, 400ms...)

**Non-retryable (permanent):**
- `MSG_SIZE_TOO_LARGE`, `UNKNOWN_TOPIC_OR_PART`, `INVALID_MSG`
- **Strategy:** Log, send to DLQ, fix root cause

**BufferError:** Special case - flush buffer and retry.

---

### Q2.14: Purpose of max.block.ms?

**A:** Maximum time producer **blocks** when:
- Buffer is full
- Metadata is being fetched

**Default:** 60 seconds

**Why it matters:**
- Prevents **indefinite blocking**
- Enables **backpressure** (producer slows down automatically)
- Allows **fail-fast** when issues occur

---

### Q2.15: What happens when producer buffer is full?

**A:**

1. If `max.block.ms > 0`: Producer **blocks** calling thread
2. Waits up to `max.block.ms` for space
3. If space available → message added to buffer
4. If timeout → **BufferError** raised

**Sender thread** (background) continuously:
- Takes batches from buffer
- Sends to brokers
- Frees up space

**Monitor:** `buffer.memory.used`, `buffer.exhausted.rate`

---

---

## 3. Consumer Deep Dive

### Q3.1: Why enable.auto.commit=false in StreamCore?

**A:** To achieve **at-least-once delivery**.

With `enable.auto.commit=true`:
- Offset committed **immediately on receipt**
- If consumer crashes after receipt but before processing → **message lost**

With `enable.auto.commit=false`:
- Offset committed **only after successful processing**
- If crash before commit → message **reprocessed** on restart
- **At-least-once** guarantee

---

### Q3.2: Why commit offset AFTER writing to Postgres?

**A:** Ensures **at-least-once semantics**.

**Order matters:**
1. Receive message from Kafka
2. Write to Postgres
3. **Commit offset** (only after successful write)

If step 2 fails → offset not committed → message **reprocessed** on restart.

If offset committed before write → message could be **lost** if write fails.

---

### Q3.3: Why auto.offset.reset=earliest in StreamCore?

**A:** To **process all existing messages** on first run.

StreamCore consumer needs to process:
- All historical events from startup
- New events as they arrive

`earliest` ensures no messages are missed during initial setup.

---

### Q3.4: Why batch 100 events in PostgresSink?

**A:** **Performance optimization**.

**Benefits:**
- 100 events = 1 network round trip (vs 100)
- **executemany()** is dramatically faster than 100 individual INSERTs
- Reduces database load

**Drawbacks:**
- Adds latency (wait for batch to fill)
- Memory usage (buffering 100 events)
- If batch fails, all 100 must be retried

---

### Q3.5: Why ON CONFLICT (event_id) DO NOTHING?

**A:** Provides **idempotent writes** at the database level.

- Each event has a **unique UUID** (`event_id`)
- If duplicate event arrives → insert fails (constraint violation)
- `ON CONFLICT` ignores the duplicate
- **Result:** No duplicate data in database

This complements Kafka-level deduplication.

---

### Q3.6: Why commit offset only after sink write?

**A:** To ensure **at-least-once delivery**.

**Flow:**
1. Receive message
2. Write to Postgres
3. **Commit offset** (only if write succeeds)

If write fails → offset not committed → message **reprocessed** on retry.

If offset committed first → message **lost** if write fails later.

---

### Q3.7: What happens during poll(timeout=1.0)?

**A:**
- Blocks for **up to 1 second** waiting for a message
- Checks `self._running` flag periodically (every ~1s)
- Returns a message if available, or `None` if timeout
- **Why 1s:** Snappy Ctrl+C response (checks running flag)

---

### Q3.8: Why do both core/consumer.py and consumers/core/consumer.py exist?

**A:** Likely **refactoring in progress**.

Possible reasons:
- Code reorganization (moving from `core/` to `consumers/`)
- Different implementations being tested
- Legacy code not yet cleaned up

**Recommendation:** Keep only one, remove the other.

---

### Q3.9: commitSync() vs commitAsync()?

**A:**

| Method | Behavior | Use Case |
|--------|----------|----------|
| `commitSync()` | Blocks until offset committed | Simple, guaranteed |
| `commitAsync()` | Returns immediately, commits in background | Higher throughput, handle callback |

---

### Q3.10: How does Kafka track consumer offsets?

**A:** Offsets are stored in a **special Kafka topic** (`__consumer_offsets`).

- Each consumer group has its own offsets
- Each partition in a topic has its own offset
- Offsets are **committed** by consumers
- Stored with key: `(group.id, topic, partition)` → value: `offset`

---

### Q3.11: What is consumer group rebalancing?

**A:** Process of **reassigning partitions to consumers** when group membership changes.

**Triggers:**
- Consumer joins/leaves
- Consumer heartbeat timeout
- Consumer poll timeout
- Topic metadata changes

**Costs:**
- All consumers stop processing during rebalance
- Potential duplicates (if offsets not committed)
- Latency spike

---

### Q3.12: subscribe() vs assign()?

**A:**

| Method | Behavior | Use Case |
|--------|----------|----------|
| `subscribe(topics)` | Subscribe to topics, **automatic partition assignment** | Normal usage |
| `assign(partitions)` | Manually assign **specific partitions** | Advanced: custom assignment logic |

---

### Q3.13: Purpose of max.poll.records?

**A:** Maximum number of records returned by a **single poll()** call.

**Default:** 500

**Why limit:**
- Prevents consumer from being overwhelmed
- Controls memory usage
- Allows processing to complete within `max.poll.interval.ms`

---

### Q3.14: Handling slow consumer?

**A:**
1. **Scale horizontally**: Add more consumers to the group
2. **Increase batch size**: Process more records per poll
3. **Optimize processing**: Reduce per-message overhead
4. **Increase max.poll.records**: Fetch more at once
5. **Use async processing**: Offload processing to thread pool

---

---

## 4. Configuration & Settings

### Q4.1: Why @lru_cache for settings?

**A:** **Singleton pattern** - ensures only one instance of each settings class is created.

**Benefits:**
- Consistent configuration across application
- Avoids repeated file I/O (reading .env)
- Thread-safe (lru_cache is thread-safe)

---

### Q4.2: Benefits of pydantic-settings over plain Python config?

**A:**
- **Type validation**: Ensures correct types
- **Environment variables**: Automatic loading from env
- **Default values**: Sensible defaults
- **Documentation**: Field descriptions
- **Nested configs**: Hierarchical configuration
- **Validation**: Fails fast on startup

---

### Q4.3: Why separate config classes (Kafka, Postgres, Producer, App)?

**A:** **Separation of Concerns** / **Single Responsibility Principle**

Each config class handles one domain:
- `KafkaSettings`: Kafka-specific configs
- `PostgresSettings`: Database configs
- `ProducerSettings`: Producer behavior
- `AppSettings`: Application-level configs

**Benefits:**
- Clear ownership
- Easy to modify
- Testable in isolation
- Avoids monolithic config

---

### Q4.4: How does Field() help with validation?

**A:** Provides metadata and validation rules:

```python
# From StreamCore
events_per_second: int = Field(default=10, ge=1, le=10000)
```

**Validation:**
- `ge=1`: Must be ≥ 1
- `le=10000`: Must be ≤ 10,000
- `default=10`: Default value
- Fails with clear error message if validation fails

---

### Q4.5: Why is dsn a property, not a method?

**A:** **Caching** and **consistency**.

As a property:
- Built once when first accessed
- Cached for subsequent accesses
- Used as an attribute (cleaner syntax)

```python
# Usage
settings = get_postgres_settings()
conn = psycopg.connect(settings.dsn)  # Clean

# vs method
conn = psycopg.connect(settings.get_dsn())  # Less clean
```

---

### Q4.6: Most important producer configs for reliability?

**A:**
1. `acks=all` - Durability
2. `retries>0` - Handle transient failures
3. `max.in.flight.requests.per.connection=1` - Prevent duplicates with acks=all
4. `enable.idempotence=true` - Idempotent production
5. `compression.type=snappy/gzip` - Network efficiency

---

### Q4.7: Configs to optimize producer throughput?

**A:**
1. `batch.size=64KB` - Larger batches
2. `linger.ms=50-100` - Wait longer for batches
3. `compression.type=lz4/snappy` - Better compression
4. `buffer.memory=64MB+` - Larger buffer
5. `max.in.flight.requests.per.connection=5` - More parallel requests

---

### Q4.8: Configure consumer for strict order?

**A:**
1. **Single partition**: All messages go to one partition
2. **Key by ordering field**: Messages with same key go to same partition
3. **Single consumer**: Only one consumer in group
4. **max.poll.records=1**: Process one message at a time

---

### Q4.9: Purpose of session.timeout.ms and heartbeat.interval.ms?

**A:**
- `session.timeout.ms`: Max time between heartbeats before consumer considered dead
- `heartbeat.interval.ms`: How often consumer sends heartbeats

**Rule:** `heartbeat.interval.ms < session.timeout.ms` (typically 1/3)

**Default:** heartbeat=3000ms, session.timeout=45000ms

---

### Q4.10: How does fetch.min.bytes affect consumer performance?

**A:** Minimum data to accumulate before returning to consumer.

**Impact:**
- **Higher**: Waits for more data → better throughput, higher latency
- **Lower**: Returns sooner → lower latency, more requests

**Default:** 1 byte (returns immediately if data available)

---

---

## 5. Architecture & Design Patterns

### Q5.1: Design pattern in KafkaProducerClient?

**A:** **Adapter Pattern**

Wraps the raw `confluent_kafka.Producer` to:
- Provide clean Python interface (Pydantic → JSON → bytes)
- Add delivery callbacks
- Handle errors consistently
- Provide graceful shutdown

---

### Q5.2: Design pattern in StreamCoreConsumer?

**A:** **Strategy Pattern**

Consumer doesn't know about sink type:
- `PostgresSink`, `BigQuerySink`, etc. are interchangeable
- Consumer just calls `sink.write()`
- Sink implementation is **pluggable**

---

### Q5.3: Why Pydantic for event schemas?

**A:**
- **Validation**: Catch malformed events at producer
- **Serialization**: Automatic JSON conversion
- **Type safety**: IDE autocomplete, mypy checks
- **Self-documenting**: Schema IS the documentation
- **Immutability**: `frozen=True` prevents bugs

---

### Q5.4: Why are events immutable (frozen=True)?

**A:** Prevents bugs in concurrent code:
- Multiple threads can safely access same event
- No risk of event being modified mid-processing
- Prevents entire class of race conditions
- Events represent **facts** (shouldn't change)

---

### Q5.5: Why generator pattern in EventGenerator?

**A:** **Memory efficiency** and **lazy evaluation**.

**Benefits:**
- Events generated **on-demand** (not all in memory)
- Can produce **infinite stream** (no memory limit)
- **Pause/resume** possible (yield control back to caller)

---

### Q5.6: Why separate producers, consumers, sinks?

**A:** **Separation of Concerns** / **Single Responsibility Principle**

- **Producers**: Generate and send events
- **Consumers**: Receive and route events
- **Sinks**: Persist events

Each module has one clear responsibility.

---

### Q5.7: Why structlog for logging?

**A:** **Structured logging** enables:
- **Queryable logs**: Filter by field (e.g., `user_id=u_123`)
- **Consistent format**: JSON output for production
- **Context enrichment**: Add user_id, request_id, etc.
- **Multiple outputs**: Console (dev), JSON (prod)

---

### Q5.8: Six layers in StreamCore architecture?

**A:**
1. **Ingestion**: Apache Kafka
2. **Storage**: PySpark + Postgres
3. **Orchestration**: Apache Airflow
4. **Warehouse**: BigQuery
5. **Modeling**: dbt
6. **Observability**: Dashboards + Data Quality

---

### Q5.9: What are vertical slices?

**A:** Each slice is **end-to-end and shippable**.

**vs Horizontal Layers:**
- **Vertical**: Build Layer 1 + Layer 2 for one use case, then Layer 3
- **Horizontal**: Build all of Layer 1, then all of Layer 2, etc.

**Benefits:**
- **Faster delivery**: Each slice provides value
- **Easier testing**: Test complete flow
- **Clear progress**: Visible milestones

---

### Q5.10: Why TopicRegistry as single source of truth?

**A:** Prevents:
- **Inconsistency**: Different topic names in producer/consumer
- **Maintenance nightmare**: Update topic name in multiple files
- **Runtime errors**: Typos in topic names
- **Discovery issues**: Hard to find all topics

---

---

## 6. Error Handling & Debugging

### Q6.1: What causes BufferError and how handled?

**A:** **Cause:** Producer's internal buffer is full (`buffer.memory` exceeded).

**Handling in StreamCore:**
```python
try:
    self._producer.produce(...)
except BufferError:
    log.warning("kafka_buffer_full_flushing")
    self._producer.flush(timeout=10)  # Flush pending messages
    self._producer.produce(...)       # Retry
```

---

### Q6.2: Why doesn't delivery callback retry?

**A:** Callbacks are for **notification**, not retry logic.

**Why:**
- Retry logic belongs in **producer code**, not callback
- Callback is **asynchronous** - retrying here could cause ordering issues
- Better to handle retries in the main produce loop

**Improvement:** Add metrics, DLQ routing, alerting in callback.

---

### Q6.3: How to make _process_message more robust?

**A:** Add **Dead Letter Queue** for failed messages:
```python
try:
    # Process message
    self._sink.write(...)
    self._consumer.store_offsets(msg)
except Exception as e:
    log.error(...)
    dlq_producer.send("dead_letter_queue", msg.value())
```

---

### Q6.4: What does partition_eof_reached mean?

**A:** **Not an error** - consumer has read all current messages in partition.

- Normal condition when consumer is caught up
- New messages will arrive later
- Just means "end of partition **for now**"

---

### Q6.5: What causes PostgresSink transaction rollback?

**A:** Any exception during:
- Database connection issues
- SQL syntax errors
- Constraint violations
- Timeout

**Result:** All pending events in the batch are **not written**.

---

### Q6.6: How to implement DLQ?

**A:**
1. Create a **dead_letter_queue** topic
2. Send failed messages there with **error metadata**:
   - Original message
   - Error type
   - Timestamp
   - Source topic/partition/offset
3. Create a **DLQ consumer** to:
   - Retry with exponential backoff
   - Alert on high rates
   - Manual inspection/reprocessing

---

### Q6.7: Common producer errors?

**A:**

| Error | Cause | Retryable? |
|-------|-------|------------|
| `LEADER_NOT_AVAILABLE` | Leader election | ✅ Yes |
| `NOT_ENOUGH_REPLICAS` | ISR too small | ✅ Yes |
| `MSG_SIZE_TOO_LARGE` | Message > limit | ❌ No |
| `UNKNOWN_TOPIC` | Topic doesn't exist | ⚠️ Maybe |
| `BUFFER_EXHAUSTED` | Buffer full | ✅ Yes |

---

### Q6.8: Common consumer errors?

**A:**

| Error | Cause | Action |
|-------|-------|--------|
| `PARTITION_EOF` | End of partition | Continue polling |
| `UNKNOWN_TOPIC` | Topic doesn't exist | Create topic |
| `GROUP_AUTHORIZATION` | No permission | Fix ACLs |
| `OFFSET_OUT_OF_RANGE` | Offset invalid | Reset offset |

---

### Q6.9: How to monitor Kafka pipeline?

**A:** Key metrics:

**Producer:**
- `record-send-rate`: Messages/sec
- `record-error-rate`: Error rate
- `record-queue-time-avg`: Buffer time
- `records-per-request-avg`: Batch size

**Consumer:**
- `records-consumed-rate`: Consumption rate
- `records-lag-max`: Max lag per partition
- `records-lag-avg`: Average lag
- `consumer-group-members`: Group size

**Broker:**
- `UnderReplicatedPartitions`: Replication issues
- `ActiveControllerCount`: Leader elections
- `RequestHandlerAvgIdlePercent`: Broker load

---

### Q6.10: How does Kafka ensure no messages missed/duplicated on consumer crash?

**A:**
- **No miss**: Committed offsets track last **successfully processed** message
- **No duplicate**: Process message → commit offset **after** processing
- On crash: Consumer restarts → resumes from **last committed offset**
- **At-least-once**: Messages may be reprocessed, but not missed

---

---

## 7. Performance & Scaling

### Q7.1: Effect of increasing linger.ms to 100ms?

**A:**
- **Throughput**: ✅ Increases (better batching)
- **Latency**: ❌ Increases (messages wait up to 100ms)
- **Network**: ✅ Improves (fewer, larger batches)

**Tradeoff:** Better throughput at cost of higher latency.

---

### Q7.2: Factors for choosing batch size (100)?

**A:**
- **Network overhead**: More records = fewer requests
- **Database performance**: `executemany()` vs individual INSERTs
- **Memory**: Buffering N events
- **Failure impact**: Lose entire batch on error
- **Latency**: Wait for batch to fill

**100 is good default** for most workloads.

---

### Q7.3: How to scale StreamCore consumer?

**A:**
1. **Add more consumers** to the group (horizontal scaling)
2. **Increase partitions** on topics (more parallelism)
3. **Optimize PostgresSink**: Larger batches, connection pooling
4. **Async processing**: Offload to thread pool
5. **Increase max.poll.records**: Fetch more at once

---

### Q7.4: Relationship between partitions and consumer parallelism?

**A:** **1:1 relationship** per consumer group.

- Each partition assigned to **exactly one consumer** in group
- **Max parallelism = number of partitions**
- More partitions → more consumers can work in parallel
- **But**: More partitions = more overhead

---

### Q7.5: Implications of keying by user_id?

**A:**

| Aspect | Impact |
|--------|--------|
| **Ordering** | ✅ Perfect - all user events in order |
| **Partition distribution** | ⚠️ Skewed if some users have more events |
| **Consumer scaling** | ⚠️ Limited by user distribution |

**Solution for skew:** Salted keys (e.g., `user_id + hash(user_id) % num_partitions`)

---

### Q7.6: Handling partition skew?

**A:**
1. **Repartition**: Increase partition count
2. **Salted keys**: Distribute hot keys across partitions
3. **Custom partitioner**: Smart distribution logic
4. **Scale consumers**: Add more consumers for hot partitions

---

### Q7.7: Configs for different optimizations?

**A:**

| Goal | Producer Configs | Consumer Configs |
|------|-----------------|-----------------|
| **Low latency** | linger.ms=0, batch.size=16KB | max.poll.records=1 |
| **High throughput** | linger.ms=100, batch.size=64KB | max.poll.records=500 |
| **Exactly-once** | acks=all, enable.idempotence=true | enable.auto.commit=false |

---

### Q7.8: Handling 10,000 events/sec (vs 10)?

**A:**
1. **Producer**: Increase batch.size, linger.ms, buffer.memory
2. **Kafka**: More partitions (e.g., 6-12), more brokers
3. **Consumer**: More consumers, larger batches
4. **Postgres**: Connection pooling, larger batches, async writes
5. **Monitoring**: Track lag, throughput, errors

---

### Q7.9: Impact of compression.type?

**A:**

| Type | Compression Ratio | CPU Usage | Speed |
|------|------------------|-----------|-------|
| none | 1.0x | ❌ Low | ✅ Fastest |
| gzip | 2.5-3x | ❌ High | ❌ Slow |
| snappy | 2-2.5x | ⚠️ Medium | ✅ Fast |
| lz4 | 2-2.5x | ⚠️ Medium | ✅ Fast |
| zstd | 3-4x | ❌ High | ⚠️ Medium |

**StreamCore uses snappy**: Good balance of ratio and speed.

---

### Q7.10: Impact of increasing batch.size?

**A:**
- ✅ **Fewer requests**: More messages per batch
- ✅ **Better compression**: Larger batches compress better
- ✅ **Higher throughput**: Less network overhead
- ❌ **Higher latency**: Wait longer for batches to fill
- ❌ **More memory**: Larger batches in memory
- ❌ **Worse failure impact**: More messages lost on error

---

---

## 8. Scenario-Based & Code Review

### Q8.1: Events out of order for a user?

**A:** **Cause:** Events for same user going to different partitions.

**Fix:**
- Verify events are **keyed by user_id**
- Check **partition count** hasn't changed
- Ensure **same producer instance** (sequence numbers reset on restart)

---

### Q8.2: Add new field to VideoViewEvent?

**A:**
1. **Pydantic model**: Add field with default or `None`
2. **Backward compatibility**: Use `default` or `Optional[Type]`
3. **Schema evolution**: JSONB column absorbs new field
4. **Consumer**: Update to handle new field (or ignore)

```python
# Add to schemas.py
new_field: str = Field(default=None, description="New field")
```

---

### Q8.3: Track marketing campaign referrer?

**A:**
1. Add `referrer` field to `BaseEvent` or specific event
2. Update simulator to generate realistic referrers
3. Update TopicRegistry if new event type
4. No consumer changes needed (JSONB absorbs)

---

### Q8.4: High latency between production and persistence?

**A:** **Diagnose:**
1. **Producer**: Check buffer usage, network to broker
2. **Kafka**: Check broker load, replication lag
3. **Consumer**: Check poll frequency, processing time
4. **Postgres**: Check insert latency, locks

**Tools:** Kafka lag metrics, database slow query log

---

### Q8.5: Kafka broker goes down?

**A:** **What happens:**
- Producer: Retries, may block on full buffer
- Consumer: May lose connection, rebalance
- **If replication factor ≥ 2**: No data loss (ISR takes over)
- **If replication factor = 1**: Data loss possible

**Improvements:**
- Increase replication factor to 3
- Add more brokers
- Monitor ISR size

---

### Q8.6: Replay past 24 hours?

**A:** Options:
1. **Reset consumer offsets**: Use `kafka-consumer-groups --reset-offsets`
2. **New consumer group**: Start with `auto.offset.reset=earliest`
3. **Kafka MirrorMaker**: Replay to new topic
4. **Custom script**: Read from earliest offset

```bash
# Reset offsets to 24 hours ago
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group streamcore-consumer-group \
  --reset-offsets --to-datetime 2024-01-15T00:00:00.000
```

---

### Q8.7: Duplicate events produced?

**A:**

**Detect:**
- Monitor for duplicate `event_id` in Postgres
- Check consumer lag not decreasing as expected

**Prevent:**
- `enable.idempotence=true` in producer
- `ON CONFLICT (event_id) DO NOTHING` in Postgres

**Fix:**
- Find and fix producer bug (e.g., retry logic)
- Deduplicate in database if needed

---

### Q8.8: Migrate from Postgres to BigQuery?

**A:** Changes needed:
1. **New sink**: `BigQuerySink` implementing same interface as `PostgresSink`
2. **Authentication**: Service account credentials
3. **Schema**: BigQuery table schema
4. **Configuration**: New settings class
5. **Consumer**: No changes (Strategy pattern!)

```python
# consumers/core/consumer.py - NO CHANGES NEEDED
consumer = StreamCoreConsumer(topics=topics, sink=BigQuerySink())
```

---

### Q8.9: Handle 10x traffic bursts?

**A:**
1. **Kafka**: Increase partitions, buffer.memory
2. **Producer**: Larger batches, more compression
3. **Consumer**: More consumers, auto-scaling
4. **Postgres**: Connection pooling, larger batches
5. **Queue**: Let Kafka buffer the burst

---

### Q8.10: Add new event type?

**A:** Files to modify:
1. `producers/events/schemas.py` → Add new event class
2. `producers/core/topic_registry.py` → Add topic mapping
3. `producers/core/config.py` → Add topic name (optional)
4. Tests → Add tests for new event

---

### Q8.11: Events being lost?

**A:** **Investigation:**
1. **Producer**: Check delivery callbacks, BufferError rate
2. **Kafka**: Check broker logs, replication status
3. **Consumer**: Check offset commits, processing errors
4. **Postgres**: Check insert success rate

**Common causes:**
- Producer not flushing on shutdown
- Kafka broker down with replication factor=1
- Consumer crashing before offset commit

---

### Q8.12: Consumer falling behind?

**A:** **Strategies:**
1. **Scale consumers**: Add more to the group
2. **Increase parallelism**: More partitions
3. **Optimize processing**: Reduce per-message overhead
4. **Batch larger**: Increase max.poll.records
5. **Async processing**: Offload to thread pool
6. **Temporary**: Increase consumer timeout to allow catch-up

---

### Q9.1: Is 30s flush timeout reasonable?

**A:** **Depends on use case.**

**Factors:**
- Message volume: More messages = longer flush
- Network: Slow network = longer
- Broker health: Unhealthy broker = timeout
- **30s is reasonable** for most cases
- **Production**: Monitor actual flush times, adjust accordingly

---

### Q9.2: What's missing from _on_delivery callback?

**A:**
- **Metrics**: Increment success/failure counters
- **DLQ**: Send failed messages to dead letter queue
- **Alerting**: Alert on high error rates
- **Retry**: For certain transient errors

---

### Q9.3: Subscribe to all topics at once vs separately?

**A:**

| Approach | Pros | Cons |
|----------|------|------|
| **All at once** (StreamCore) | Simple, automatic rebalancing | All or nothing |
| **Separately** | Granular control, can prioritize | Complex, manual rebalancing |

**StreamCore's approach is fine** for most use cases.

---

### Q9.4: Pros/cons of JSONB payload vs individual columns?

**A:**

| Approach | Pros | Cons |
|----------|------|------|
| **JSONB** | Schema evolution, flexible, less ALTER TABLE | Harder to query, no constraints, larger storage |
| **Columns** | Fast queries, constraints, smaller storage | Schema changes require migration, rigid |

**StreamCore uses JSONB**: Good for raw event storage (Bronze layer).

---

### Q9.5: Better throughput metric than every 100 events?

**A:** **Moving average** or **per-second rate**.

Current:
```python
if event_count % 100 == 0:
    actual_rate = event_count / elapsed
```

Better:
```python
# Exponentially weighted moving average
actual_rate = 0.9 * actual_rate + 0.1 * (100 / (time.time() - last_100_time))
```

Or:
```python
# Per-second rate with window
rates.append(100 / (time.time() - last_100_time))
if len(rates) > 10: rates.pop(0)
actual_rate = sum(rates) / len(rates)  # 10-event moving average
```

---

### Q9.6: Implications of fixed Faker/Random seeds?

**A:**

| Environment | Impact | Good/Bad |
|-------------|--------|----------|
| **Testing** | Reproducible results | ✅ Good |
| **Development** | Debugging consistency | ✅ Good |
| **Production** | Predictable data, no randomness | ❌ **Bad** |

**Fix:** Remove seeds in production or make configurable.

---

### Q9.7: Is KeyError right for unregistered event types?

**A:** **Yes, fail fast.**

**Why:**
- Catches configuration errors early
- Prevents silent data loss
- Clear error message points to fix

**Alternatives:**
- Log warning + use default topic (dangerous - silent failures)
- Return None (caller must handle)
- Raise custom exception with more context

---

### Q9.8: How to implement graceful shutdown?

**A:** Use **signal handlers** or **context managers**:

```python
import signal
import sys

def shutdown(signum, frame):
    consumer.stop()
    sink.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# Then in main
try:
    consumer.start()
finally:
    sink.close()
```

---

### Q9.9: Why duplicate postgres_sink.py files?

**A:** **Code duplication issue** - likely from refactoring.

**Impact:**
- Maintenance burden (changes must be made in two places)
- Risk of divergence (files may differ)
- Confusion (which one is used?)

**Fix:**
1. Decide which location is correct (`consumers/sinks/` seems better)
2. Delete the other
3. Update all imports
4. Add deprecation warning if needed

---

### Q9.10: Postgres connection fails in run_consumer.py?

**A:** Currently: **Unhandled exception** → consumer crashes.

**Improvement:**
```python
try:
    sink.connect()
except Exception as e:
    log.error("Failed to connect to Postgres", error=str(e))
    raise  # Or exit with error code
```

---

---

## 9. Advanced Kafka Concepts

### Q9.1: Kafka's transactional API?

**A:** Enables **exactly-once semantics** across multiple partitions.

**How it works:**
1. Producer assigns **transactional ID**
2. `begin_transaction()` starts transaction
3. Send messages (buffered, not sent yet)
4. `commit_transaction()` sends all or none
5. If any fail → `abort_transaction()` rolls back

**Use when:**
- Need exactly-once across multiple partitions
- Atomic writes to multiple topics
- Producer restarts (idempotence + transactions)

---

### Q9.2: Kafka Streams vs PySpark Structured Streaming?

**A:**

| Feature | Kafka Streams | PySpark Structured Streaming |
|---------|---------------|-----------------------------|
| **Library** | Java/Scala | Python (PySpark) |
| **Deployment** | Runs in Kafka brokers | Runs in Spark cluster |
| **State** | Local state stores | Checkpointed to storage |
| **Fault tolerance** | Local + changelog | Checkpointing |
| **Language** | JVM only | Python, Java, Scala, R |
| **Use case** | Lightweight transformations | Heavy processing, ML |
| **StreamCore** | Not used | Used in `streaming/` |

---

### Q9.3: What is a compacted topic?

**A:** Topic that **retains only the latest value for each key**.

**How it works:**
- Messages with same key → only latest retained
- Old values for same key are **deleted**
- **Use for**: Key-value stores (e.g., user profiles, configurations)

**Example:**
```
Key: user_id, Value: user_profile
- Message 1: (u123, {name: "Alice", age: 25})
- Message 2: (u123, {name: "Alice", age: 26})
- Compacted: Only message 2 retained
```

---

### Q9.4: How does Kafka handle message expiration?

**A:**

**Time-based:**
- `log.retention.ms` (default: 7 days)
- Messages older than this are deleted

**Size-based:**
- `log.retention.bytes`
- Delete oldest messages when total size exceeded

**Segment-based:**
- Deletes entire segments (not individual messages)
- Checked every `log.retention.check.interval.ms` (5 min)

---

### Q9.5: What are Kafka consumer groups?

**A:** Set of consumers that **collectively read from a topic**.

**Key features:**
- Each partition assigned to **one consumer** in group
- **Horizontal scaling**: Add more consumers → more partitions processed
- **Fault tolerance**: Consumer fails → its partitions reassigned
- **Offset tracking**: Group tracks which messages processed

---

### Q9.6: What is consumer lag?

**A:** Difference between **latest offset** and **consumer's offset** in a partition.

**Calculation:**
```
lag = latest_offset - consumer_offset
```

**Monitoring:**
- `records-lag` metric per partition
- `records-lag-max` across all partitions
- Alert when lag > threshold

---

### Q9.7: 1 partition vs multiple partitions?

**A:**

| Aspect | 1 Partition | Multiple Partitions |
|--------|-------------|---------------------|
| **Ordering** | ✅ Global ordering | ⚠️ Per-partition ordering |
| **Throughput** | ❌ Limited to 1 consumer | ✅ Parallel processing |
| **Scalability** | ❌ Cannot scale | ✅ Horizontal scaling |
| **Overhead** | ✅ Low | ⚠️ Higher (more state) |
| **Use case** | Single consumer, ordered data | Multi-consumer, high volume |

---

### Q9.8: End-to-end exactly-once processing?

**A:** Combine:
1. **Idempotent producer** (`enable.idempotence=true`)
2. **Transactional producer** (`transactional.id`)
3. **At-least-once consumer** (manual offset commits)
4. **Idempotent sink** (e.g., `ON CONFLICT DO NOTHING`)

**Result:** Exactly-once from producer to database.

---

### Q9.9: What is idempotent production?

**A:** Sending same message multiple times → **written exactly once**.

**Kafka implementation:**
- Sequence numbers per (producer, partition)
- Broker deduplicates based on sequence numbers
- **Enable:** `enable.idempotence=true`

---

### Q9.10: Tradeoffs of partitions, replication, acks?

**A:**

| Increase | Benefit | Cost |
|----------|---------|------|
| **Partitions** | More parallelism, higher throughput | More overhead, more open files |
| **Replication** | More fault tolerance, higher availability | More storage, more network |
| **acks** | More durability | More latency, lower throughput |

---

### Q9.11: What is Kafka MirrorMaker?

**A:** Tool to **copy data between Kafka clusters**.

**Use cases:**
- Data center migration
- Aggregating data from multiple clusters
- Disaster recovery
- Cross-region replication

---

### Q9.12: Kafka quotas?

**A:** **Rate limiting** for clients.

**Types:**
- **Request quota**: Max requests/sec per client
- **Byte quota**: Max bytes/sec per client
- **CPU quota**: Max CPU usage per client

**Configuration:**
```
# broker config
quota.byte.rate=1048576  # 1MB/s
quota.request.percent=50  # 50% of broker capacity
```

**Use when:** Multi-tenant clusters, preventing noisy neighbors.

---

---

## Scoring Rubric

| Level | Producer | Consumer | Architecture | Debugging | Performance | Total |
|-------|----------|----------|-------------|-----------|-------------|-------|
| Junior | 0-2 | 0-2 | 0-2 | 0-1 | 0-1 | 0-10 |
| Mid | 3-4 | 3-4 | 3-4 | 2-3 | 2-3 | 11-18 |
| Senior | 5 | 5 | 5 | 4-5 | 4-5 | 19-25 |

**Key Concepts to Look For:**
- ✅ Understands acks configurations and tradeoffs
- ✅ Knows about keying for ordered delivery
- ✅ Understands manual vs automatic offset commits
- ✅ Knows at-least-once vs at-most-once vs exactly-once
- ✅ Understands adapter and strategy patterns
- ✅ Knows how to diagnose consumer lag
- ✅ Understands the impact of batch sizes
- ✅ Knows about partitioning strategies

---

## Quick Reference

### StreamCore Kafka Stack

**Topics:**
```
streamcore.video.views → VideoViewEvent
streamcore.video.watch_progress → WatchProgressEvent
```

**Producer Config:**
```python
acks = "all"
retries = 5
linger.ms = 10
compression.type = "snappy"
```

**Consumer Config:**
```python
enable.auto.commit = false
auto.offset.reset = "earliest"
group.id = "streamcore-consumer-group"
```

**Key Design Decisions:**
- Events keyed by `user_id` for ordered delivery
- Manual offset commits for at-least-once delivery
- Batch inserts (100 events) for Postgres performance
- JSONB column for schema evolution
- Pydantic models for validation
- Structured logging with structlog

---

*Generated based on StreamCore project analysis - Kafka Producer/Consumer Implementation*
