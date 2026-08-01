# StreamCore Project Documentation

> Detailed, code-level documentation of what StreamCore actually does today, file by file.
> Companion to `README.md` (quick start, roadmap) and `CLAUDE.md` (guidance for AI coding
> assistants working in this repo) — this document goes deeper into *why* things are built the
> way they are.

---

## 1. Executive Summary

`StreamCore` is a local-first data engineering project that simulates the data platform behind
a video streaming product (a miniature Netflix/YouTube analytics stack), built in **vertical
slices** rather than horizontal layers — every slice is end-to-end and independently runnable.

What it does, end to end:

1. A Python simulator generates realistic video-watching events (`producers/`).
2. Events are published to Kafka topics.
3. A Kafka consumer writes every raw event into Postgres as JSONB (`streamcore_raw.events`).
4. A PySpark Structured Streaming job independently reads the same Kafka topics and maintains
   three real-time aggregate tables in Postgres (`streamcore_aggregated.*`).
5. ClickHouse reads `streamcore_raw.events` live out of Postgres (via a federated table engine,
   not a copy) and dbt builds a staging → intermediate → marts model on top of it.
6. Airflow runs `dbt build` hourly, followed by a custom data-quality check.
7. Metabase visualizes the real-time Postgres aggregates; dbt's own tests plus a standalone
   `quality/` package catch pipeline problems dbt's pass/fail signal alone would miss.

All six slices on the original roadmap are implemented and have been verified against a live,
running stack — not just written.

---

## 2. Mental Model: What Problem Is This Solving?

A video streaming platform needs to answer questions like:

- How many people are watching each video right now?
- Which devices are buffering the most?
- Which videos are trending in the last 5 minutes?
- Which videos do people actually finish watching?
- Is the pipeline itself healthy — is data still flowing, are the marts actually populated?

`StreamCore` models the data backbone for this. It does not serve video — it focuses entirely
on the data events a streaming platform produces and the layers of processing built on top of
them.

Event types:

- `video_view` — a user starts watching a video (`VideoViewEvent`).
- `watch_progress` — a periodic (~10s) playback tick while watching (`WatchProgressEvent`).

---

## 3. Current Architecture

### 3.1 Data Flow

```
Producer (Python sim)
        │
        ▼
   Kafka topics
        │
        ├──────────────────────────────┐
        ▼                              ▼
Postgres raw table              PySpark Structured Streaming
(streamcore_raw.events,         (concurrent viewers / buffering
 JSONB, via the consumer)        rate / top videos)
        │                              │
        │                              ▼
        │                     Postgres aggregate tables
        │                     (streamcore_aggregated.*)
        │                              │
        │                              ▼
        │                          Metabase dashboards
        │
        ▼  (ClickHouse's PostgreSQL table engine — live query, not a copy)
    ClickHouse
        │
        ▼
       dbt (staging → intermediate → marts)
        │
        ▼
Airflow (hourly): dbt build, then data-quality checks
```

The producer, consumer, and Spark job are **always-on** — three long-lived processes, started
manually, independent of Airflow, and they only ever talk to Kafka/Postgres. ClickHouse/dbt/
Airflow are a batch analytical layer bolted on afterward; nothing about the streaming path
changed to support them.

### 3.2 Components

| Layer | Component | Purpose | Status |
|---|---|---|---|
| Event generation | Python simulator (`producers/events/`) | Realistic user/video/session event streams | Done |
| Ingestion | Kafka | Buffers and distributes events | Done |
| Raw storage | Postgres (`streamcore_raw.events`) | Append-only JSONB landing zone | Done |
| Stream processing | PySpark Structured Streaming | Real-time windowed aggregations | Done |
| Real-time storage | Postgres (`streamcore_aggregated.*`) | Upserted per micro-batch | Done |
| Warehouse | ClickHouse | Reads Postgres live via table-engine bridge | Done |
| Modeling | dbt (`dbt-clickhouse`) | staging → intermediate → marts | Done |
| Orchestration | Airflow | Hourly `dbt build` + data-quality task | Done |
| Dashboards | Metabase | Visualizes `streamcore_aggregated.*` | Done |
| Data quality | dbt tests + `quality/` package | Freshness, range, and volume checks | Done |

---

## 4. Repository Structure

```
streamcore/
├── producers/                 # Event schemas, simulator, Kafka producer client, config
│   ├── core/                  # config.py, kafka_client.py, logging_setup.py, topic_registry.py
│   └── events/                # schemas.py (Pydantic event contracts), simulator.py
├── consumers/                 # Kafka consumer + Postgres sink
│   ├── core/consumer.py
│   └── sinks/postgres_sink.py
├── streaming/                 # PySpark Structured Streaming job
│   ├── core/spark_session.py
│   └── jobs/watch_aggregator.py
├── quality/                   # Slice 6 — standalone data-quality checks
│   └── checks.py
├── infra/postgres/            # SQL run automatically on first Postgres volume init
│   ├── 01_init_schema.sql     # streamcore_raw.events
│   └── 02_streaming_schema.sql # streamcore_aggregated.*
├── streamcore_dbt/            # Separate dbt project — targets ClickHouse
│   ├── models/staging/        # stg_video_views, stg_watch_progress (views)
│   ├── models/intermediate/   # int_watch_sessions (table)
│   ├── models/marts/          # mart_content_performance, mart_device_quality (tables)
│   ├── macros/                # postgres_bridge.sql, generate_schema_name.sql
│   ├── tests/                 # 3 singular data tests
│   └── profiles.example.yml   # host-side dbt connection profile
├── airflow/                   # Batch orchestration (Slice 4 + Slice 6's second task)
│   ├── Dockerfile             # Airflow image + dbt-clickhouse + quality-check deps
│   ├── dags/streamcore_dbt_dag.py
│   └── profiles/profiles.yml  # container-side dbt connection profile
├── scripts/                   # CLI entry points (run_producer/consumer/streaming, run_data_quality_checks)
├── tests/                     # pytest suite — schemas, simulator, Kafka client, consumer, sink, quality checks
├── docs/                      # This file
├── docker-compose.yml         # Full local stack
├── pyproject.toml             # Python dependencies, pytest/ruff/mypy config
└── .env.example               # Config template (matches producers/core/config.py's defaults)
```

There is no root-level `dbt_project.yml` — dbt commands only work from inside `streamcore_dbt/`.
There are no legacy `core/`/`sinks/` duplicate packages at the repo root — everything imports
from `producers.*` / `consumers.*`.

---

## 5. Main Runtime Flows

### 5.1 Producer Flow

Entry point: `scripts/run_producer.py`

1. Configure structured logging (`producers/core/logging_setup.py`, `structlog`).
2. Load `KafkaSettings` / `ProducerSettings` from `.env` via `pydantic-settings`.
3. Build a `KafkaProducerClient` (`producers/core/kafka_client.py`) — wraps `confluent-kafka`
   with `acks=all`, 5 retries, `linger.ms=10`, `snappy` compression.
4. Build a `TopicRegistry` (`producers/core/topic_registry.py`) — the single source of truth
   mapping event classes to Kafka topic names.
5. Pull events from `EventGenerator.stream()` (`producers/events/simulator.py`) — an infinite
   generator of realistic user sessions.
6. For each event: look up its topic, serialize (`event.model_dump(mode="json")`), send keyed
   by `user_id` (so a user's events land in the same partition and stay ordered).
7. On shutdown: `client.close()` flushes any buffered, undelivered messages.

**Simulator realism** (`producers/events/simulator.py`): device type affects initial video
quality (mobile gets lower quality, smart TV/desktop gets 1080p/4K); watch duration follows a
right-skewed Beta(5, 2) distribution (mean ~71% completion) to model drop-off; mobile devices
buffer more often (10% per tick vs 3% for other devices); video durations follow a weighted
long-tail distribution (60s–3600s). `Faker` and `random` are seeded (`42`) for reproducible runs.

### 5.2 Consumer Flow

Entry point: `scripts/run_consumer.py`

1. Configure logging, connect to Postgres (`consumers/sinks/postgres_sink.py`).
2. Subscribe to every topic in `TopicRegistry.all_topics`.
3. Poll Kafka in a loop (`consumers/core/consumer.py`, `StreamCoreConsumer`).
4. For each message: decode JSON, hand it to the sink (`PostgresSink.write`), then commit the
   offset.
5. On shutdown: flush pending rows, close the Kafka consumer.

**Offset handling** — manual, not auto-commit:
```python
"enable.auto.commit": False,
"enable.auto.offset.store": False,
```
`_process_message` writes to Postgres *first*, then calls `store_offsets(msg)` **and**
`commit(message=msg, asynchronous=False)`. `store_offsets` alone only stages the offset locally
— without the explicit synchronous `commit()`, nothing is ever sent to the broker, so a
restarted consumer group would have no durable checkpoint and would replay the entire topic from
`auto.offset.reset=earliest`. This exact bug existed earlier in the project and was fixed; a
regression test for it lives in `tests/test_consumer.py`.

**Sink** (`consumers/sinks/postgres_sink.py`, strategy pattern — `StreamCoreConsumer` doesn't
know or care what the sink is): batches up to `BATCH_SIZE=100` events, then
`executemany`s a single `INSERT ... ON CONFLICT (event_id) DO NOTHING` per batch. Payloads are
stored as `::jsonb` — schema evolution (new event fields) doesn't require a migration.

### 5.3 Streaming Aggregation Flow

Entry point: `scripts/run_streaming.py`

1. `streaming/core/spark_session.py` builds a singleton `SparkSession` (`local[*]`, all cores),
   auto-downloading the Kafka connector via `spark.jars.packages`.
2. `streaming/jobs/watch_aggregator.py` (`WatchAggregatorJob`) starts three independent
   Structured Streaming queries, each reading Kafka directly (not through the consumer/Postgres
   raw table):

| Aggregation | Source topic | Window | Watermark | Key | Metric |
|---|---|---|---|---|---|
| `concurrent_viewers` | watch_progress | 30s tumbling | 1 min | `video_id` | `approx_count_distinct(session_id)` |
| `buffering_rate` | watch_progress | 1 min tumbling | 2 min | `device_type` | `buffering_events / total_events * 100` |
| `top_videos` | video_view | 5 min tumbling | 2 min | `video_id`, `video_title` | `count(*)` |

3. Every micro-batch is written via `foreachBatch` (`make_postgres_writer`) — a dynamically
   built `INSERT ... ON CONFLICT (<keys>) DO UPDATE SET ..., written_at = NOW()` upsert, because
   late-arriving events can change an already-emitted window's numbers and duplicate rows aren't
   wanted.
4. `outputMode("update")`, `trigger(processingTime="10 seconds")` on all three queries;
   `spark.streams.awaitAnyTermination()` blocks until one query stops or errors.

**Known dependency constraint — pyspark must stay below 4.0.0.** `spark_session.py` hardcodes
the Kafka connector as `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0` — a Scala 2.12 / Spark
3.5.0 Maven coordinate. pyspark 4.x moved to Scala 2.13; running the streaming job against an
unpinned/4.x pyspark install crashes at stream-read time with
`NoSuchMethodError: scala.Predef$.wrapRefArray`. This was an actual, previously-undiscovered bug
in this project (`pyproject.toml` originally had an unbounded `pyspark>=3.5.0`, which resolved to
4.2.0) — found by actually running the job end-to-end, not by inspection. Fixed by pinning
`pyspark>=3.5.0,<4.0.0`. If you ever upgrade off Spark 3.5.x, bump the connector coordinate in
`spark_session.py` at the same time.

---

## 6. Data Contracts (`producers/events/schemas.py`)

All events subclass `BaseEvent` — a `frozen=True` (immutable) Pydantic model. This is the
contract between the producer and every downstream consumer: the Postgres sink, the Spark
job's manually-maintained schemas, and dbt's staging models. **Nothing shares a single schema
source across these** — changing a field means updating the Pydantic model, `VIEW_EVENT_SCHEMA`/
`PROGRESS_EVENT_SCHEMA` in `watch_aggregator.py`, and the relevant `stg_*.sql` model, by hand.

### 6.1 `BaseEvent` (common fields)

| Field | Type | Notes |
|---|---|---|
| `event_id` | `str` | Unique event identifier |
| `event_type` | `str` | Discriminator: `video_view` / `watch_progress` |
| `event_timestamp` | `datetime` | UTC, defaults to now |
| `user_id` | `str` | |
| `session_id` | `str` | |
| `device_type` | `DeviceType` (enum) | `mobile_ios`, `mobile_android`, `web_desktop`, `web_mobile`, `smart_tv`, `tablet` |
| `ip_address` | `str` | Simulated |
| `country_code` | `str` | Exactly 2 chars (ISO 3166-1 alpha-2) |

### 6.2 `VideoViewEvent`

Fired once, when playback starts. Adds: `video_id`, `video_title`, `video_duration_seconds`
(`ge=0`), `initial_quality` (`VideoQuality` enum: `240p`/`480p`/`720p`/`1080p`/`4k`), `referrer`
(nullable — `homepage`/`search`/`recommendation`/`None`).
Kafka topic default: `streamcore.video.views`.

### 6.3 `WatchProgressEvent`

Fired ~every 10s during playback — the high-volume event type. Adds: `video_id`,
`position_seconds` (`ge=0`), `quality`, `is_buffering` (`bool`), `playback_rate`
(`0.25`–`2.0`, default `1.0`).
Kafka topic default: `streamcore.video.watch_progress`.

---

## 7. Database Design

### 7.1 Raw Layer — Postgres (`infra/postgres/01_init_schema.sql`)

`streamcore_raw.events` — append-only bronze layer. Columns: `id` (surrogate PK), `event_id`
(unique), `event_type`, `event_timestamp`, `payload JSONB`, `ingested_at` (defaults `NOW()`),
plus `kafka_topic`/`kafka_partition`/`kafka_offset`. Indexes: `(event_type, event_timestamp DESC)`,
`(ingested_at DESC)`, and a GIN index on `payload` for querying inside the JSON.

### 7.2 Aggregated Layer — Postgres (`infra/postgres/02_streaming_schema.sql`)

`streamcore_aggregated.*` — the Spark job's silver layer, written by `foreachBatch` upserts:

| Table | Primary key | Columns of note |
|---|---|---|
| `concurrent_viewers` | `(window_start, video_id)` | `concurrent_viewers` |
| `buffering_rate` | `(window_start, device_type)` | `total_events`, `buffering_events`, `buffering_rate_pct NUMERIC(5,2)` |
| `top_videos` | `(window_start, video_id)` | `video_title`, `view_count` |

All three have a `written_at TIMESTAMPTZ DEFAULT NOW()`, updated by the upsert's
`SET ..., written_at = NOW()`.

### 7.3 Warehouse — ClickHouse, via dbt (see §9)

`streamcore_staging` → `streamcore_intermediate` → `streamcore_marts` databases, built entirely
by dbt from a live federated read of `streamcore_raw.events` — no separate ingestion pipeline.

---

## 8. Configuration (`producers/core/config.py`)

`pydantic-settings` classes, each wrapped in an `@lru_cache`d getter (singleton — first call
loads `.env`, later calls return the cached instance). Fail-fast: invalid/missing config raises
at import/startup, not later during a run.

| Settings class | Env prefix | Key fields |
|---|---|---|
| `KafkaSettings` | `KAFKA_` | `bootstrap_servers`, `topic_view_events`, `topic_watch_events`, `client_id` |
| `PostgresSettings` | `POSTGRES_` | `host`, `port`, `db`, `user`, `password`; exposes `.dsn` |
| `ClickHouseSettings` | `CLICKHOUSE_` | `host`, `port` (8123, HTTP interface), `db`, `user`, `password` |
| `DataQualitySettings` | `DATA_QUALITY_` | `freshness_threshold_minutes` (default 15) |
| `ProducerSettings` | `PRODUCER_` | `events_per_second`, `simulated_users`, `simulated_videos` |
| `AppSettings` | none | `app_env`, `log_level` |

`.env.example` at the repo root mirrors every one of these fields with its default value —
copying it to `.env` with no edits is a valid local setup.

---

## 9. dbt Project (`streamcore_dbt/`) — Targets ClickHouse

This project originally targeted BigQuery on the roadmap; it now targets **ClickHouse** instead.
Nothing upstream changed to make this work — Kafka, the consumer, and the Spark job still write
to Postgres exactly as before.

### 9.1 The Postgres bridge (why no new ingestion pipeline was needed)

`macros/postgres_bridge.sql` defines two macros, wired into `dbt_project.yml`'s `on-run-start`
hooks (two macros because dbt runs each `on-run-start` entry as one statement, and
`CREATE DATABASE` / `CREATE TABLE` can't be combined):

1. `create_raw_events_database()` — `CREATE DATABASE IF NOT EXISTS streamcore_raw` inside
   ClickHouse.
2. `create_raw_events_bridge_table()` — creates `streamcore_raw.events` **inside ClickHouse**
   using ClickHouse's `PostgreSQL` table engine, pointed at the real Postgres table. This is a
   **live federated view**, not a copy — every query against it round-trips to Postgres. The
   embedded host is hardcoded to `'postgres:5432'` because that string is resolved by the
   *ClickHouse server* (always on the Docker network), not by whichever machine happens to run
   `dbt build` (a developer's laptop, or the Airflow container) — so it stays correct either way.

Both hooks are idempotent (`IF NOT EXISTS`) since they run on every single `dbt build`.

`source('streamcore_raw', 'events')` in `models/staging/sources.yml` then reads through this
bridge exactly like any normal ClickHouse table.

### 9.2 Dialect differences from Postgres

Because the bridge table's `payload` column arrives as a plain ClickHouse `String` (raw JSON
text, not a native JSONB type), and because ClickHouse's SQL dialect differs from Postgres's in
several other places, every model had to be written (not just copy-pasted) for ClickHouse:

| Postgres | ClickHouse | Used in |
|---|---|---|
| `payload->>'field'` / `(payload->>'x')::int` | `JSONExtractString(payload, 'field')`, `JSONExtractInt(...)`, also `JSONExtractBool`/`JSONExtractFloat`, and `JSONExtract(payload, 'field', 'Nullable(String)')` for genuinely-optional fields | `stg_video_views.sql`, `stg_watch_progress.sql` |
| `extract(epoch from (a - b))` | `dateDiff('second', b, a)` | `int_watch_sessions.sql` |
| `count(*) filter (where ...)` | `countIf(...)` | `int_watch_sessions.sql`, both marts |
| `x::numeric / y` | `x / y` (ClickHouse's `/` is always float division, no cast needed) | `int_watch_sessions.sql` |
| `nullif(x, y)` | `nullIf(x, y)` (camelCase) | `int_watch_sessions.sql` |

### 9.3 `generate_schema_name` override

`macros/generate_schema_name.sql` overrides dbt's default behavior, which otherwise
*concatenates* the profile's base schema with each model's `+schema` config (e.g.
`streamcore_staging` would become `<base_schema>_streamcore_staging`). Every model here sets an
explicit `+schema`, and the override makes that name used exactly as written. This is dbt's own
documented pattern for this exact situation.

### 9.4 Layering

Materializations are set once in `dbt_project.yml`, not per-model:

**Staging** (views, schema `streamcore_staging`) — 1:1 cleaned windows over the raw source, no
joins or aggregation:
- `stg_video_views.sql` — one row per `video_view` event, all payload fields extracted and typed.
- `stg_watch_progress.sql` — one row per `watch_progress` event.

**Intermediate** (tables, schema `streamcore_intermediate`):
- `int_watch_sessions.sql` — the load-bearing model almost every mart builds on. Aggregates all
  progress ticks per `(session_id, video_id)` into one row: `watch_duration_seconds`
  (`dateDiff`), `completion_pct` (`max_position_seconds / video_duration_seconds`, capped at
  100), `is_completed` (≥ 90% watched), `buffering_rate_pct`, `dominant_quality`,
  `avg_playback_rate` — then left-joins back to the originating view event for video/device/
  country metadata.

**Marts** (tables, schema `streamcore_marts`):
- `mart_content_performance.sql` — per-video: `total_views`, `unique_viewers`,
  `avg_completion_pct`, `completion_rate_pct`, device/traffic-source breakdowns, plus
  `rank() over (...)` window functions for views and completion. Audience: content/product.
- `mart_device_quality.sql` — per `(device_type, country_code)`: buffering stats, quality-level
  distribution (4k/1080p/720p/low), and a `buffering_status` flag (`critical` > 15%,
  `warning` > 8%, else `healthy`). Audience: engineering.

### 9.5 Two dbt profiles, one difference

`streamcore_dbt/profiles.example.yml` (copy to `~/.dbt/profiles.yml` for host-side use) connects
to ClickHouse at `localhost:8123`. `airflow/profiles/profiles.yml` connects to the `clickhouse`
service by its Docker network hostname instead — used only inside the Airflow container. Nothing
else differs between them; both read credentials from environment variables.

---

## 10. Data Quality (Slice 6) — Three Independent Layers

Each layer catches something the other two structurally cannot:

**1. dbt source freshness** (`streamcore_dbt/models/staging/sources.yml`) — `loaded_at_field:
ingested_at` (when *we* received the event, not when it happened — catches a stuck consumer,
not simulated event-time drift), `warn_after: 15 min`, `error_after: 60 min`. Run via
`dbt source freshness`. Only checks staleness, and needs the ClickHouse bridge table to already
exist (source freshness doesn't run `on-run-start` hooks by default in this dbt version), so a
`dbt build` must have run at least once first.

**2. dbt singular tests** (`streamcore_dbt/tests/*.sql`) — three assertions that run as part of
every `dbt build`:
- `assert_completion_pct_in_range.sql` — `completion_pct` must stay in `[0, 100]`.
- `assert_buffering_rate_pct_in_range.sql` — same for `buffering_rate_pct`.
- `assert_no_negative_watch_duration.sql` — `watch_duration_seconds` must never be negative.

**3. `quality/checks.py`** (entry point: `scripts/run_data_quality_checks.py`) — the only layer
that runs **independently of dbt entirely**:
- `check_raw_events_freshness()` — queries Postgres directly for
  `max(ingested_at)` on `streamcore_raw.events`, compared against
  `DataQualitySettings.freshness_threshold_minutes`. This is the root-cause signal: if the
  producer/consumer die, everything downstream (Spark aggregates, the ClickHouse bridge, every
  dbt model) goes stale too — checking here catches it earliest, before dbt is even involved.
- `check_mart_tables_not_empty()` — queries ClickHouse directly for row counts on
  `mart_content_performance` and `mart_device_quality`. Exists specifically because `dbt build`
  can report "Completed successfully" even when a mart ends up empty (an empty result set isn't
  a SQL error) — e.g. if the bridge table silently failed to connect.
- `run_all_checks()` returns every result (not just failures); `scripts/run_data_quality_checks.py`
  logs each one via `structlog` and exits non-zero if any failed.

Runs as the second task in the Airflow DAG (`data_quality_checks`, after `dbt_build`) — see §11.

---

## 11. Airflow Batch Orchestration (`airflow/`)

Its own docker-compose service, built from `airflow/Dockerfile` (the official
`apache/airflow:2.9.3-python3.11` image plus `dbt-clickhouse`, `psycopg[binary]`,
`clickhouse-connect`, `pydantic`, `pydantic-settings`, `python-dotenv`, `structlog` —
deliberately **not** the whole app via `pip install -e .`, which would also drag in
`pyspark`/`confluent-kafka` for no reason). Runs in Airflow's `standalone` mode: one container,
webserver + scheduler + auto-created admin user.

`airflow/dags/streamcore_dbt_dag.py` — DAG `streamcore_dbt_batch`, `@hourly`, `catchup=False`,
two `BashOperator` tasks:

```
dbt_build  >>  data_quality_checks
```

- `dbt_build`: `cd streamcore_dbt && dbt build --profiles-dir /opt/airflow/dbt_profiles` — this
  is `dbt run` + `dbt test` in dependency order, so a broken model or a failed singular/schema
  test fails the task instead of silently shipping bad data to the marts.
- `data_quality_checks`: `cd /opt/airflow/streamcore && python -m scripts.run_data_quality_checks`
  — needs `producers.*`/`quality.*`/`scripts.*` importable, which is why docker-compose bind-mounts
  `./producers`, `./quality`, `./scripts` (all read-only) into the container and sets
  `PYTHONPATH=/opt/airflow/streamcore`.

`BashOperator` (shelling out) was chosen over a dedicated dbt provider like `astronomer-cosmos`
deliberately — Cosmos turns each dbt model into its own Airflow task with full lineage in the UI
(the production-grade answer), but that's a second tool to learn on top of Airflow itself; this
starts with the simplest thing that works.

Airflow's own metadata tables share the same Postgres instance as the app data, landing in the
default `public` schema (not `streamcore_raw`/`streamcore_aggregated`), so they don't collide.

**Two Apple Silicon / Docker notes worth knowing if you touch this service:**
- No `platform: linux/amd64` pin on the `airflow` service (unlike every other service in
  `docker-compose.yml`) — the official image publishes native `linux/arm64` builds, and forcing
  `amd64` here means every one of Airflow's processes runs under Rosetta emulation, which made
  first boot pathologically slow in practice.
- The `streamcore_dbt` bind mount is **not** `:ro` — dbt writes its own `logs/`/`target/` output
  back into that directory (both gitignored); a read-only mount there makes `dbt build` fail
  with `OSError: [Errno 30] Read-only file system`.

---

## 12. Metabase Dashboards (Slice 6)

The `metabase` docker-compose service connects to **Postgres**, not ClickHouse — Metabase ships
a native Postgres driver, but ClickHouse needs an extra driver plugin it doesn't bundle by
default, so visualizing `streamcore_aggregated.*` (the Spark job's real-time tables — exactly
what they're built for) was the zero-extra-setup option. Visualizing the ClickHouse marts would
need the `metabase-clickhouse-driver` plugin JAR dropped into a mounted plugins directory — not
done here, since it requires downloading a binary from GitHub releases rather than anything
scriptable through Docker Compose alone.

Metabase uses its own embedded H2 app-db (`MB_DB_FILE=/metabase-data/metabase.db`) rather than a
second Postgres instance — simplest choice for local dev — persisted via the `metabase-data`
named volume so dashboards survive a container recreate.

**Operational note on that H2 file**: Metabase's own runtime stores the actual `.mv.db`/
`.trace.db` files *inside* a directory literally named `metabase.db` (i.e.
`/metabase-data/metabase.db/metabase.db.mv.db`), not as a flat file at the path given by
`MB_DB_FILE`. Any ad-hoc H2 CLI query against the literal `MB_DB_FILE` path (skipping Metabase's
own internal path handling) silently creates and queries a *separate, empty* database instead of
erroring — worth knowing before concluding "the database is empty" from a raw H2 query. The
`java -jar metabase.jar reset-password <email>` CLI subcommand has the same issue. To interact
with the real database directly (e.g. to recover from a lost admin password), stop the container
first to release its file lock, then point an H2 shell explicitly at the nested path.

---

## 13. Docker Compose Stack (`docker-compose.yml`)

| Service | Image | Port(s) | Purpose |
|---|---|---|---|
| `zookeeper` | `confluentinc/cp-zookeeper:7.7.0` | internal | Kafka cluster coordination |
| `kafka` | `confluentinc/cp-kafka:7.7.0` | `9092` | Single-broker Kafka |
| `kafka-ui` | `provectuslabs/kafka-ui:latest` | `8080` | Browser UI for topics/messages |
| `postgres` | `postgres:16-alpine` | `5432` | Raw events + streaming aggregates + Airflow metadata |
| `clickhouse` | `clickhouse/clickhouse-server:24.3` | `8123` (HTTP/dbt), `9000` (native) | dbt's warehouse |
| `airflow` | built from `./airflow` | `8081`→`8080` | Hourly `dbt build` + data-quality task |
| `metabase` | `metabase/metabase:v0.50.34` | `3000` | Dashboards on Postgres |

All services except `airflow` are pinned to `platform: linux/amd64` (see §11 for why `airflow`
is the exception). Postgres auto-runs everything in `./infra/postgres/` on first volume init.
Named volumes: `postgres-data`, `clickhouse-data`, `airflow-logs`, `metabase-data`.

---

## 14. Tests (`tests/`, `pytest -q`, 37 tests total)

| File | Covers |
|---|---|
| `test_simulator.py` | Event immutability, country-code validation, session event ordering (view → progress events, 10s increments), infinite generator behavior |
| `test_topic_registry.py` | Event → topic routing, `KeyError` on unregistered event types |
| `test_kafka_client.py` | `KafkaProducerClient` serialization, delivery callback handling, key-by-`user_id` partitioning |
| `test_postgres_sink.py` | Batch buffering/flush threshold, `ON CONFLICT DO NOTHING` upsert shape |
| `test_consumer.py` | `StreamCoreConsumer` message handling, and a **regression test for the offset-commit bug** (§5.2) — asserts `commit()` is actually called, not just `store_offsets()` |
| `test_quality_checks.py` | `quality/checks.py`'s freshness and mart-volume checks, mocking `psycopg`/`clickhouse_connect` |

All mock external I/O (`confluent_kafka`, `psycopg`, `clickhouse_connect`) — no test requires the
Docker stack to be running.

---

## 15. How to Run Locally

```bash
cp .env.example .env
docker compose up -d

python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Three long-lived processes, one per terminal:
python -m scripts.run_producer
python -m scripts.run_consumer
python -m scripts.run_streaming

# dbt, once data has flowed through:
pip install -e ".[dbt]"
cp streamcore_dbt/profiles.example.yml ~/.dbt/profiles.yml
cd streamcore_dbt && dbt build

# Standalone data-quality check + dashboards:
python -m scripts.run_data_quality_checks
open http://localhost:3000   # Metabase
open http://localhost:8081   # Airflow UI
open http://localhost:8080   # Kafka UI
```

`pytest -q`, `ruff check .`, `mypy .` for tests/lint/types — see `CLAUDE.md` for the full command
reference, including single-test invocation and dbt-specific commands.

---

## 16. Teaching Explanation: How the Pieces Fit Together

Imagine a streaming app like Netflix. When a user presses play, the app emits a `video_view`
event; while the video plays, it emits `watch_progress` events every few seconds. `StreamCore`
simulates that behavior locally, at whatever rate `PRODUCER_EVENTS_PER_SECOND` is set to.

Kafka is the durable event highway — producers write once, and two independent readers (the
consumer, and the Spark job) each read the full stream at their own pace, unaware of each other.

Postgres plays two distinct roles: the bronze/raw landing zone (`streamcore_raw.events`,
untouched JSONB, kept forever for reprocessing) and the silver/real-time aggregate layer
(`streamcore_aggregated.*`, continuously upserted by Spark).

ClickHouse+dbt is the batch analytical/gold layer: rather than duplicate ingestion, ClickHouse
simply queries live through to the same Postgres raw table via a federated table engine, and dbt
builds proper dimensional models (staging → intermediate → marts) on top of that single source
of truth. Airflow's only job is to run that batch step on a schedule and give one place to see
whether the last run succeeded — the always-on streaming path doesn't depend on Airflow being up
at all.

Data quality is deliberately layered three ways rather than trusting any single signal: dbt's own
test framework catches bad *values*; the standalone `quality/` checks catch a *stopped pipeline*
or a *silently empty* mart — failure modes dbt's pass/fail alone can't see.

---

## 17. Notable Bugs Found and Fixed During Development

These are documented here because they were non-obvious and worth knowing about if you're
extending the code, not because they're still open:

1. **Kafka consumer offsets were never actually committed.** `store_offsets(msg)` was called
   without a following `commit()` — offsets were staged locally but never sent to the broker,
   so a restart would replay the entire topic. Fixed in `consumers/core/consumer.py`; regression
   test in `tests/test_consumer.py`.
2. **`pyspark` resolved to an incompatible major version.** An unbounded `pyspark>=3.5.0` in
   `pyproject.toml` resolved to 4.2.0, which crashes the streaming job at runtime
   (`NoSuchMethodError: scala.Predef$.wrapRefArray`) against the hardcoded Scala 2.12 Kafka
   connector. Only surfaced by actually running the job end-to-end. Fixed by pinning
   `pyspark>=3.5.0,<4.0.0` (see §5.3).
3. **`pip install -e ".[dev]"` failed at the metadata step.** hatchling couldn't infer wheel
   contents because the project name (`streamcore`) doesn't match any single top-level package.
   Fixed by adding an explicit `[tool.hatch.build.targets.wheel] packages = [...]` list.
4. **A stray, uncommitted-looking dump of `.git` internals** (`HEAD`, `config`, `hooks/*.sample`,
   `objects/**`, `refs/**`) previously existed at the repo root alongside the real `.git/` —
   removed; `.gitignore` guards against recurrence.
5. **Legacy duplicate `core/`/`sinks/` packages** at the repo root (pre-`consumers/` restructure)
   were removed — everything now imports from `consumers.*`/`producers.*` only.

---

## 18. One-Sentence Summary

`StreamCore` is a local, educational, production-inspired streaming analytics platform that
generates fake video events, pushes them through Kafka, stores them in Postgres, computes
real-time viewing metrics with PySpark, and separately builds a ClickHouse/dbt warehouse with
its own orchestration (Airflow) and data-quality safety net — all runnable end-to-end on a
laptop.
