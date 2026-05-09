# StreamCore Project Documentation

> Confluence-style documentation generated from a project scan.

## 1. Executive Summary

`StreamCore` is a local-first data engineering project that simulates the data platform behind a video streaming product.

At a high level, it does this:

1. Generates realistic video-watching events in Python.
2. Sends those events into Apache Kafka.
3. Consumes Kafka events and stores raw event payloads in Postgres.
4. Runs PySpark Structured Streaming jobs that calculate real-time analytics.
5. Stores streaming aggregates back into Postgres.
6. Includes a starter dbt project for future transformation/modeling work.

Think of it as a miniature Netflix/YouTube analytics pipeline built for learning and portfolio demonstration.

---

## 2. Mental Model: What Problem Is This Solving?

A video streaming platform needs to answer questions like:

- How many people are watching each video right now?
- Which devices are buffering the most?
- Which videos are trending in the last 5 minutes?
- What raw user behavior happened so analysts can model it later?

`StreamCore` models the data backbone for this.

The system does not serve videos. Instead, it focuses on the data events produced by a streaming platform.

Example event types:

- `video_view`: user starts watching a video.
- `watch_progress`: user is still watching and sends periodic progress updates.

---

## 3. Current Architecture

### 3.1 Data Flow

Producer simulator → Kafka topics → Postgres raw table → PySpark streaming aggregations → Postgres aggregate tables → future dbt/dashboard layer

### 3.2 Components

| Layer | Component | Purpose | Current Status |
|---|---|---|---|
| Event generation | Python simulator | Creates realistic user/video/session events | Implemented |
| Ingestion | Kafka | Buffers and distributes events | Implemented through Docker Compose |
| Raw storage | Postgres | Stores original Kafka payloads as JSONB | Implemented |
| Stream processing | PySpark Structured Streaming | Computes live metrics | Implemented |
| Modeling | dbt | Future warehouse-style transformations | Starter project only |
| Orchestration | Airflow | Future scheduled jobs | Directory exists, not implemented |
| Observability | Logs, tests, Kafka UI | Local visibility into system behavior | Partially implemented |

---

## 4. Repository Structure

| Path | Meaning |
|---|---|
| `README.md` | Human-facing overview, architecture, quick start, and roadmap |
| `pyproject.toml` | Python package metadata, dependencies, pytest config, linting config |
| `docker-compose.yml` | Local infrastructure: Zookeeper, Kafka, Kafka UI, Postgres |
| `.env` | Environment configuration file; values were intentionally not copied into this documentation |
| `producers/` | Python event simulator and Kafka producer client |
| `consumers/` | Kafka consumer and Postgres sink implementation |
| `streaming/` | PySpark Structured Streaming session factory and aggregation job |
| `infra/postgres/` | SQL initialization scripts for raw and aggregate schemas |
| `scripts/` | CLI entry points for producer, consumer, and streaming job |
| `tests/` | Unit tests for schemas and simulator behavior |
| `streamcore_dbt/` | Starter dbt project |
| `airflow/dags/` | Placeholder for future Airflow DAGs |
| `docs/` | Project documentation, including this file |
| `dbt/` | Empty placeholder; actual dbt project currently lives in `streamcore_dbt/` |
| `logs/` and `streamcore_dbt/logs/` | dbt execution/debug logs |
| `HEAD`, `config`, `hooks/`, `objects/`, `refs/`, `info/` | Git repository metadata present at project root; not application code |

---

## 5. Main Runtime Flows

### 5.1 Producer Flow

Entry point: `scripts/run_producer.py`

Flow:

1. Configure structured logging.
2. Load producer settings from `.env` through Pydantic settings.
3. Create a Kafka producer client.
4. Create a topic registry.
5. Create an infinite event generator.
6. For each event:
   - Determine the correct Kafka topic.
   - Serialize the Pydantic event model to JSON.
   - Send the event to Kafka using `user_id` as the message key.
   - Sleep based on configured events-per-second.
7. On shutdown, flush the Kafka producer so buffered events are not lost.

Important files:

| File | Responsibility |
|---|---|
| `scripts/run_producer.py` | Starts the event producer loop |
| `producers/events/simulator.py` | Generates fake users, videos, sessions, and event streams |
| `producers/events/schemas.py` | Defines event contracts using Pydantic |
| `producers/core/kafka_client.py` | Wraps `confluent-kafka` producer |
| `producers/core/topic_registry.py` | Maps event classes to Kafka topics |
| `producers/core/config.py` | Loads app, Kafka, Postgres, and producer configuration |
| `producers/core/logging_setup.py` | Configures structured logs through `structlog` |

### 5.2 Consumer Flow

Entry point: `scripts/run_consumer.py`

Flow:

1. Configure structured logging.
2. Connect to Postgres.
3. Read all registered Kafka topic names from `TopicRegistry`.
4. Subscribe to those topics.
5. Poll Kafka in a loop.
6. For each message:
   - Decode JSON payload.
   - Extract event metadata.
   - Write event to Postgres raw table.
   - Store/commit offset intent.
7. On shutdown, flush pending Postgres rows and close the Kafka consumer.

Important files:

| File | Responsibility |
|---|---|
| `scripts/run_consumer.py` | Starts the consumer process |
| `consumers/core/consumer.py` | Reads Kafka messages and routes them to a sink |
| `consumers/sinks/postgres_sink.py` | Batches raw events and inserts them into Postgres |
| `infra/postgres/01_init_schema.sql` | Creates `streamcore_raw.events` |

### 5.3 Streaming Aggregation Flow

Entry point: `scripts/run_streaming.py`

Flow:

1. Configure structured logging.
2. Create a singleton SparkSession.
3. Read Kafka streams using Spark Structured Streaming.
4. Parse Kafka JSON payloads into typed Spark DataFrames.
5. Compute three aggregations:
   - Concurrent viewers per video.
   - Buffering rate by device type.
   - Top videos by view count.
6. Write each micro-batch into Postgres using `foreachBatch` and upsert logic.

Important files:

| File | Responsibility |
|---|---|
| `scripts/run_streaming.py` | Starts the PySpark streaming job |
| `streaming/core/spark_session.py` | Creates/caches SparkSession with Kafka connector |
| `streaming/jobs/watch_aggregator.py` | Defines Kafka readers, JSON parsers, aggregations, and Postgres writers |
| `infra/postgres/02_streaming_schema.sql` | Creates aggregate tables |

---

## 6. Data Contracts

### 6.1 Base Event

All events inherit from `BaseEvent` in `producers/events/schemas.py`.

Common fields:

| Field | Meaning |
|---|---|
| `event_id` | Unique event identifier |
| `event_type` | Event discriminator, such as `video_view` or `watch_progress` |
| `event_timestamp` | UTC event time |
| `user_id` | User who generated the event |
| `session_id` | Watching session identifier |
| `device_type` | Device category |
| `ip_address` | Simulated IP address |
| `country_code` | Two-character country code |

The Pydantic model is frozen, which means events are immutable after creation.

### 6.2 `VideoViewEvent`

Represents the moment a user starts watching a video.

Additional fields:

| Field | Meaning |
|---|---|
| `video_id` | Video identifier |
| `video_title` | Simulated video title |
| `video_duration_seconds` | Total video length |
| `initial_quality` | Starting playback quality |
| `referrer` | Source such as homepage, search, recommendation, or null |

Kafka topic by default: `streamcore.video.views`

### 6.3 `WatchProgressEvent`

Represents ongoing playback progress while the user watches.

Additional fields:

| Field | Meaning |
|---|---|
| `video_id` | Video identifier |
| `position_seconds` | Current playback position |
| `quality` | Current playback quality |
| `is_buffering` | Whether playback is buffering |
| `playback_rate` | Playback speed |

Kafka topic by default: `streamcore.video.watch_progress`

---

## 7. Database Design

### 7.1 Raw Layer

Defined in `infra/postgres/01_init_schema.sql`.

Schema: `streamcore_raw`

Table: `streamcore_raw.events`

Purpose: store original Kafka events without losing detail.

Key design choice: event payloads are stored as `JSONB`.

Why this matters:

- Event schemas can evolve without immediately changing table columns.
- Raw data is preserved for reprocessing.
- Consumers and analytics can query nested fields when needed.

Important columns:

| Column | Purpose |
|---|---|
| `id` | Internal surrogate key |
| `event_id` | Unique producer event id |
| `event_type` | Event type discriminator |
| `event_timestamp` | When the event happened |
| `payload` | Full original event as JSONB |
| `ingested_at` | When Postgres received the event |
| `kafka_topic` | Source Kafka topic |
| `kafka_partition` | Source Kafka partition |
| `kafka_offset` | Source Kafka offset |

Indexes:

- Event type + timestamp index.
- Ingestion timestamp index.
- JSONB GIN index for querying inside payloads.

### 7.2 Aggregated Layer

Defined in `infra/postgres/02_streaming_schema.sql`.

Schema: `streamcore_aggregated`

Tables:

| Table | Purpose | Primary Key |
|---|---|---|
| `concurrent_viewers` | Count unique sessions watching each video in 30-second windows | `window_start`, `video_id` |
| `buffering_rate` | Calculate buffering percentage by device type in 1-minute windows | `window_start`, `device_type` |
| `top_videos` | Count video views in 5-minute windows | `window_start`, `video_id` |

---

## 8. Configuration

Configuration is handled through `producers/core/config.py` using `pydantic-settings`.

Configuration groups:

| Settings class | Environment prefix | Purpose |
|---|---|---|
| `KafkaSettings` | `KAFKA_` | Kafka brokers, topic names, client id |
| `PostgresSettings` | `POSTGRES_` | Host, port, database, user, password, DSN |
| `ProducerSettings` | `PRODUCER_` | Event rate, number of simulated users/videos |
| `AppSettings` | none | App environment and log level |

The project contains a `.env` file. I intentionally treated it as sensitive configuration and did not copy values into this document.

Important note: `README.md` references `.env.example`, but the scan found `.env`, not `.env.example`.

---

## 9. Docker Compose Stack

Defined in `docker-compose.yml`.

Services:

| Service | Image | Purpose | Local Port |
|---|---|---|---|
| `zookeeper` | `confluentinc/cp-zookeeper:7.7.0` | Kafka coordination | internal only |
| `kafka` | `confluentinc/cp-kafka:7.7.0` | Local Kafka broker | `9092` |
| `kafka-ui` | `provectuslabs/kafka-ui:latest` | Browser UI for Kafka topics/messages | `8080` |
| `postgres` | `postgres:16-alpine` | Local database for raw and aggregate tables | `5432` |

The Postgres service mounts `./infra/postgres` into `/docker-entrypoint-initdb.d`, so the SQL files are auto-executed when the database volume is first initialized.

---

## 10. dbt Area

The actual dbt project is under `streamcore_dbt/`.

Current state:

| File/Directory | Purpose |
|---|---|
| `streamcore_dbt/dbt_project.yml` | dbt project configuration |
| `streamcore_dbt/README.md` | Default starter dbt README |
| `streamcore_dbt/models/example/my_first_dbt_model.sql` | Starter model with sample rows |
| `streamcore_dbt/models/example/my_second_dbt_model.sql` | Starter model referencing the first model |
| `streamcore_dbt/models/example/schema.yml` | dbt tests and model descriptions |
| `streamcore_dbt/analyses`, `macros`, `seeds`, `snapshots`, `tests` | Standard dbt folders with `.gitkeep` placeholders |

The dbt models are currently starter examples, not StreamCore-specific analytics models yet.

The logs show two useful facts:

1. Running dbt from the repository root failed because no root-level `dbt_project.yml` exists.
2. Running dbt from `streamcore_dbt/` later succeeded in `dbt debug` with user `streamcore`.

---

## 11. Tests

Test file: `tests/test_simulator.py`

Coverage areas:

| Test area | What it checks |
|---|---|
| Schema immutability | Events cannot be modified after creation |
| Country code validation | Country code must be exactly two characters |
| Session event order | A session starts with a `VideoViewEvent`, followed by progress events |
| Progress ordering | Watch progress positions increase by 10 seconds |
| Infinite generator | Event generator keeps yielding events |

I attempted to run `pytest -q`, but the current shell does not have `pytest` installed or available, so tests could not be executed in this environment.

---

## 12. File-by-File Explanation

### Root files

| File | Explanation |
|---|---|
| `.env` | Local configuration; should not be committed with real secrets |
| `.gitignore` | Currently effectively empty |
| `HEAD` | Git metadata pointing to `refs/heads/main` |
| `README.md` | Main project overview and roadmap |
| `__init__.py` | Marks root as Python package; currently empty |
| `config` | Git repository metadata, not app configuration |
| `description` | Git repository description metadata |
| `docker-compose.yml` | Defines local Kafka/Zookeeper/Kafka UI/Postgres stack |
| `pyproject.toml` | Python dependencies and tooling configuration |

### Producer files

| File | Explanation |
|---|---|
| `producers/__init__.py` | Empty package marker |
| `producers/core/__init__.py` | Empty package marker |
| `producers/core/config.py` | Central settings module using Pydantic |
| `producers/core/kafka_client.py` | Kafka producer adapter with serialization, delivery callbacks, flush handling |
| `producers/core/logging_setup.py` | `structlog` setup for local console or JSON production logging |
| `producers/core/topic_registry.py` | Single source of truth for event-to-topic routing |
| `producers/events/__init__.py` | Empty package marker |
| `producers/events/schemas.py` | Pydantic event models/enums |
| `producers/events/simulator.py` | Realistic user/video/session event generator |

### Consumer files

| File | Explanation |
|---|---|
| `consumers/__init__.py` | Empty package marker |
| `consumers/core/__init__.py` | Empty package marker |
| `consumers/core/consumer.py` | Kafka consumer loop and sink routing |
| `consumers/sinks/__init__.py` | Empty package marker |
| `consumers/sinks/postgres_sink.py` | Batched Postgres writer for raw events |

### Duplicate/legacy-looking files

| File | Explanation |
|---|---|
| `core/__init__.py` | Empty package marker |
| `core/consumer.py` | Duplicate of the consumer implementation, with one config difference |
| `sinks/__init__.py` | Empty package marker |
| `sinks/postgres_sink.py` | Duplicate of `consumers/sinks/postgres_sink.py` |

The main scripts import from `consumers.*`, not from the top-level `core/` or `sinks/` packages. The top-level duplicates should be reviewed and probably removed or consolidated to avoid confusion.

### Streaming files

| File | Explanation |
|---|---|
| `streaming/core/spark_session.py` | Builds a singleton SparkSession configured with the Spark Kafka connector |
| `streaming/jobs/watch_aggregator.py` | Main Structured Streaming job with Kafka reads, parsing, aggregations, and Postgres writes |

### Scripts

| File | Explanation |
|---|---|
| `scripts/__init__.py` | Empty package marker |
| `scripts/run_producer.py` | CLI entry point for Kafka producer |
| `scripts/run_consumer.py` | CLI entry point for Kafka consumer |
| `scripts/run_streaming.py` | CLI entry point for PySpark streaming job |

### Infrastructure files

| File | Explanation |
|---|---|
| `infra/postgres/01_init_schema.sql` | Creates raw event schema/table/indexes |
| `infra/postgres/02_streaming_schema.sql` | Creates aggregate schema/tables |
| `infra/kafka/` | Empty placeholder for future Kafka topic configs |

### dbt files

| File | Explanation |
|---|---|
| `streamcore_dbt/.gitignore` | dbt-specific ignore rules |
| `streamcore_dbt/README.md` | Starter dbt README |
| `streamcore_dbt/dbt_project.yml` | dbt project definition |
| `streamcore_dbt/models/example/my_first_dbt_model.sql` | Starter example model |
| `streamcore_dbt/models/example/my_second_dbt_model.sql` | Starter model referencing first model |
| `streamcore_dbt/models/example/schema.yml` | dbt model tests and documentation |
| `streamcore_dbt/*/.gitkeep` | Keeps empty dbt directories in git |

### Tests and logs

| File | Explanation |
|---|---|
| `tests/__init__.py` | Empty package marker |
| `tests/test_simulator.py` | Tests event schemas and simulator behavior |
| `logs/dbt.log` | dbt debug log from repository root; shows root-level dbt project missing and an earlier DB auth failure |
| `streamcore_dbt/logs/dbt.log` | dbt debug log from inside dbt project; shows successful connection |

### Git metadata files

The scan found Git metadata at the project root:

- `HEAD`
- `config`
- `description`
- `hooks/`
- `objects/`
- `refs/`
- `info/exclude`

These are not StreamCore application files. They look like a bare Git repository structure mixed into the project root. This is worth reviewing because it is unusual for normal source trees.

---

## 13. Current Implementation Status

| Area | Status |
|---|---|
| Event schemas | Good foundation |
| Event simulation | Good foundation with realistic behavior |
| Kafka producer | Implemented with production-style adapter pattern |
| Kafka consumer | Implemented, but offset commit behavior needs review |
| Raw Postgres sink | Implemented with batch inserts and JSONB payloads |
| PySpark streaming | Implemented with three useful aggregations |
| Aggregate Postgres tables | Implemented |
| dbt | Starter project only |
| Airflow | Placeholder only |
| Kafka topic configs | Placeholder only |
| Tests | Present but not run in current shell because `pytest` is unavailable |
| Documentation | README plus this generated document |

---

## 14. Important Observations and Risks

### 14.1 Kafka consumer offset handling needs review

The consumer comments describe manual offset commits, but the implementation calls `store_offsets(msg)` and does not call `commit()`.

With `enable.auto.commit` set to `False`, offsets may not actually be committed to Kafka. This could cause the consumer to reprocess messages after restart.

Recommended fix direction:

- After a successful sink write, explicitly call `commit(message=msg, asynchronous=False)` or a suitable asynchronous commit strategy.
- Keep `enable.auto.offset.store=False` if you want full manual control.

### 14.2 Duplicate consumer/sink modules

There are duplicated files:

- `consumers/core/consumer.py`
- `core/consumer.py`
- `consumers/sinks/postgres_sink.py`
- `sinks/postgres_sink.py`

This can confuse future development. Pick one package layout and remove the duplicate.

### 14.3 `.env.example` is referenced but not present

`README.md` says to copy `.env.example` to `.env`, but the scan found `.env` only.

Recommended fix direction:

- Add `.env.example` with safe template values.
- Add `.env` to `.gitignore`.

### 14.4 `.gitignore` is effectively empty

The project should ignore at least:

- `.env`
- `.venv/`
- `__pycache__/`
- `.pytest_cache/`
- `logs/`
- `streamcore_dbt/logs/`
- Spark checkpoints
- dbt `target/` and `dbt_packages/`

### 14.5 dbt folder mismatch

The root has an empty `dbt/` directory, but the actual dbt project is `streamcore_dbt/`.

Recommended fix direction:

- Either rename `streamcore_dbt/` to `dbt/`, or update documentation to consistently point to `streamcore_dbt/`.

### 14.6 Airflow is not implemented yet

`airflow/dags/` exists but contains no DAGs. The README roadmap correctly describes Airflow as future work.

### 14.7 dbt models are still starter examples

The current dbt models do not model StreamCore event data yet.

Useful next dbt models could include:

- `stg_events`
- `stg_video_views`
- `stg_watch_progress`
- `mart_video_engagement`
- `mart_device_quality`

---

## 15. How to Run Locally

### 15.1 Start infrastructure

Command: `docker compose up -d`

Then open Kafka UI at `http://localhost:8080`.

### 15.2 Install Python dependencies

Command: `python -m venv .venv`

Command: `source .venv/bin/activate`

Command: `pip install -e ".[dev]"`

### 15.3 Run the producer

Command: `python -m scripts.run_producer`

### 15.4 Run the consumer

In a second terminal:

Command: `python -m scripts.run_consumer`

### 15.5 Run the streaming job

In a third terminal:

Command: `python -m scripts.run_streaming`

### 15.6 Run tests

After installing dev dependencies:

Command: `pytest -q`

### 15.7 Run dbt debug

From inside the dbt project:

Command: `cd streamcore_dbt`

Command: `dbt debug`

---

## 16. Teaching Explanation: How the Pieces Fit Together

Imagine a streaming app like Netflix.

When a user presses play, the app emits a `video_view` event. While the video keeps playing, the app emits `watch_progress` events every few seconds.

`StreamCore` simulates that behavior locally.

Kafka acts like a durable event highway. Producers write events to Kafka topics. Consumers and Spark jobs read from those topics independently.

Postgres has two roles:

1. It stores raw events exactly as received.
2. It stores processed aggregate metrics for dashboards.

PySpark Structured Streaming is the real-time analytics engine. It continuously reads Kafka events and updates metrics in small time windows.

dbt is intended to become the transformation and semantic modeling layer, but right now it is still a starter scaffold.

---

## 17. Recommended Next Steps

Priority order:

1. Add `.env.example` and update `.gitignore`.
2. Fix Kafka consumer offset commit behavior.
3. Remove duplicate `core/` and `sinks/` modules or consolidate imports.
4. Add unit tests for `TopicRegistry`, `KafkaProducerClient` serialization, and `PostgresSink` batching.
5. Add dbt models that actually read from `streamcore_raw.events` and `streamcore_aggregated.*`.
6. Add Airflow DAGs only after the pipeline pieces are stable.
7. Add a dashboard layer such as Metabase or Superset.
8. Add data quality checks for event freshness, schema validity, and aggregate sanity.

---

## 18. One-Sentence Summary

`StreamCore` is a local, educational, production-inspired streaming analytics platform that generates fake video events, sends them through Kafka, stores them in Postgres, and computes real-time viewing metrics with PySpark.
