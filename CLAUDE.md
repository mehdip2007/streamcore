# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

StreamCore is a local-first, educational data engineering project simulating the data backbone of a
video streaming platform (think: a miniature Netflix/YouTube analytics pipeline). It is built in
**vertical slices** — each slice is end-to-end and shippable — rather than horizontally by layer.
Code favors verbose, teaching-style docstrings/comments explaining *why*, since the project doubles as
a portfolio/learning artifact. Match that style when editing existing modules.

## Commands

```bash
# Environment config (values already match producers/core/config.py's defaults)
cp .env.example .env

# Local infra (Kafka, Zookeeper, Kafka UI, Postgres, ClickHouse, Airflow)
docker compose up -d
docker compose down            # stop
docker compose down -v         # stop and wipe volumes (destructive)
open http://localhost:8080     # Kafka UI
open http://localhost:8081     # Airflow UI (admin password: docker compose logs airflow | grep password)
# ClickHouse HTTP interface (used by dbt): localhost:8123 — no browser UI by default

# Python env
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Run the three pipeline processes (each in its own terminal)
python -m scripts.run_producer
python -m scripts.run_consumer
python -m scripts.run_streaming

# Tests
pytest -q                      # whole suite (testpaths = tests/, per pyproject.toml)
pytest tests/test_simulator.py::TestSimulator::test_progress_events_in_order -q  # single test

# Lint / type check
ruff check .
mypy .

# dbt (must run from inside streamcore_dbt/ — no root-level dbt_project.yml exists)
pip install -e ".[dbt]"                      # installs dbt-core + dbt-clickhouse
cp streamcore_dbt/profiles.example.yml ~/.dbt/profiles.yml   # first time only
cd streamcore_dbt
dbt debug
dbt build   # = dbt run + dbt test, in dependency order
```

## Architecture

Data flow: **Producer (Python sim) → Kafka topics → Postgres raw table (`streamcore_raw.events`,
JSONB) → PySpark Structured Streaming aggregations → Postgres aggregate tables
(`streamcore_aggregated.*`) → ClickHouse (dbt's warehouse, staging → intermediate → marts, orchestrated
hourly by Airflow) → future dashboards**.

The producer, consumer, and Spark job are always-on — three long-lived processes started manually,
independent of Airflow, and they only ever talk to Postgres/Kafka. ClickHouse and dbt are a separate
analytical layer bolted on afterward: ClickHouse reads Postgres live through a table-engine bridge (see
the dbt project section below) rather than through any new ingestion code. Airflow (`airflow/dags/`)
only orchestrates the batch step (dbt); it is not part of the streaming path and the pipeline keeps
running whether or not Airflow is up.

### Event contracts (`producers/events/schemas.py`)
All events subclass `BaseEvent` (Pydantic, `frozen=True` — immutable once constructed) and carry
`event_id`, `event_type`, `event_timestamp`, `user_id`, `session_id`, `device_type`, `ip_address`,
`country_code`. Two concrete event types: `VideoViewEvent` (play started) and `WatchProgressEvent`
(periodic ~10s playback tick). These schemas are the contract between producer and every downstream
consumer (Postgres sink, Spark parsing schemas, dbt staging models) — changing a field means updating
all of those in lockstep, since nothing shares a single schema source across languages.

### Producer flow
`scripts/run_producer.py` → `producers/events/simulator.py` generates fake users/videos/sessions and
an infinite event stream → `producers/core/topic_registry.py` (`TopicRegistry`) maps each event class
to its Kafka topic (single source of truth — adding a new event type means: add schema, add topic env
var, register it in `TopicRegistry._routes`) → `producers/core/kafka_client.py` wraps
`confluent-kafka` for serialization/delivery/flush.

### Consumer flow
`scripts/run_consumer.py` → `consumers/core/consumer.py` (`StreamCoreConsumer`) polls Kafka and hands
raw payloads to a sink (strategy pattern — the sink type is pluggable, currently
`consumers/sinks/postgres_sink.py`). Offset handling is manual (`enable.auto.commit=False`): write to
Postgres first, then `store_offsets` + a synchronous `commit(message=msg, asynchronous=False)`, to get
at-least-once delivery instead of losing messages on crash. The root-level `core/` and `sinks/`
packages (legacy duplicates of `consumers/core` and `consumers/sinks`) have been removed — always
import from `consumers.*`.

### Streaming aggregation flow
`scripts/run_streaming.py` → `streaming/core/spark_session.py` builds a singleton `SparkSession` with
the Kafka connector → `streaming/jobs/watch_aggregator.py` defines three tumbling-window aggregations
over Kafka topics (concurrent viewers/30s, buffering rate by device/1min, top videos/5min), each with
its own watermark, and writes every micro-batch to Postgres via `foreachBatch` using a dynamically
built `INSERT ... ON CONFLICT DO UPDATE` upsert (see `make_postgres_writer`). Spark schemas here
(`VIEW_EVENT_SCHEMA`, `PROGRESS_EVENT_SCHEMA`) must stay in sync with the Pydantic event schemas by
hand — there is no shared schema registry.

### Configuration (`producers/core/config.py`)
Pydantic-settings classes, each `@lru_cache`d into a singleton: `KafkaSettings` (`KAFKA_*`),
`PostgresSettings` (`POSTGRES_*`, exposes `.dsn`), `ProducerSettings` (`PRODUCER_*`), `AppSettings`
(no prefix). All read from `.env`. Fail-fast: invalid/missing config raises at startup, not later.

### Postgres schema (`infra/postgres/*.sql`, auto-run by docker-compose on first volume init)
- `01_init_schema.sql` — `streamcore_raw.events`: append-only raw Kafka payloads as JSONB, plus Kafka
  metadata (topic/partition/offset) and indexes (event_type+timestamp, ingested_at, GIN on payload).
- `02_streaming_schema.sql` — `streamcore_aggregated.*`: the three tables the Spark job upserts into
  (`concurrent_viewers`, `buffering_rate`, `top_videos`), each keyed by `(window_start, <dimension>)`.

### dbt project (`streamcore_dbt/`) — targets ClickHouse
Separate project rooted at `streamcore_dbt/dbt_project.yml` — dbt commands only work run from inside
that directory, and need `dbt-clickhouse` installed (`pip install -e ".[dbt]"`; kept out of the main
dependency list since it's unrelated to the app's runtime — same reasoning as Airflow below).

**Why ClickHouse instead of querying Postgres directly**: the warehouse layer originally planned for
BigQuery now targets ClickHouse. Nothing upstream changes — Kafka, the consumer, and the Spark job
still write to Postgres exactly as before. `macros/postgres_bridge.sql`'s `on-run-start` hooks (wired
in `dbt_project.yml`) create a `streamcore_raw.events` table *inside ClickHouse* using ClickHouse's
`PostgreSQL` table engine — a live federated view of the real Postgres table, not a copy — so
`source('streamcore_raw', 'events')` reads through it like any other table, with zero new ingestion
code. Because the bridge table's `payload` column arrives as a plain `String` (raw JSON text, not a
native JSONB type), staging models parse it with ClickHouse's `JSONExtractString`/`JSONExtractInt`/
`JSONExtractBool`/`JSONExtractFloat` functions instead of Postgres's `->>`/`::type` operators.
`macros/generate_schema_name.sql` overrides dbt's default schema-naming (which would otherwise
concatenate the profile's base schema with each model's `+schema` config) so custom schemas resolve to
exactly the names below — this is dbt's own documented pattern for this situation.

Layering, each with its own default materialization set in `dbt_project.yml`:
- `models/staging/` (views, schema `streamcore_staging`) — `stg_video_views`, `stg_watch_progress`,
  cleaned 1:1 windows over the `streamcore_raw.events` source (declared in `sources.yml`).
- `models/intermediate/` (tables, schema `streamcore_intermediate`) — `int_watch_sessions`: joins view
  + progress events into full watch sessions (completion %, buffering, quality); nearly every mart
  builds on this. Uses ClickHouse's `dateDiff`/`countIf`/`nullIf` in place of Postgres's
  `extract(epoch from ...)`/`FILTER (WHERE ...)`/`nullif`.
- `models/marts/` (tables, schema `streamcore_marts`) — `mart_content_performance` (views/completion
  by video, for content/product) and `mart_device_quality` (buffering health by device/country, for
  engineering).

Two profiles exist because the ClickHouse *connection* host differs by where `dbt` runs from — see
`streamcore_dbt/profiles.example.yml` (host machine, `localhost:8123`) vs `airflow/profiles/profiles.yml`
(inside the airflow container, Docker network hostname `clickhouse:8123`). The bridge table's embedded
Postgres host is unrelated to this and always hardcoded to `postgres:5432` (see the macro's comment for
why — that string is resolved by the ClickHouse *server*, not by whatever machine invoked `dbt build`).

### Airflow batch orchestration (`airflow/`)
Runs as its own docker-compose service (`airflow`), built from `airflow/Dockerfile` (official Airflow
image + `dbt-clickhouse`) rather than as a `pyproject.toml` dependency — Airflow's own dependency pinning
is heavy and unrelated to the app's runtime. `airflow/dags/streamcore_dbt_dag.py` runs `dbt build`
hourly via `BashOperator` against `streamcore_dbt/` (bind-mounted read-only into the container).
`airflow/profiles/profiles.yml` is a *separate* dbt profile used only inside that container — see the
dbt project section above for why it differs from a developer's host-side profile. The `airflow`
service runs in `standalone` mode (single container: webserver + scheduler + auto-created admin user)
and shares the same Postgres instance as the app data for its own metadata — those tables land in the
default `public` schema, not `streamcore_raw`/`streamcore_aggregated`, so they don't collide.
