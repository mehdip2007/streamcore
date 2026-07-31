# StreamCore

> The data brain behind a video streaming platform.

An end-to-end open source data engineering project demonstrating production-grade architecture for streaming analytics.

## Architecture

```
                                  ┌──────────────────┐
                                  │  Event Producer  │
                                  │  (Python sim)    │
                                  └────────┬─────────┘
                                           │
                                           ▼
                              ┌─────────────────────┐
                              │   Apache Kafka      │  ← Layer 1: Ingestion
                              │  (events stream)    │
                              └──────┬──────┬───────┘
                                     │      │
                         ┌───────────┘      └────────────┐
                         ▼                               ▼
                 ┌───────────────┐              ┌────────────────┐
                 │ PySpark       │              │ Postgres       │  ← Layer 2: Storage
                 │ Streaming Job │              │ (raw events)   │
                 └───────────────┘              └───────┬────────┘
                                                         │ ClickHouse's PostgreSQL
                                                         │ table engine (live query,
                                                         │ not a copy)
                                                         ▼
                                                 ┌────────────────┐
                                                 │  ClickHouse    │  ← Layer 4: Warehouse
                                                 └───────┬────────┘
                                                         │
                                                         ▼
                                                 ┌────────────────┐
                                                 │      dbt       │  ← Layer 5: Modeling
                                                 │ (staging/marts)│
                                                 └───────┬────────┘
                                                         │
                                                         ▼
                                                 ┌────────────────┐
                                                 │  Dashboards    │
                                                 │ + Data Quality │  ← Layer 6: Observability
                                                 └────────────────┘

         Orchestrated by Apache Airflow (Layer 3) — runs the dbt step hourly
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Event Ingestion | Apache Kafka |
| Stream Processing | PySpark Structured Streaming |
| Orchestration | Apache Airflow |
| Operational storage | Postgres (raw events + streaming aggregates) |
| Warehouse | ClickHouse (dbt's target — reads Postgres live via a table-engine bridge) |
| Modeling | dbt (`dbt-clickhouse`) |
| Observability | Custom data quality + Metabase |
| Container | Docker Compose |
| Language | Python 3.11+ |

## Project Structure

```
streamcore/
├── docs/                    # Architecture, data model, runbook
├── infra/                   # Kafka topic configs, SQL migrations
├── producers/               # Layer 1 — Event generation
│   ├── core/                # Config, logging, shared utilities
│   └── events/              # Event schemas, simulator
├── consumers/               # Layer 1 — Kafka consumer + Postgres sink
├── streaming/               # Layer 2 — PySpark Structured Streaming jobs
├── airflow/                 # Layer 3 — DAGs (batch dbt orchestration)
├── streamcore_dbt/          # Layer 4 — Models (staging/intermediate/marts)
├── tests/                   # Test suite
├── scripts/                 # Operational scripts
├── docker-compose.yml       # Local stack
├── pyproject.toml           # Python dependencies
└── .env.example             # Config template
```

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- ~4 GB free RAM for the local stack

### Setup

```bash
# 1. Clone and enter the project
git clone https://github.com/mehdip2007/streamcore.git
cd streamcore

# 2. Copy environment template
cp .env.example .env

# 3. Start local infrastructure
docker compose up -d

# 4. Verify Kafka is running
open http://localhost:8080  # Kafka UI

# 5. Install Python dependencies
python -m venv .venv
source .venv/bin/activate    # Linux/Mac
pip install -e ".[dev]"

# 6. Run the producer
python -m scripts.run_producer

# 7. (optional) Run dbt against ClickHouse once some data has flowed through
pip install -e ".[dbt]"
cp streamcore_dbt/profiles.example.yml ~/.dbt/profiles.yml
cd streamcore_dbt && dbt build
```

## Build Roadmap

This project is built in **vertical slices** — each slice is end-to-end and shippable.

- [x] **Slice 1** — Event schemas + simulator + local stack
- [x] **Slice 2** — Kafka producer + Postgres consumer
- [x] **Slice 3** — PySpark streaming job
- [x] **Slice 4** — Airflow batch DAGs (runs `dbt build` hourly)
- [x] **Slice 5** — dbt models on ClickHouse (staging/intermediate/marts, verified end-to-end against real
  pipeline data; see CLAUDE.md's dbt project section for how ClickHouse reads Postgres without a new
  ingestion pipeline)
- [ ] **Slice 6** — Data quality + observability (dashboards, freshness/anomaly checks beyond dbt's built-in column tests)

## Design Principles

1. **Local first, cloud later** — everything runs on a laptop
2. **One layer at a time, end-to-end** — vertical slices, not horizontal
3. **Production patterns from day one** — env vars, structured logging, schemas
4. **Modular by design** — clear separation of concerns, single responsibility per module

## License

MIT

---

Built by [Mehdi Pourhadi](https://linkedin.com/in/mehdipourhadi)
