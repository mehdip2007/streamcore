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
                 └───────┬───────┘              └────────────────┘
                         │
                         ▼
                 ┌────────────────┐
                 │   BigQuery     │  ← Layer 4: Warehouse
                 └───────┬────────┘
                         │
                         ▼
                 ┌────────────────┐
                 │      dbt       │  ← Layer 5: Modeling
                 │ (staging/marts)│
                 └────────────────┘
                         │
                         ▼
                 ┌────────────────┐
                 │  Dashboards    │
                 │ + Data Quality │  ← Layer 6: Observability
                 └────────────────┘

         Orchestrated by Apache Airflow (Layer 3)
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Event Ingestion | Apache Kafka |
| Stream Processing | PySpark Structured Streaming |
| Orchestration | Apache Airflow |
| Storage | Postgres (local) → BigQuery (production) |
| Modeling | dbt |
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
├── streaming/               # Layer 2 — PySpark jobs (later)
├── airflow/                 # Layer 3 — DAGs (later)
├── dbt/                     # Layer 4 — Models (later)
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
```

## Build Roadmap

This project is built in **vertical slices** — each slice is end-to-end and shippable.

- [x] **Slice 1** — Event schemas + simulator + local stack
- [ ] **Slice 2** — Kafka producer + Postgres consumer
- [ ] **Slice 3** — PySpark streaming job
- [ ] **Slice 4** — Airflow batch DAGs
- [ ] **Slice 5** — dbt models on BigQuery
- [ ] **Slice 6** — Data quality + observability

## Design Principles

1. **Local first, cloud later** — everything runs on a laptop
2. **One layer at a time, end-to-end** — vertical slices, not horizontal
3. **Production patterns from day one** — env vars, structured logging, schemas
4. **Modular by design** — clear separation of concerns, single responsibility per module

## License

MIT

---

Built by [Mehdi Pourhadi](https://linkedin.com/in/mehdipourhadi)
