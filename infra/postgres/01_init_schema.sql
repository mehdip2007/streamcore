-- ============================================================
-- StreamCore — Initial Schema
-- ============================================================
-- This file is auto-executed by Postgres on first startup
-- (mounted into /docker-entrypoint-initdb.d in docker-compose).
-- ============================================================

CREATE SCHEMA IF NOT EXISTS streamcore_raw;

-- Raw events landing zone — stores events as JSONB exactly as received.
-- This is the bronze/raw layer in a medallion architecture.
-- Transformations and modeling happen LATER (in dbt).
CREATE TABLE IF NOT EXISTS streamcore_raw.events (
    -- Surrogate key for our ingestion record
    id              BIGSERIAL PRIMARY KEY,

    -- Original event identifier from the producer
    event_id        TEXT NOT NULL UNIQUE,
    event_type      TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,

    -- The full event payload as JSONB — preserves everything
    payload         JSONB NOT NULL,

    -- Ingestion metadata — when WE received it (vs when it happened)
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Kafka tracking metadata
    kafka_topic     TEXT,
    kafka_partition INT,
    kafka_offset    BIGINT
);

-- Indexes for the most common query patterns
CREATE INDEX IF NOT EXISTS idx_events_type_time
    ON streamcore_raw.events (event_type, event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_events_ingested_at
    ON streamcore_raw.events (ingested_at DESC);

-- GIN index on the JSONB payload — enables fast lookups inside JSON.
-- Example: WHERE payload->>'user_id' = 'u_4521'
CREATE INDEX IF NOT EXISTS idx_events_payload_gin
    ON streamcore_raw.events USING GIN (payload);

COMMENT ON TABLE streamcore_raw.events IS
    'Raw events from Kafka — bronze layer in medallion architecture';
