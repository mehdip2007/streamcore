-- ============================================================
-- StreamCore — Streaming Aggregations Schema
-- ============================================================
-- This is the SILVER LAYER — processed, aggregated data.
-- Written by PySpark Structured Streaming jobs.
-- Read by dashboards, analysts, and dbt models.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS streamcore_aggregated;

-- ----- Concurrent Viewers -----
-- How many unique sessions are actively watching each video
-- in a given 30-second window. Answers: "what's live right now?"
CREATE TABLE IF NOT EXISTS streamcore_aggregated.concurrent_viewers (
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    video_id            TEXT NOT NULL,
    concurrent_viewers  BIGINT NOT NULL,
    written_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, video_id)
);

-- ----- Buffering Rate -----
-- Percentage of progress events that had is_buffering=true
-- per device type in 1-minute windows.
-- Answers: "which devices are having streaming problems?"
CREATE TABLE IF NOT EXISTS streamcore_aggregated.buffering_rate (
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    device_type         TEXT NOT NULL,
    total_events        BIGINT NOT NULL,
    buffering_events    BIGINT NOT NULL,
    buffering_rate_pct  NUMERIC(5, 2) NOT NULL,
    written_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, device_type)
);

-- ----- Top Videos -----
-- View count per video in 5-minute windows.
-- Answers: "what are people clicking on right now?"
CREATE TABLE IF NOT EXISTS streamcore_aggregated.top_videos (
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    video_id            TEXT NOT NULL,
    video_title         TEXT NOT NULL,
    view_count          BIGINT NOT NULL,
    written_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, video_id)
);

COMMENT ON SCHEMA streamcore_aggregated IS
    'Silver layer — PySpark streaming aggregations';
