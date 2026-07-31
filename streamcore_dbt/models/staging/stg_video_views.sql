/*
Staging model: stg_video_views

Purpose:
  Extract video_view events from the raw JSON payload into
  clean, typed, properly named columns.

Rules for staging models:
  1. One-to-one with the source — no joins, no aggregations
  2. Rename columns to business-friendly names
  3. Cast types — JSON text → proper types
  4. Filter to relevant event type only
  5. No business logic — that belongs in intermediate/marts

Materialization: view (configured in dbt_project.yml)
This means no data is stored — it queries the source live.

Why JSONExtract* instead of Postgres's ->> / :: ?
  The source() here is a ClickHouse table using the PostgreSQL table
  engine (see macros/postgres_bridge.sql) — `payload` arrives as a plain
  String (the raw JSON text), so we parse it with ClickHouse's native
  JSON functions instead of Postgres's JSONB operators.
*/

with source as (

    -- source() tells dbt this table was NOT created by dbt.
    -- It generates: streamcore_raw.events
    -- dbt tracks this dependency in the lineage graph.
    select * from {{ source('streamcore_raw', 'events') }}

),

video_views as (

    select
        -- Core event fields
        event_id,
        event_timestamp,

        -- Extract fields from the JSON payload.
        -- JSONExtractString/Int return sensible zero-values ('' / 0) if
        -- a key is missing; referrer is genuinely optional (JSON null),
        -- so it uses JSONExtract(..., 'Nullable(String)') to preserve NULL.
        JSONExtractString(payload, 'user_id')                       as user_id,
        JSONExtractString(payload, 'session_id')                   as session_id,
        JSONExtractString(payload, 'video_id')                     as video_id,
        JSONExtractString(payload, 'video_title')                  as video_title,
        JSONExtractString(payload, 'device_type')                  as device_type,
        JSONExtractString(payload, 'country_code')                 as country_code,
        JSONExtractString(payload, 'ip_address')                   as ip_address,
        JSONExtract(payload, 'referrer', 'Nullable(String)')       as referrer,
        JSONExtractInt(payload, 'video_duration_seconds')          as video_duration_seconds,
        JSONExtractString(payload, 'initial_quality')              as initial_quality,

        -- Metadata for debugging and lineage
        ingested_at,
        kafka_topic,
        kafka_partition,
        kafka_offset

    from source
    where event_type = 'video_view'

)

select * from video_views
