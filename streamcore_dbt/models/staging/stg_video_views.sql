/*
Staging model: stg_video_views

Purpose:
  Extract video_view events from the raw JSONB payload into
  clean, typed, properly named columns.

Rules for staging models:
  1. One-to-one with the source — no joins, no aggregations
  2. Rename columns to business-friendly names
  3. Cast types — JSONB text → proper types
  4. Filter to relevant event type only
  5. No business logic — that belongs in intermediate/marts

Materialization: view (configured in dbt_project.yml)
This means no data is stored — it queries raw table live.
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

        -- Extract fields from JSONB payload using ->> operator
        -- ->>  returns text
        -- ::   casts to target type
        payload->>'user_id'                             as user_id,
        payload->>'session_id'                          as session_id,
        payload->>'video_id'                            as video_id,
        payload->>'video_title'                         as video_title,
        payload->>'device_type'                         as device_type,
        payload->>'country_code'                        as country_code,
        payload->>'ip_address'                          as ip_address,
        payload->>'referrer'                            as referrer,
        (payload->>'video_duration_seconds')::integer   as video_duration_seconds,
        (payload->>'initial_quality')                   as initial_quality,

        -- Metadata for debugging and lineage
        ingested_at,
        kafka_topic,
        kafka_partition,
        kafka_offset

    from source
    where event_type = 'video_view'

)

select * from video_views
