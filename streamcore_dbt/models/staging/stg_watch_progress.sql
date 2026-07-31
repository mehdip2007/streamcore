/*
Staging model: stg_watch_progress

Purpose:
  Extract watch_progress events from the raw JSON payload into clean
  columns. These are the high-volume events — fired every 10 seconds
  per viewer.

Key field: position_seconds
  This tells us exactly where in the video the user is.
  Combined with video_duration_seconds from stg_video_views,
  we can calculate completion percentage in downstream models.

See stg_video_views.sql for why this uses ClickHouse's JSONExtract*
functions instead of Postgres's ->> / :: operators.
*/

with source as (

    select * from {{ source('streamcore_raw', 'events') }}

),

watch_progress as (

    select
        event_id,
        event_timestamp,

        JSONExtractString(payload, 'user_id')          as user_id,
        JSONExtractString(payload, 'session_id')       as session_id,
        JSONExtractString(payload, 'video_id')         as video_id,
        JSONExtractString(payload, 'device_type')      as device_type,
        JSONExtractString(payload, 'country_code')     as country_code,
        JSONExtractInt(payload, 'position_seconds')    as position_seconds,
        JSONExtractBool(payload, 'is_buffering')       as is_buffering,
        JSONExtractString(payload, 'quality')          as quality,
        JSONExtractFloat(payload, 'playback_rate')     as playback_rate,

        ingested_at,
        kafka_topic,
        kafka_partition,
        kafka_offset

    from source
    where event_type = 'watch_progress'

)

select * from watch_progress
