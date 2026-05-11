/*
Staging model: stg_watch_progress

Purpose:
  Extract watch_progress events from raw JSONB into clean columns.
  These are the high-volume events — fired every 10 seconds per viewer.

Key field: position_seconds
  This tells us exactly where in the video the user is.
  Combined with video_duration_seconds from stg_video_views,
  we can calculate completion percentage in downstream models.
*/

with source as (

    select * from {{ source('streamcore_raw', 'events') }}

),

watch_progress as (

    select
        event_id,
        event_timestamp,

        payload->>'user_id'                         as user_id,
        payload->>'session_id'                      as session_id,
        payload->>'video_id'                        as video_id,
        payload->>'device_type'                     as device_type,
        payload->>'country_code'                    as country_code,
        (payload->>'position_seconds')::integer     as position_seconds,
        (payload->>'is_buffering')::boolean         as is_buffering,
        payload->>'quality'                         as quality,
        (payload->>'playback_rate')::numeric(4,2)   as playback_rate,

        ingested_at,
        kafka_topic,
        kafka_partition,
        kafka_offset

    from source
    where event_type = 'watch_progress'

)

select * from watch_progress
