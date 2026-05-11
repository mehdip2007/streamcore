/*
Intermediate model: int_watch_sessions

Purpose:
  Combine view events and progress events to reconstruct
  complete watch sessions per user per video.

  A "session" starts with a video_view event and ends when
  progress events stop. We calculate:
    - How long did the user actually watch?
    - What percentage of the video did they complete?
    - Did they experience buffering?
    - What quality did they watch at?

This is the most important model — almost every mart
builds on top of this.

Materialization: table (configured in dbt_project.yml)
We store this because it joins two large datasets and
multiple downstream models need it.

Why ref() instead of source()?
  ref() points to OTHER dbt models.
  source() points to raw tables dbt didn't create.
  ref() ensures dbt runs dependencies in correct order.
*/

with views as (

    -- ref() generates: streamcore_staging.stg_video_views
    -- dbt knows this model depends on stg_video_views
    select * from {{ ref('stg_video_views') }}

),

progress as (

    select * from {{ ref('stg_watch_progress') }}

),

-- Aggregate all progress events per session
session_progress as (

    select
        session_id,
        video_id,

        -- Latest position = how far they got in the video
        max(position_seconds)                   as max_position_seconds,

        -- First and last event timestamps = actual watch duration
        min(event_timestamp)                    as watch_start_at,
        max(event_timestamp)                    as watch_end_at,

        -- Total elapsed time in seconds
        extract(
            epoch from (max(event_timestamp) - min(event_timestamp))
        )::integer                              as watch_duration_seconds,

        -- Buffering stats
        count(*) filter (where is_buffering)    as buffering_ticks,
        count(*)                                as total_ticks,

        -- Most common quality — mode approximation using max
        max(quality)                            as dominant_quality,

        -- Average playback rate
        round(avg(playback_rate)::numeric, 2)   as avg_playback_rate

    from progress
    group by session_id, video_id

),

-- Join with view events to get video metadata
final as (

    select
        -- Session identifiers
        v.user_id,
        v.session_id,
        v.video_id,
        v.video_title,

        -- Device and location context
        v.device_type,
        v.country_code,

        -- Video metadata
        v.video_duration_seconds,
        v.referrer,
        v.initial_quality,

        -- Timing
        v.event_timestamp                       as view_started_at,
        sp.watch_start_at,
        sp.watch_end_at,
        sp.watch_duration_seconds,

        -- Completion metrics
        sp.max_position_seconds,

        -- Completion percentage — how far through the video did they get?
        -- Capped at 100% to handle edge cases from our simulator
        least(
            round(
                (sp.max_position_seconds::numeric / nullif(v.video_duration_seconds, 0)) * 100,
                2
            ),
            100.00
        )                                       as completion_pct,

        -- Did they finish? (watched at least 90%)
        case
            when sp.max_position_seconds >= v.video_duration_seconds * 0.9
            then true
            else false
        end                                     as is_completed,

        -- Buffering
        sp.buffering_ticks,
        sp.total_ticks,
        round(
            (sp.buffering_ticks::numeric / nullif(sp.total_ticks, 0)) * 100,
            2
        )                                       as buffering_rate_pct,

        -- Quality
        sp.dominant_quality,
        sp.avg_playback_rate

    from views v
    left join session_progress sp
        on v.session_id = sp.session_id
        and v.video_id = sp.video_id

)

select * from final
