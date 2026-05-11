/*
Mart: mart_device_quality

Business question: Which devices and regions have the worst experience?

This is what a platform engineering team looks at.
It answers:
  - Are mobile users buffering more than desktop?
  - Which countries have quality problems?
  - What quality level do most users watch at?
  - Is buffering affecting completion rates?

Audience: Engineering, infrastructure, QA teams
*/

with sessions as (

    select * from {{ ref('int_watch_sessions') }}

),

device_metrics as (

    select
        device_type,
        country_code,

        -- Volume
        count(distinct session_id)              as total_sessions,
        count(distinct user_id)                 as unique_users,

        -- Buffering quality
        round(avg(buffering_rate_pct), 2)       as avg_buffering_rate_pct,
        max(buffering_rate_pct)                 as max_buffering_rate_pct,

        -- Sessions with any buffering
        count(*) filter (
            where buffering_rate_pct > 0
        )                                       as sessions_with_buffering,

        -- Quality distribution
        count(*) filter (
            where dominant_quality = '4k'
        )                                       as sessions_4k,
        count(*) filter (
            where dominant_quality = '1080p'
        )                                       as sessions_1080p,
        count(*) filter (
            where dominant_quality = '720p'
        )                                       as sessions_720p,
        count(*) filter (
            where dominant_quality in ('480p', '240p')
        )                                       as sessions_low_quality,

        -- Engagement by device
        round(avg(completion_pct), 2)           as avg_completion_pct,
        round(avg(avg_playback_rate), 2)        as avg_playback_rate,
        round(avg(watch_duration_seconds), 0)   as avg_watch_duration_seconds

    from sessions
    group by device_type, country_code

)

select
    *,
    -- Flag device+country combinations with high buffering
    case
        when avg_buffering_rate_pct > 15 then 'critical'
        when avg_buffering_rate_pct > 8  then 'warning'
        else 'healthy'
    end as buffering_status
from device_metrics
order by avg_buffering_rate_pct desc
