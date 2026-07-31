/*
Mart: mart_content_performance

Business question: How is each video performing?

This is what a content team looks at every morning.
It answers:
  - Which videos get the most views?
  - Which videos do people actually finish watching?
  - Which videos have buffering problems?
  - Where are viewers dropping off?

Audience: Content team, product managers, executives

Materialization: table — queried frequently, must be fast

Dialect note: Postgres's `count(*) filter (where ...)` becomes
ClickHouse's `countIf(...)` throughout (see int_watch_sessions.sql).
*/

with sessions as (

    select * from {{ ref('int_watch_sessions') }}

),

content_metrics as (

    select
        video_id,
        video_title,

        -- Volume metrics
        count(distinct session_id)              as total_views,
        count(distinct user_id)                 as unique_viewers,

        -- Engagement metrics
        round(avg(completion_pct), 2)           as avg_completion_pct,
        round(avg(watch_duration_seconds), 0)   as avg_watch_duration_seconds,
        countIf(is_completed)                   as completed_views,

        -- Completion rate — what % of viewers finish the video?
        round(
            countIf(is_completed) / nullIf(count(*), 0) * 100,
            2
        )                                        as completion_rate_pct,

        -- Quality metrics
        round(avg(buffering_rate_pct), 2)       as avg_buffering_rate_pct,

        -- Device breakdown
        countIf(device_type in ('mobile_ios', 'mobile_android'))  as mobile_views,
        countIf(device_type in ('web_desktop', 'web_mobile'))     as web_views,
        countIf(device_type = 'smart_tv')                         as tv_views,

        -- Traffic sources
        countIf(referrer = 'search')            as views_from_search,
        countIf(referrer = 'recommendation')    as views_from_recommendation,
        countIf(referrer = 'homepage')          as views_from_homepage,

        -- Timestamps
        min(view_started_at)                    as first_view_at,
        max(view_started_at)                    as last_view_at

    from sessions
    group by video_id, video_title

)

select
    *,
    -- Rank videos by total views for easy top-N queries
    rank() over (order by total_views desc)         as views_rank,
    -- Rank by completion rate — finds highest quality content
    rank() over (order by completion_rate_pct desc) as completion_rank
from content_metrics
order by total_views desc
