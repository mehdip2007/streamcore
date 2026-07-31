/*
Singular data test: watch_duration_seconds must never be negative.

watch_duration_seconds is dateDiff('second', min(event_timestamp),
max(event_timestamp)) over each session's progress events — it can
only go negative if progress events for a session arrive with
inconsistent/out-of-order timestamps, which would indicate a producer
or clock-skew bug worth catching here rather than downstream in a mart.
*/

select *
from {{ ref('int_watch_sessions') }}
where watch_duration_seconds is not null
  and watch_duration_seconds < 0
