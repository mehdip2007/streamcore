/*
Singular data test: completion_pct must be a valid percentage.

dbt singular tests are inverted — this query should return ZERO rows.
Any row returned here is a real bug: either the completion_pct math in
int_watch_sessions.sql broke, or bad data slipped through upstream
(e.g. a corrupt video_duration_seconds nullIf didn't catch).
*/

select *
from {{ ref('int_watch_sessions') }}
where completion_pct is not null
  and (completion_pct < 0 or completion_pct > 100)
