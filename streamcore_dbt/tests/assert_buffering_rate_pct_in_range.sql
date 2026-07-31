/*
Singular data test: buffering_rate_pct must be a valid percentage.

Same reasoning as assert_completion_pct_in_range.sql — buffering_ticks
can never exceed total_ticks, so this ratio should never leave [0, 100].
*/

select *
from {{ ref('int_watch_sessions') }}
where buffering_rate_pct is not null
  and (buffering_rate_pct < 0 or buffering_rate_pct > 100)
