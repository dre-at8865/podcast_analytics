-- Test to ensure no future timestamps in events
select
    count(*) as future_events
from {{ ref('stg_events') }}
where event_timestamp > current_timestamp