-- Test to ensure no future timestamps in events
-- This should return 0 rows (empty result set) to pass
select
    'Future event found' as error_message,
    user_id,
    episode_id,
    event_timestamp
from {{ ref('stg_events') }}
where event_timestamp > current_timestamp