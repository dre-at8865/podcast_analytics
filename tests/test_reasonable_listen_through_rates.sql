-- Custom test to check for reasonable listen-through rates
-- Test fails if any sessions have impossible listen-through rates > 50.0
-- This should return 0 rows (empty result set) to pass

select
    'Invalid listen-through rate' as error_message,
    session_key,
    user_id,
    episode_id,
    listen_through_rate
from {{ ref('fact_listening_sessions') }}
where listen_through_rate > 50.0