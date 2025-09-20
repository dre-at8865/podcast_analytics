-- Custom test to check for reasonable listen-through rates
-- Test fails if more than 5% of sessions have listen-through rates > 1.5

select
    count(*) as invalid_rates
from {{ ref('fact_listening_sessions') }}
where listen_through_rate > 1.5