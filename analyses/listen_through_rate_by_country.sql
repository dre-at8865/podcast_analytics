-- Average listen-through rate by country
-- Calculated as total listen duration / total episode duration

with country_listen_rates as (
    select
        u.country,
        count(distinct s.session_key) as total_sessions,
        count(distinct s.user_id) as unique_users,
        count(distinct s.episode_id) as unique_episodes,
        sum(s.total_listen_duration) as total_listen_duration,
        sum(s.episode_total_duration) as total_episode_duration,
        avg(s.listen_through_rate) as avg_session_listen_through_rate,
        -- Aggregate-level calculation: sum(listen_time) / sum(episode_time)
        case 
            when sum(s.episode_total_duration) > 0
            then sum(s.total_listen_duration)::float / sum(s.episode_total_duration)::float
            else 0
        end as overall_listen_through_rate
    from {{ ref('fact_listening_sessions') }} s
    join {{ ref('dim_users') }} u
        on s.user_id = u.user_id
    group by u.country
)

select
    country,
    total_sessions,
    unique_users,
    unique_episodes,
    round(total_listen_duration / 60.0, 2) as total_listen_minutes,
    round(avg_session_listen_through_rate, 4) as avg_session_listen_through_rate,
    round(overall_listen_through_rate, 4) as overall_listen_through_rate
from country_listen_rates
order by overall_listen_through_rate desc