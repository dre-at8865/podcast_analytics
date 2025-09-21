{{
  config(
    materialized='table'
  )
}}

-- Daily user activity aggregation
-- Aggregates fact table data for user behavior analysis

select
    s.user_id,
    s.session_date,
    u.country,
    u.signup_date,
    u.user_cohort,
    count(distinct s.episode_id) as episodes_listened,
    count(distinct s.podcast_id) as podcasts_listened,
    sum(s.total_listen_duration) as total_daily_listen_duration,
    round(sum(s.total_listen_duration) / 60.0, 2) as total_daily_listen_minutes,
    sum(s.completed_episode) as episodes_completed,
    sum(s.engagement_score) as daily_engagement_score,
    count(distinct s.session_key) as total_sessions,
    case 
        when count(distinct s.episode_id) >= 3 then 1
        else 0
    end as is_power_user_day,
    case 
        when sum(s.engagement_score) >= 20 then 'High'
        when sum(s.engagement_score) >= 10 then 'Medium'
        when sum(s.engagement_score) >= 5 then 'Low'
        else 'Minimal'
    end as daily_engagement_level,
    current_timestamp as _loaded_at
from {{ ref('fact_listening_sessions') }} s
left join {{ ref('dim_users') }} u
    on s.user_id = u.user_id
group by s.user_id, s.session_date, u.country, u.signup_date, u.user_cohort