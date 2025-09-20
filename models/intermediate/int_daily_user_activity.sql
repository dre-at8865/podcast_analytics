{{
  config(
    materialized='view'
  )
}}

with daily_user_activity as (
    select
        user_id,
        session_date,
        count(distinct episode_id) as episodes_listened,
        count(distinct podcast_id) as podcasts_listened,
        sum(total_listen_duration) as total_daily_listen_duration,
        sum(completed_episode) as episodes_completed,
        sum(engagement_score) as daily_engagement_score,
        count(distinct session_id) as total_sessions
    from {{ ref('int_user_sessions') }}
    group by 1, 2
),

user_activity_enriched as (
    select
        da.*,
        u.country,
        u.signup_date,
        u.user_cohort,
        -- Power user classification
        case 
            when episodes_listened >= 3 then 1
            else 0
        end as is_power_user_day,
        -- Engagement level
        case 
            when daily_engagement_score >= 20 then 'High'
            when daily_engagement_score >= 10 then 'Medium'
            when daily_engagement_score >= 5 then 'Low'
            else 'Minimal'
        end as daily_engagement_level,
        -- Listen duration category
        case 
            when total_daily_listen_duration >= 3600 then 'Heavy (1+ hrs)'
            when total_daily_listen_duration >= 1800 then 'Moderate (30+ min)'
            when total_daily_listen_duration >= 600 then 'Light (10+ min)'
            else 'Casual (< 10 min)'
        end as daily_listen_category
    from daily_user_activity da
    left join {{ ref('stg_users') }} u
        on da.user_id = u.user_id
)

select
    user_id,
    session_date,
    country,
    signup_date,
    user_cohort,
    episodes_listened,
    podcasts_listened,
    total_daily_listen_duration,
    round(total_daily_listen_duration / 60.0, 2) as total_daily_listen_minutes,
    episodes_completed,
    daily_engagement_score,
    total_sessions,
    is_power_user_day,
    daily_engagement_level,
    daily_listen_category,
    current_timestamp as _loaded_at
from user_activity_enriched