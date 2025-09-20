{{
  config(
    materialized='table'
  )
}}

select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'session_date']) }} as daily_activity_key,
    user_id,
    session_date,
    country,
    signup_date,
    user_cohort,
    episodes_listened,
    podcasts_listened,
    total_daily_listen_duration,
    total_daily_listen_minutes,
    episodes_completed,
    daily_engagement_score,
    total_sessions,
    is_power_user_day,
    daily_engagement_level,
    daily_listen_category,
    _loaded_at
from {{ ref('int_daily_user_activity') }}