{{
  config(
    materialized='table'
  )
}}

select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'episode_id', 'session_id']) }} as session_key,
    user_id,
    episode_id,
    session_id,
    episode_title,
    podcast_id,
    episode_total_duration,
    session_start,
    session_end,
    session_duration_seconds,
    session_duration_minutes,
    play_events,
    pause_events,
    seek_events,
    complete_events,
    total_listen_duration,
    total_listen_minutes,
    completed_episode,
    listen_through_rate,
    engagement_score,
    session_date,
    _loaded_at
from {{ ref('int_user_sessions') }}