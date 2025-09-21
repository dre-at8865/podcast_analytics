{{
  config(
    materialized='table'
  )
}}

select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'episode_id', 'session_date']) }} as session_key,
    user_id,
    episode_id,
    episode_title,
    podcast_id,
    episode_total_duration,
    session_start,
    session_end,
    session_duration_seconds,
    round(session_duration_seconds / 60.0, 2) as session_duration_minutes,
    play_events,
    pause_events,
    seek_events,
    complete_events,
    total_listen_duration,
    round(total_listen_duration / 60.0, 2) as total_listen_minutes,
    completed_episode,
    case 
        when episode_total_duration > 0 and total_listen_duration > 0
        then round(total_listen_duration::float / episode_total_duration::float, 4)
        else 0
    end as listen_through_rate,
    -- Multi-factor engagement score: 70% listen time + 30% interactions (see README for details)
    case 
        when total_listen_duration > 0 then
            round(
                (total_listen_duration / 60.0) * 0.7 +
                (complete_events * 2 + play_events + pause_events + seek_events) * 0.3
            , 2)
        else (complete_events * 2 + play_events + pause_events + seek_events)
    end as engagement_score,
    session_date
from {{ ref('int_session_metrics') }}