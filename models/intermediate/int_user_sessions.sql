{{
  config(
    materialized='view'
  )
}}

with events_with_episode_info as (
    select 
        e.user_id,
        e.episode_id,
        e.event_type,
        e.event_timestamp,
        e.duration_seconds as event_duration,
        e.event_date,
        ep.duration_seconds as episode_total_duration,
        ep.title as episode_title,
        ep.podcast_id,
        -- Create session identifier based on gaps in activity
        sum(case when 
            lag(e.event_timestamp) over (
                partition by e.user_id, e.episode_id 
                order by e.event_timestamp
            ) is null 
            or extract(epoch from e.event_timestamp - lag(e.event_timestamp) over (
                partition by e.user_id, e.episode_id 
                order by e.event_timestamp
            )) > 1800  -- 30 minutes gap = new session
        then 1 else 0 end) over (
            partition by e.user_id, e.episode_id 
            order by e.event_timestamp 
            rows unbounded preceding
        ) as session_id
    from {{ ref('stg_events') }} e
    left join {{ ref('stg_episodes') }} ep
        on e.episode_id = ep.episode_id
),

session_metrics as (
    select
        user_id,
        episode_id,
        session_id,
        episode_title,
        podcast_id,
        episode_total_duration,
        min(event_timestamp) as session_start,
        max(event_timestamp) as session_end,
        -- Calculate session duration
        extract(epoch from max(event_timestamp) - min(event_timestamp)) as session_duration_seconds,
        -- Count different event types
        sum(case when event_type = 'play' then 1 else 0 end) as play_events,
        sum(case when event_type = 'pause' then 1 else 0 end) as pause_events,
        sum(case when event_type = 'seek' then 1 else 0 end) as seek_events,
        sum(case when event_type = 'complete' then 1 else 0 end) as complete_events,
        -- Sum duration for play and complete events
        sum(case when event_type in ('play', 'complete') then coalesce(event_duration, 0) else 0 end) as total_listen_duration,
        -- Flags
        max(case when event_type = 'complete' then 1 else 0 end) as completed_episode,
        date_trunc('day', min(event_timestamp)) as session_date
    from events_with_episode_info
    group by 1, 2, 3, 4, 5, 6
)

select
    user_id,
    episode_id,
    session_id,
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
    -- Calculate listen-through rate
    case 
        when episode_total_duration > 0 and total_listen_duration > 0
        then round(total_listen_duration::float / episode_total_duration::float, 4)
        else 0
    end as listen_through_rate,
    -- Engagement score (combination of events and completion)
    (play_events + pause_events + seek_events + (complete_events * 5)) as engagement_score,
    session_date,
    current_timestamp as _loaded_at
from session_metrics