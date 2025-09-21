{{
  config(
    materialized='view'
  )
}}

-- Intermediate: Session aggregation logic
-- Aggregates enriched events to session level with basic metrics

select
    user_id,
    episode_id,
    episode_title,
    podcast_id,
    episode_total_duration,
    min(event_timestamp) as session_start,
    max(event_timestamp) as session_end,
    datediff('second', min(event_timestamp), max(event_timestamp)) as session_duration_seconds,
    sum(case when event_type = 'play' then 1 else 0 end) as play_events,
    sum(case when event_type = 'pause' then 1 else 0 end) as pause_events,
    sum(case when event_type = 'seek' then 1 else 0 end) as seek_events,
    sum(case when event_type = 'complete' then 1 else 0 end) as complete_events,
    sum(case when event_type in ('play', 'complete') then coalesce(event_duration, 0) else 0 end) as total_listen_duration,
    max(case when event_type = 'complete' then 1 else 0 end) as completed_episode,
    cast(min(event_timestamp) as date) as session_date
from {{ ref('int_events_enriched') }}
group by user_id, episode_id, episode_title, podcast_id, episode_total_duration