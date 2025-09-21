{{
  config(
    materialized='view'
  )
}}

-- Intermediate: Clean and enrich events with episode metadata
-- Joins staging events with episode data for downstream processing

with deduplicated_events as (
    select distinct
        event_type,
        user_id,
        episode_id,
        event_timestamp,
        duration_seconds,
        cast(event_timestamp as date) as event_date
    from {{ ref('stg_events') }}
),

-- Join with episode metadata
events_with_episodes as (
    select 
        e.user_id,
        e.episode_id,
        e.event_type,
        e.event_timestamp,
        e.duration_seconds as event_duration,
        e.event_date,
        ep.duration_seconds as episode_total_duration,
        ep.title as episode_title,
        ep.podcast_id
    from deduplicated_events e
    left join {{ ref('stg_episodes') }} ep
        on e.episode_id = ep.episode_id
)

select * from events_with_episodes