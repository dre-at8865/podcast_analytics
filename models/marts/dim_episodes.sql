{{
  config(
    materialized='table'
  )
}}

select
    episode_id as episode_key,
    episode_id,
    podcast_id,
    title,
    release_date,
    duration_seconds,
    duration_minutes,
    duration_category,
    days_since_release,
    episode_age_category,
    _loaded_at
from {{ ref('stg_episodes') }}