{{
  config(
    materialized='table'
  )
}}

select
    episode_id,
    podcast_id,
    title,
    release_date_str,
    duration_seconds,
    case 
        when duration_seconds is not null 
        then round(duration_seconds / 60.0, 2)
        else null 
    end as duration_minutes,
    -- Duration categories based on actual data quartiles (see README for details)
    case 
        when duration_seconds is null then 'Unknown'  
        when duration_seconds < 2400 then 'Short (< 40 min)'
        when duration_seconds < 3600 then 'Medium (40-60 min)'
        when duration_seconds < 4500 then 'Long (60-75 min)'
        else 'Extended (75+ min)'
    end as duration_category
from {{ ref('stg_episodes') }}