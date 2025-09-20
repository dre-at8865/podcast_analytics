{{
  config(
    materialized='view'
  )
}}

with source_data as (
    select * from {{ source('raw_podcast_data', 'episodes') }}
),

cleaned_episodes as (
    select
        episode_id,
        podcast_id,
        trim(title) as title,
        -- Parse and validate release_date
        case 
            when release_date is not null and release_date != ''
            then cast(release_date as date)
            else null
        end as release_date,
        -- Validate duration_seconds
        case 
            when duration_seconds is not null and cast(duration_seconds as integer) > 0
            then cast(duration_seconds as integer)
            else null
        end as duration_seconds,
        current_timestamp as _loaded_at
    from source_data
)

select
    episode_id,
    podcast_id,
    title,
    release_date,
    duration_seconds,
    -- Add derived fields
    round(duration_seconds / 60.0, 2) as duration_minutes,
    case 
        when duration_seconds < 900 then 'Short (< 15 min)'
        when duration_seconds < 1800 then 'Medium (15-30 min)'
        when duration_seconds < 3600 then 'Long (30-60 min)'
        else 'Extended (> 60 min)'
    end as duration_category,
    date_diff('day', release_date, current_date) as days_since_release,
    case 
        when date_diff('day', release_date, current_date) <= 7 then 'New'
        when date_diff('day', release_date, current_date) <= 30 then 'Recent'
        when date_diff('day', release_date, current_date) <= 90 then 'Established'
        else 'Archive'
    end as episode_age_category,
    _loaded_at
from cleaned_episodes
where 
    episode_id is not null 
    and episode_id != ''
    and podcast_id is not null 
    and podcast_id != ''