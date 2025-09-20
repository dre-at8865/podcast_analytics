{{
  config(
    materialized='view'
  )
}}

with source_data as (
    select * from {{ source('raw_podcast_data', 'event_logs') }}
),

cleaned_events as (
    select
        event_type,
        user_id,
        episode_id,
        -- Parse and validate timestamp
        case 
            when timestamp is not null 
                and timestamp != '' 
                and timestamp ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
            then cast(timestamp as timestamp)
            else null
        end as event_timestamp,
        -- Handle duration - only valid for play and complete events
        case 
            when event_type in ('play', 'complete') and duration is not null and duration != ''
            then cast(duration as integer)
            else null
        end as duration_seconds,
        -- Add row number for deduplication
        row_number() over (
            partition by user_id, episode_id, timestamp, event_type 
            order by timestamp
        ) as row_num
    from source_data
)

select
    event_type,
    user_id,
    episode_id,
    event_timestamp,
    duration_seconds,
    -- Add derived fields
    extract(date from event_timestamp) as event_date,
    extract(hour from event_timestamp) as event_hour,
    extract(dow from event_timestamp) as day_of_week,
    current_timestamp as _loaded_at
from cleaned_events
where 
    -- Data quality filters
    user_id is not null 
    and user_id != ''
    and episode_id is not null 
    and episode_id != ''
    and event_timestamp is not null
    and event_type in ('play', 'pause', 'seek', 'complete')
    -- Remove duplicates
    and row_num = 1