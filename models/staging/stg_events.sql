{{
  config(
    materialized='view'
  )
}}

with source_data as (
    select * from read_csv_auto('sources/event_logs.csv', header=true)
)

select
    event_type,
    user_id,
    episode_id,
    try_cast(timestamp as timestamp) as event_timestamp,
    try_cast(duration as integer) as duration_seconds
from source_data
where 
    user_id is not null 
    and user_id != ''
    and episode_id is not null 
    and episode_id != ''
    and event_type in ('play', 'pause', 'seek', 'complete')
    and timestamp is not null 
    and timestamp != ''
    and timestamp != 'malformed-date'