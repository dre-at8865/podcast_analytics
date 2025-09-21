{{
  config(
    materialized='view'
  )
}}

with source_data as (
    select 
        episode_id,
        podcast_id,
        title,
        release_date,
        duration_seconds
    from read_csv('sources/episodes.csv',
                   columns={'episode_id': 'VARCHAR', 'podcast_id': 'VARCHAR', 'title': 'VARCHAR', 'release_date': 'VARCHAR', 'duration_seconds': 'VARCHAR'},
                   header=true)
)

select
    episode_id,
    podcast_id,
    trim(title) as title,
    case 
        when release_date is not null and trim(release_date) != '' and length(trim(release_date)) >= 8
        then trim(release_date)
        else null
    end as release_date_str,
    case 
        when duration_seconds is not null and cast(duration_seconds as integer) > 0
        then cast(duration_seconds as integer)
        else null
    end as duration_seconds,
    current_timestamp as _loaded_at
from source_data
where 
    episode_id is not null 
    and episode_id != ''
    and podcast_id is not null 
    and podcast_id != ''