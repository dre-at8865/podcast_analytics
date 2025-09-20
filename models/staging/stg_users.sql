{{
  config(
    materialized='view'
  )
}}

with source_data as (
    select * from {{ source('raw_podcast_data', 'users') }}
),

cleaned_users as (
    select
        user_id,
        -- Parse and validate signup_date
        case 
            when signup_date is not null and signup_date != ''
            then cast(signup_date as date)
            else null
        end as signup_date,
        -- Clean and standardize country codes
        upper(trim(country)) as country,
        current_timestamp as _loaded_at
    from source_data
)

select
    user_id,
    signup_date,
    country,
    -- Add derived fields
    date_diff('day', signup_date, current_date) as days_since_signup,
    case 
        when date_diff('day', signup_date, current_date) <= 30 then 'New'
        when date_diff('day', signup_date, current_date) <= 90 then 'Recent'
        when date_diff('day', signup_date, current_date) <= 365 then 'Established'
        else 'Veteran'
    end as user_cohort,
    _loaded_at
from cleaned_users
where 
    user_id is not null 
    and user_id != ''