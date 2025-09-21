{{
  config(
    materialized='view'
  )
}}

with source_data as (
    select 
        user_id,
        signup_date,
        country
    from read_csv('sources/users.csv', 
                   columns={'user_id': 'VARCHAR', 'signup_date': 'VARCHAR', 'country': 'VARCHAR'},
                   header=true)
)

select
    user_id,
    case 
        when signup_date is not null and signup_date != ''
        then cast(signup_date as date)
        else null
    end as signup_date,
    country,
    current_timestamp as _loaded_at
from source_data
where 
    user_id is not null 
    and user_id != ''