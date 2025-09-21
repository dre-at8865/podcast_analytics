{{
  config(
    materialized='table'
  )
}}

select
    user_id,
    signup_date,
    country,
    case 
        when signup_date is not null 
        then date_diff('day', signup_date, current_date) 
        else null 
    end as days_since_signup,
    -- User lifecycle cohorts based on behavioral stages (see README for details)
    case 
        when signup_date is null then 'Unknown'
        when signup_date is not null and date_diff('day', signup_date, current_date) <= 90 then 'Recent (0-3 months)'
        when signup_date is not null and date_diff('day', signup_date, current_date) <= 180 then 'Growing (3-6 months)'
        when signup_date is not null and date_diff('day', signup_date, current_date) <= 365 then 'Loyal (6-12 months)'
        when signup_date is not null then 'Champion (12+ months)'
        else 'Unknown'
    end as user_cohort
from {{ ref('stg_users') }}