{{
  config(
    materialized='table'
  )
}}

select
    user_id as user_key,
    user_id,
    signup_date,
    country,
    user_cohort,
    days_since_signup,
    _loaded_at
from {{ ref('stg_users') }}