-- Number of distinct users who listened to 3+ different episodes in one day
-- This identifies "power users" with high daily engagement

with power_users_by_day as (
    select
        session_date,
        count(distinct user_id) as power_users_count
    from {{ ref('fact_daily_user_activity') }}
    where is_power_user_day = 1  -- 3+ episodes listened
    group by 1
),

overall_stats as (
    select
        count(distinct case when is_power_user_day = 1 then user_id end) as total_unique_power_users,
        count(distinct user_id) as total_unique_users,
        sum(is_power_user_day) as total_power_user_days,
        count(*) as total_user_days
    from {{ ref('fact_daily_user_activity') }}
)

select
    'Daily Power User Counts' as metric_type,
    session_date,
    power_users_count,
    null::float as percentage
from power_users_by_day

union all

select
    'Summary Statistics' as metric_type,
    null::date as session_date,
    total_unique_power_users as power_users_count,
    round(total_unique_power_users::float / total_unique_users::float * 100, 2) as percentage
from overall_stats

union all

select
    'Power User Days' as metric_type,
    null::date as session_date,
    total_power_user_days as power_users_count,
    round(total_power_user_days::float / total_user_days::float * 100, 2) as percentage
from overall_stats

order by metric_type, session_date