-- Additional analysis: Podcast performance and user engagement patterns

with podcast_performance as (
    select
        e.podcast_id,
        count(distinct s.episode_id) as total_episodes,
        count(distinct s.user_id) as unique_listeners,
        count(distinct s.session_key) as total_sessions,
        sum(s.completed_episode) as total_completions,
        avg(s.listen_through_rate) as avg_listen_through_rate,
        sum(s.total_listen_duration) as total_listen_duration
    from {{ ref('fact_listening_sessions') }} s
    join {{ ref('dim_episodes') }} e
        on s.episode_id = e.episode_id
    group by e.podcast_id
),

user_cohort_analysis as (
    select
        da.user_cohort,
        count(distinct da.user_id) as users_in_cohort,
        avg(da.episodes_listened) as avg_daily_episodes,
        avg(da.total_daily_listen_minutes) as avg_daily_listen_minutes,
        sum(da.is_power_user_day) as power_user_days,
        count(*) as total_user_days
    from {{ ref('mart_daily_user_activity') }} da
    group by da.user_cohort
),

engagement_by_episode_length as (
    select
        e.duration_category,
        count(distinct s.session_key) as total_sessions,
        avg(s.listen_through_rate) as avg_listen_through_rate,
        sum(s.completed_episode) as total_completions,
        round(sum(s.completed_episode)::float / count(distinct s.session_key)::float * 100, 2) as completion_rate_pct
    from {{ ref('fact_listening_sessions') }} s
    join {{ ref('dim_episodes') }} e
        on s.episode_id = e.episode_id
    group by e.duration_category
)

-- Combine all insights
select 'Podcast Performance' as analysis_type, 
       podcast_id as category, 
       total_episodes as metric_1,
       unique_listeners as metric_2,
       round(avg_listen_through_rate, 4) as metric_3
from podcast_performance
where total_sessions >= 10  -- Filter for podcasts with meaningful data

union all

select 'User Cohort Analysis' as analysis_type,
       user_cohort as category,
       users_in_cohort as metric_1,
       round(avg_daily_episodes, 2) as metric_2,
       round(avg_daily_listen_minutes, 2) as metric_3
from user_cohort_analysis

union all

select 'Engagement by Episode Length' as analysis_type,
       duration_category as category,
       total_sessions as metric_1,
       round(avg_listen_through_rate, 4) as metric_2,
       completion_rate_pct as metric_3
from engagement_by_episode_length

order by analysis_type, category