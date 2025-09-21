-- Top 10 most completed episodes in the past 7 days
-- Using the analysis_date variable from dbt_project.yml for consistent testing

with recent_completions as (
    select
        e.episode_id,
        e.title as episode_title,
        e.podcast_id,
        count(*) as total_completions,
        count(distinct s.user_id) as unique_completers,
        avg(s.listen_through_rate) as avg_listen_through_rate
    from {{ ref('fact_listening_sessions') }} s
    join {{ ref('dim_episodes') }} e
        on s.episode_id = e.episode_id
    where s.completed_episode = 1
        and s.session_date >= '{{ var("analysis_date") }}'::date - interval '7 days'
        and s.session_date <= '{{ var("analysis_date") }}'::date
    group by e.episode_id, e.title, e.podcast_id
)

select
    episode_id,
    episode_title,
    podcast_id,
    total_completions,
    unique_completers,
    round(avg_listen_through_rate, 4) as avg_listen_through_rate
from recent_completions
order by total_completions desc
limit 10