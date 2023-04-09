with base as (
    select * from {{source('duckdb', 'github_stars')}}
)

select


full_name,
split_part(full_name, '/', 1) as owner,
split_part(full_name, '/', 2) as repo_name,
forks_count,
stargazers_count,
watchers_count,
subscribers_count,
cast(date as date) as snapshot_date,


from base


