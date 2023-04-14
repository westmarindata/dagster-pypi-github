with github as (
    select * from {{ref('stg_github_stars')}}
),

pypi as (
    select * from {{ref('stg_pypi_downloads')}}
),

joined as (
    select

    github.owner,
    github.repo_name,
    github.forks_count,
    github.stargazers_count,
    github.watchers_count,
    github.subscribers_count,
    github.snapshot_date as github_snapshot_date,

    pypi.download_date,
    pypi.project_name,
    pypi.project_version,
    pypi.project_name || '-'||
        split(project_version, '.')[safe_offset(0)] || '.' || split(project_version, '.')[safe_offset(1)] as project_name_version,

    pypi.file_downloads_count


    from github
    left join pypi
        on github.repo_name = pypi.project_name
        and pypi.download_date = github.snapshot_date
)

select * from joined
