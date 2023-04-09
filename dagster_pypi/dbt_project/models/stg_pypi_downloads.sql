with base as (
select * from {{source('duckdb', 'pypi_downloads')}}
)


select

download_date,
project_name,
project_version,
file_downloads_count

from base
