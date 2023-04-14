import datetime
import os

import pandas as pd
from dagster_dbt import dbt_cli_resource
from dagster_duckdb_pandas import duckdb_pandas_io_manager
from dagster_gcp_pandas import bigquery_pandas_io_manager
from dagster_hex.resources import hex_resource
from google.cloud import bigquery

DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "./dbt_project")
DBT_PROFILE_DIR = os.path.join(DBT_PROJECT_DIR, "./profiles")
HEX_PROJECT_ID = os.getenv("HEX_PROJECT_ID")
API_KEY = os.getenv("HEX_API_KEY")
STEAMPIPE_CONN = os.getenv("STEAMPIPE_CONN")

my_hex_resource = hex_resource.configured({"api_key": API_KEY})

duckdb_io_manager = duckdb_pandas_io_manager.configured(
    {"database": os.path.join(DBT_PROJECT_DIR, "dbt_duckdb.db")}
)

bigquery_pandas_io_manager = bigquery_pandas_io_manager.configured(
    {"project": "westmarindata"}
)


def pypi_local_download(date: str):
    print("Pretending to fetch for a given date: ", date)
    pypi_path = os.path.join(os.path.dirname(__file__), "../data/pypi_downloads.csv")
    df = pd.read_csv(pypi_path)
    df["download_date"] = datetime.datetime.strptime(date, "%Y-%m-%d")
    return df


def pypi_bigquery_download(date: str):
    print("Fetching from bigquery for a given date: ", date)
    client = bigquery.Client()
    query = f"""
    SELECT
      date_trunc(file_downloads.timestamp, DAY) AS download_date,
      file_downloads.file.project  AS project_name,
      file_downloads.file.version as project_version,
      COUNT(*) AS file_downloads_count

    FROM `bigquery-public-data.pypi.file_downloads` AS file_downloads
    WHERE (file_downloads.file.project LIKE '%dagster%')

    AND date_trunc(file_downloads.timestamp, DAY) = '{date}'
    GROUP BY 1,2,3
    ORDER BY 1,2,3
    """
    return client.query(query).result().to_dataframe()


def get_github_stars_local(date):
    print("Pretending to fetch Github data for a given date: ", date)
    github_path = os.path.join(
        os.path.dirname(__file__), "../data/github_star_count.csv"
    )
    df = pd.read_csv(github_path)
    df["date"] = datetime.datetime.strptime(date, "%Y-%m-%d")

    return df


def get_github_stars_streampipe(date):
    print("Fetching Github data from Streampipe for a given date: ", date)
    sql = f"""select
        cast('{date}' as timestamp) as date,
        full_name,
        forks_count,
        stargazers_count,
        subscribers_count,
        watchers_count
    from github_repository
    where full_name in ('dagster-io/dagster')
    """
    df = pd.read_sql(sql, STEAMPIPE_CONN)
    return df


resource_def = {
    "LOCAL": {
        "io_manager": duckdb_io_manager,
        "github_manager": get_github_stars_local,
        "pypi_manager": pypi_local_download,
        "hex": my_hex_resource,
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": DBT_PROJECT_DIR,
                "profiles_dir": DBT_PROFILE_DIR,
                "target": "local",
            }
        ),
    },
    "PROD": {
        "io_manager": bigquery_pandas_io_manager,
        "github_manager": get_github_stars_streampipe,
        "pypi_manager": pypi_bigquery_download,
        "hex": my_hex_resource,
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": DBT_PROJECT_DIR,
                "profiles_dir": DBT_PROFILE_DIR,
                "target": "prod",
            }
        ),
    },
}
