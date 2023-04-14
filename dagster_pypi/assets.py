import pandas as pd
from dagster import DailyPartitionsDefinition, asset
from dagster_dbt import load_assets_from_dbt_project

from .resources import DBT_PROFILE_DIR, DBT_PROJECT_DIR, HEX_PROJECT_ID

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILE_DIR,
)

START_DATE = "2023-04-10"

@asset(
    key_prefix=["dagster_pypi"],
    required_resource_keys={"pypi_manager"},
    partitions_def=DailyPartitionsDefinition(start_date=START_DATE),
    metadata={"partition_expr": "download_date"},
)
def raw_pypi_downloads(context) -> pd.DataFrame:
    return context.resources.pypi_manager(context.partition_key)


@asset(
    key_prefix=["dagster_pypi"],
    required_resource_keys={"github_manager"},
    partitions_def=DailyPartitionsDefinition(start_date=START_DATE),
    metadata={"partition_expr": "date"},
)
def raw_github_stars(context) -> pd.DataFrame:
    df = context.resources.github_manager(context.partition_key)
    return df


@asset(
    key_prefix=["dagster_pypi"],
    partitions_def=DailyPartitionsDefinition(start_date=START_DATE),
    metadata={"partition_expr": "download_date"},
)
def pypi_downloads(raw_pypi_downloads) -> pd.DataFrame:
    df = raw_pypi_downloads
    # Here we could perform some pandas transformations on data
    return df


@asset(
    key_prefix=["dagster_pypi"],
    partitions_def=DailyPartitionsDefinition(start_date=START_DATE),
    metadata={"partition_expr": "date"},
)
def github_stars(raw_github_stars) -> pd.DataFrame:
    # Here we could perform some pandas transformations on data
    df = raw_github_stars
    return df


@asset(
    non_argument_deps={"base_joined"},
    required_resource_keys={"hex"},
)
def hex_notebook(context) -> None:
    context.resources.hex.run_and_poll(
        project_id=HEX_PROJECT_ID,
        inputs=None,
    )
