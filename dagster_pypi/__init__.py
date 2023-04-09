from dagster_gcp import bigquery_resource
from dagster_gcp_pandas import bigquery_pandas_io_manager
from dagster import (
    Definitions,
    load_assets_from_modules,
    ScheduleDefinition,
    define_asset_job,
)

from . import assets
from . import resources

daily_schedule = ScheduleDefinition( job=define_asset_job(name="dagster_pypi_job"),
        cron_schedule="0 0 * * *",
        )

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    schedules=[daily_schedule],
    resources=resources.resource_def['LOCAL']
)
