import os

from dagster_dbt import dbt_cli_resource
from dagster_duckdb import build_duckdb_io_manager
from dagster_duckdb_pandas import DuckDBPandasTypeHandler
from dagster_gcp_pandas import bigquery_pandas_io_manager

DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "./dbt_project")

duckdb_io_manager = build_duckdb_io_manager([DuckDBPandasTypeHandler()]).configured(
    {"database": os.path.join(DBT_PROJECT_DIR, "dbt_duckdb.db")}
)

bigquery_pandas_io_manager = bigquery_pandas_io_manager.configured(
    {"project": "westmarindata"}
)

resource_def = {
    "LOCAL": {
        "io_manager": duckdb_io_manager,
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": DBT_PROJECT_DIR,
                "profiles_dir": os.path.join(DBT_PROJECT_DIR, "profiles"),
                "target": "LOCAL",
            }
        ),
    },
    "PROD": {
        "io_manager": bigquery_pandas_io_manager,
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": DBT_PROJECT_DIR,
                "profiles_dir": os.path.join(DBT_PROJECT_DIR, "profiles"),
                "target": "PROD",
            }
        ),
    },
}
