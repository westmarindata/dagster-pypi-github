import datetime
import os

import pandas as pd
from dagster import asset


@asset(
    key_prefix=["duckdb"],
)
def raw_pypi_downloads() -> pd.DataFrame:
    pypi_path = os.path.join(os.path.dirname(__file__), "../data/pypi_downloads.csv")
    return pd.read_csv(pypi_path)


@asset(
    key_prefix=["duckdb"],
)
def raw_github_stars() -> pd.DataFrame:
    github_path = os.path.join(
        os.path.dirname(__file__), "../data/github_star_count.csv"
    )
    df = pd.read_csv(github_path)
    df["date"] = datetime.datetime.today()
    return df


@asset(
    key_prefix=["duckdb"],
)
def pypi_downloads(raw_pypi_downloads) -> pd.DataFrame:
    df = raw_pypi_downloads
    return df


@asset(
    key_prefix=["duckdb"],
)
## TODO: How can I insert rows instead of overwriting the table?
## The Streampipe API has no historical record functionality
def github_stars(raw_github_stars) -> pd.DataFrame:
    df = raw_github_stars
    return df
