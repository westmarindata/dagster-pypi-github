from setuptools import find_packages, setup

setup(
    name="dagster_pypi",
    packages=find_packages(exclude=["dagster_pypi_tests"]),
    install_requires=[
        "dagster~=1.2.4",
        "dagster-cloud",
        "dagster-dbt~=0.18.6",
        "dagster-duckdb-pandas~=0.18.6",
        "dagster-gcp~=0.18.4",
        "dagster-gcp-pandas~=0.18.4",
        "dagster-hex~=0.1.2",
        "dagster-pandas~=0.18.4",
        "dbt-bigquery~=1.4.3",
        "dbt-duckdb~=1.4.1",
        "google-cloud-bigquery~=3.9.0",

    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
