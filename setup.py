from setuptools import find_packages, setup

setup(
    name="dagster_pypi",
    packages=find_packages(exclude=["dagster_pypi_tests"]),
    install_requires=[
        "dagster~=1.2.4",
        "dagster-cloud",
        "dagster-gcp~=0.18.4",
        "dagster-gcp-pandas~=0.18.4",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
