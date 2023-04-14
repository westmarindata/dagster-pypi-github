# Dagster Pypi / Github

This starter kit shows how to build the Dagster's
Software-Defined Assets for an Analytics ETL Workflow with
different deployments for a local and prod environment.

Locally, this project uses DuckDB, dbt, and CSV files to load sample
data and generate example models.

In production, this project uses BigQuery along with the pandas
io manager to fetch data on Pypi downloads from the Bigquery public
dataset, as well as Steampipe to fetch the latest Github data
for stars, forks, and other metrics.

## Prequisites

### Local Development

To run locally, all you need is to install this package locally:

```bash
pip install -e .
```

Then start dagit

```bash
dagit dev
```


### Production
The production version requires a few things to run:

- A BigQuery project with application credentials configured
- A running Steampipe service with [Github credentials](https://hub.steampipe.io/plugins/turbot/github#credentials)
  configured for API access.
- A Hex account with an API key and a project to run

## Using environment variables to handle secrets

Dagster allows using environment variables to handle sensitive information. You can define various configuration options and access environment variables through them. This also allows you to parameterize your pipeline without modifying code.

- BIGQUERY
  - `BIGQUERY_PROJECT` e.g. `my-bigquery-project`
- HEX
  - `HEX_API_KEY` e.g. `super-secret-api-key`
  - `HEX_PROJECT_ID` e.g. `abc-123-def`
- STEAMPIPE
  - `STEAMPIPE_CONN` e.g. `postgresql://steampipe:hunter42@localhost:9193/steampipe`

You can declare environment variables in various ways:
- **Local development**: [Using `.env` files to load env vars into local environments](https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets#declaring-environment-variables)
- **Dagster Cloud**: [Using the Dagster Cloud UI](https://docs.dagster.io/master/dagster-cloud/developing-testing/environment-variables-and-secrets#using-the-dagster-cloud-ui) to manage environment variables
- **Dagster Open Source**: How environment variables are set for Dagster projects deployed on your infrastructure depends on where Dagster is deployed. Read about how to declare environment variables [here](https://docs.dagster.io/master/guides/dagster/using-environment-variables-and-secrets#declaring-environment-variables).

Check out [Using environment variables and secrets guide](https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets) for more info and examples.
