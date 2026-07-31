# Extends the official Airflow image with dbt-clickhouse so the
# streamcore_dbt project (see streamcore_dbt/dbt_project.yml) can run
# as a scheduled Airflow task instead of a manual `dbt build`.
#
# dbt isn't a dependency of the main pyproject.toml — it lives entirely
# inside this image and streamcore_dbt/, same as it does for local use.
FROM apache/airflow:2.9.3-python3.11

RUN pip install --no-cache-dir "dbt-clickhouse>=1.8,<1.9"
