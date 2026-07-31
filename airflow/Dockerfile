# Extends the official Airflow image with:
#   - dbt-clickhouse, so the streamcore_dbt project (see
#     streamcore_dbt/dbt_project.yml) can run as a scheduled Airflow task
#     instead of a manual `dbt build`.
#   - The specific runtime deps quality/checks.py needs (Slice 6's data
#     quality task) to import producers.core.config and talk to
#     Postgres/ClickHouse. Deliberately NOT `pip install -e .` against
#     the main pyproject.toml — that would also pull in pyspark and
#     confluent-kafka, which this container has no use for and which
#     would make the image far larger/slower to build than necessary.
FROM apache/airflow:2.9.3-python3.11

RUN pip install --no-cache-dir \
    "dbt-clickhouse>=1.8,<1.9" \
    "psycopg[binary]>=3.2.0" \
    "clickhouse-connect>=0.7.0" \
    "pydantic>=2.8.0" \
    "pydantic-settings>=2.4.0" \
    "python-dotenv>=1.0.0" \
    "structlog>=24.4.0"
