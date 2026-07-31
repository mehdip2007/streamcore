"""
Airflow DAG — batch dbt orchestration (Slice 4).

Why this exists
----------------
Slices 2 and 3 are ALWAYS-ON: the producer, consumer, and Spark job run
continuously, processing events in near-real-time. dbt is different —
it's a BATCH transformation. Staging/intermediate/mart models just read
whatever currently sits in Postgres and rebuild derived tables from it.

Airflow's job here isn't to replace the streaming pipeline — it's to
run that batch step on a schedule, retry it if it fails, and give us
one place to see whether the last run succeeded. The producer/consumer/
Spark job keep running whether or not this DAG (or Airflow itself) is up.

Why BashOperator and not a dedicated dbt provider (e.g. astronomer-cosmos)?
----------------------------------------------------------------------------
Cosmos turns each dbt model into its own Airflow task with full lineage
in the Airflow UI — the production-grade answer. That's a second tool to
learn on top of Airflow itself, though. This vertical slice starts with
the simplest thing that works, shelling out to `dbt build`, and can grow
into per-model tasks once the orchestration layer itself is understood.
"""
from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# dbt only works when invoked from inside its own project directory —
# there's no root-level dbt_project.yml (see CLAUDE.md). This path is
# where docker-compose.yml's airflow service bind-mounts streamcore_dbt/.
DBT_PROJECT_DIR = "/opt/airflow/streamcore_dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"

with DAG(
    dag_id="streamcore_dbt_batch",
    description=(
        "Rebuild staging/intermediate/mart models from "
        "streamcore_raw.events and streamcore_aggregated.*"
    ),
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["streamcore", "dbt", "slice-4"],
) as dag:
    # `dbt build` = `dbt run` + `dbt test` in dependency order, so a
    # broken model or a failed data test both fail this task instead
    # of silently shipping bad data to the marts.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt build --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )
