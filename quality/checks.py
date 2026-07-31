"""
Custom data-quality checks (Slice 6).

These complement dbt's built-in column tests, the singular tests in
streamcore_dbt/tests/, and the source freshness check in
streamcore_dbt/models/staging/sources.yml. The difference: dbt tests only
run when someone runs `dbt build` or `dbt test`. These checks are meant
to run independently of that — as a scheduled Airflow task (see
airflow/dags/streamcore_dbt_dag.py) or standalone via
`python -m scripts.run_data_quality_checks` — so a stalled pipeline gets
caught even if nobody happens to be running dbt at the time.

Two kinds of check:
  1. Freshness — is new data actually arriving in Postgres? This is the
     root-cause signal: if the producer/consumer have stopped, everything
     downstream (Spark aggregates, the ClickHouse bridge, every dbt
     model) is stale too, so checking here catches it earliest.
  2. Volume — do the ClickHouse marts have data at all? A dbt run can
     report "Completed successfully" even if a mart ends up empty (an
     empty result set isn't a SQL error) — e.g. if the postgres_bridge
     on-run-start hooks silently failed to connect. This check exists
     specifically to catch what dbt's own success/failure signal can't.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import clickhouse_connect
import psycopg

from producers.core.config import (
    get_clickhouse_settings,
    get_data_quality_settings,
    get_postgres_settings,
)

MART_TABLES = ("mart_content_performance", "mart_device_quality")


@dataclass(frozen=True)
class CheckResult:
    """The outcome of a single data-quality check."""

    name: str
    passed: bool
    detail: str


def check_raw_events_freshness() -> CheckResult:
    """
    Fail if the newest row in streamcore_raw.events is older than
    DATA_QUALITY_FRESHNESS_THRESHOLD_MINUTES.
    """
    settings = get_postgres_settings()
    threshold = get_data_quality_settings().freshness_threshold_minutes

    with psycopg.connect(settings.dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT max(ingested_at) FROM streamcore_raw.events")
        (latest,) = cur.fetchone()

    if latest is None:
        return CheckResult(
            name="raw_events_freshness",
            passed=False,
            detail="streamcore_raw.events is empty — no data has ever landed",
        )

    age_minutes = (datetime.now(timezone.utc) - latest).total_seconds() / 60
    return CheckResult(
        name="raw_events_freshness",
        passed=age_minutes <= threshold,
        detail=f"newest row is {age_minutes:.1f} min old (threshold: {threshold} min)",
    )


def check_mart_tables_not_empty() -> list[CheckResult]:
    """
    Fail if either mart table is empty.
    """
    settings = get_clickhouse_settings()
    client = clickhouse_connect.get_client(
        host=settings.host,
        port=settings.port,
        username=settings.user,
        password=settings.password,
        database="streamcore_marts",
    )

    results = []
    for table in MART_TABLES:
        count = client.query(f"SELECT count(*) FROM {table}").result_rows[0][0]
        results.append(
            CheckResult(name=f"{table}_not_empty", passed=count > 0, detail=f"{count} rows")
        )
    return results


def run_all_checks() -> list[CheckResult]:
    """
    Run every data-quality check and return ALL results, not just
    failures — callers decide what to do with a clean bill of health
    vs. a failure (e.g. log everything, but only exit non-zero on fail).
    """
    return [check_raw_events_freshness(), *check_mart_tables_not_empty()]
