"""
Entry point for data-quality checks (Slice 6).

Run standalone:
    python -m scripts.run_data_quality_checks

Or as a scheduled Airflow task, immediately after dbt_build (see
airflow/dags/streamcore_dbt_dag.py) — this exits non-zero on any failed
check, so either caller can treat it as a pass/fail gate.
"""
import sys

from producers.core.logging_setup import configure_logging, get_logger
from quality.checks import run_all_checks


def main() -> None:
    configure_logging()
    log = get_logger(__name__)

    results = run_all_checks()

    for result in results:
        log_fn = log.info if result.passed else log.error
        log_fn(
            "data_quality_check",
            name=result.name,
            passed=result.passed,
            detail=result.detail,
        )

    failures = [r for r in results if not r.passed]
    if failures:
        log.error(
            "data_quality_checks_failed",
            failed_count=len(failures),
            total=len(results),
        )
        sys.exit(1)

    log.info("data_quality_checks_passed", total=len(results))


if __name__ == "__main__":
    main()
