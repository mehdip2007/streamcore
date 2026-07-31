"""
Tests for quality/checks.py — the custom data-quality checks (Slice 6).

psycopg and clickhouse_connect are both mocked so these run without a
live Postgres/ClickHouse instance, matching the style of
test_postgres_sink.py and test_consumer.py.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from quality.checks import (
    MART_TABLES,
    check_mart_tables_not_empty,
    check_raw_events_freshness,
    run_all_checks,
)


@pytest.fixture
def mock_pg_cursor():
    with patch("quality.checks.psycopg.connect") as connect_fn:
        mock_conn = MagicMock()
        connect_fn.return_value.__enter__.return_value = mock_conn
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
        yield mock_cursor


@pytest.fixture
def mock_clickhouse_client():
    with patch("quality.checks.clickhouse_connect.get_client") as get_client_fn:
        client = MagicMock()
        get_client_fn.return_value = client
        yield client


class TestRawEventsFreshness:
    def test_passes_when_newest_row_is_within_threshold(self, mock_pg_cursor):
        mock_pg_cursor.fetchone.return_value = (datetime.now(timezone.utc),)

        result = check_raw_events_freshness()

        assert result.passed is True
        assert result.name == "raw_events_freshness"

    def test_fails_when_newest_row_is_older_than_threshold(self, mock_pg_cursor):
        stale_time = datetime.now(timezone.utc) - timedelta(hours=2)
        mock_pg_cursor.fetchone.return_value = (stale_time,)

        result = check_raw_events_freshness()

        assert result.passed is False
        assert "min old" in result.detail

    def test_fails_when_table_is_empty(self, mock_pg_cursor):
        mock_pg_cursor.fetchone.return_value = (None,)

        result = check_raw_events_freshness()

        assert result.passed is False
        assert "empty" in result.detail


class TestMartTablesNotEmpty:
    def test_passes_when_tables_have_rows(self, mock_clickhouse_client):
        mock_clickhouse_client.query.return_value.result_rows = [[42]]

        results = check_mart_tables_not_empty()

        assert len(results) == len(MART_TABLES)
        assert all(r.passed for r in results)
        assert all("42 rows" in r.detail for r in results)

    def test_fails_when_a_table_is_empty(self, mock_clickhouse_client):
        mock_clickhouse_client.query.return_value.result_rows = [[0]]

        results = check_mart_tables_not_empty()

        assert all(not r.passed for r in results)

    def test_checks_every_mart_table(self, mock_clickhouse_client):
        mock_clickhouse_client.query.return_value.result_rows = [[1]]

        results = check_mart_tables_not_empty()

        checked_names = {r.name for r in results}
        assert checked_names == {f"{table}_not_empty" for table in MART_TABLES}


class TestRunAllChecks:
    def test_runs_freshness_and_all_mart_checks(self, mock_pg_cursor, mock_clickhouse_client):
        mock_pg_cursor.fetchone.return_value = (datetime.now(timezone.utc),)
        mock_clickhouse_client.query.return_value.result_rows = [[10]]

        results = run_all_checks()

        assert len(results) == 1 + len(MART_TABLES)
        assert results[0].name == "raw_events_freshness"
