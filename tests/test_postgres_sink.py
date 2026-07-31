"""
Tests for PostgresSink — verifies batching, insert semantics, and
transaction rollback behavior without needing a live Postgres instance.

We bypass PostgresSink.connect() and inject a MagicMock connection
directly, since the interesting behavior (batching, SQL shape, rollback)
lives above the actual driver call.
"""
import json
from unittest.mock import MagicMock

import pytest

from consumers.sinks.postgres_sink import KafkaMetadata, PostgresSink


def _kafka_meta(offset: int = 0) -> KafkaMetadata:
    return KafkaMetadata(topic="streamcore.video.views", partition=0, offset=offset)


@pytest.fixture
def sink():
    instance = PostgresSink()
    instance._conn = MagicMock()
    return instance


class TestPostgresSink:
    def test_write_buffers_without_flushing_below_batch_size(self, sink):
        for i in range(PostgresSink.BATCH_SIZE - 1):
            sink.write(
                event_id=f"e_{i}",
                event_type="video_view",
                event_timestamp="2024-01-01T00:00:00Z",
                payload={"event_id": f"e_{i}"},
                kafka_meta=_kafka_meta(offset=i),
            )

        sink._conn.cursor.assert_not_called()
        assert len(sink._pending) == PostgresSink.BATCH_SIZE - 1

    def test_write_auto_flushes_once_batch_size_is_reached(self, sink):
        for i in range(PostgresSink.BATCH_SIZE):
            sink.write(
                event_id=f"e_{i}",
                event_type="video_view",
                event_timestamp="2024-01-01T00:00:00Z",
                payload={"event_id": f"e_{i}"},
                kafka_meta=_kafka_meta(offset=i),
            )

        sink._conn.cursor.assert_called_once()
        sink._conn.commit.assert_called_once()
        assert sink._pending == []

    def test_flush_is_a_noop_when_nothing_pending(self, sink):
        sink.flush()
        sink._conn.cursor.assert_not_called()

    def test_flush_inserts_with_on_conflict_do_nothing(self, sink):
        # executemany() is called with a reference to sink._pending, which
        # flush() clears in place right after — so we must snapshot the
        # rows inside the mock call itself, not read call_args afterward.
        captured = {}

        def _capture_rows(sql, rows):
            captured["sql"] = sql
            captured["rows"] = list(rows)

        cursor = sink._conn.cursor.return_value.__enter__.return_value
        cursor.executemany.side_effect = _capture_rows

        sink.write(
            event_id="e_1",
            event_type="video_view",
            event_timestamp="2024-01-01T00:00:00Z",
            payload={"foo": "bar"},
            kafka_meta=_kafka_meta(),
        )
        sink.flush()

        cursor.executemany.assert_called_once()
        assert "ON CONFLICT (event_id) DO NOTHING" in captured["sql"]
        assert captured["rows"][0]["event_id"] == "e_1"
        assert json.loads(captured["rows"][0]["payload"]) == {"foo": "bar"}

    def test_flush_rolls_back_and_reraises_on_error(self, sink):
        cursor = sink._conn.cursor.return_value.__enter__.return_value
        cursor.executemany.side_effect = RuntimeError("db exploded")

        sink.write(
            event_id="e_1",
            event_type="video_view",
            event_timestamp="2024-01-01T00:00:00Z",
            payload={"foo": "bar"},
            kafka_meta=_kafka_meta(),
        )

        with pytest.raises(RuntimeError):
            sink.flush()

        sink._conn.rollback.assert_called_once()
        sink._conn.commit.assert_not_called()

    def test_close_flushes_pending_then_closes_connection(self, sink):
        sink.write(
            event_id="e_1",
            event_type="video_view",
            event_timestamp="2024-01-01T00:00:00Z",
            payload={"foo": "bar"},
            kafka_meta=_kafka_meta(),
        )

        sink.close()

        sink._conn.cursor.assert_called_once()
        sink._conn.close.assert_called_once()
