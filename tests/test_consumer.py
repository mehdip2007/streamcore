"""
Tests for StreamCoreConsumer.

Includes a regression test for the offset-commit bug: store_offsets()
only stages an offset locally, so without a real commit() call the
offset is never durably persisted to the broker. See CLAUDE.md.

confluent_kafka.Consumer is patched throughout — these are unit tests
for the message-processing/error-handling logic, not integration tests
against a live broker.
"""
import json
from unittest.mock import ANY, MagicMock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException

from consumers.core.consumer import StreamCoreConsumer


def _payload(event_id: str = "e_1", event_type: str = "video_view") -> dict:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_timestamp": "2024-01-01T00:00:00Z",
        "user_id": "u_1",
    }


def _make_message(
    payload: dict,
    topic: str = "streamcore.video.views",
    partition: int = 0,
    offset: int = 5,
):
    msg = MagicMock()
    msg.value.return_value = json.dumps(payload).encode("utf-8")
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    return msg


@pytest.fixture
def mock_kafka_consumer():
    with patch("consumers.core.consumer.Consumer") as consumer_cls:
        instance = MagicMock()
        consumer_cls.return_value = instance
        yield instance


class TestProcessMessage:
    def test_writes_decoded_payload_to_sink(self, mock_kafka_consumer):
        sink = MagicMock()
        consumer = StreamCoreConsumer(topics=["streamcore.video.views"], sink=sink)
        payload = _payload()

        consumer._process_message(_make_message(payload))

        sink.write.assert_called_once_with(
            event_id="e_1",
            event_type="video_view",
            event_timestamp="2024-01-01T00:00:00Z",
            payload=payload,
            kafka_meta=ANY,
        )

    def test_commits_offset_after_store_offsets_regression(self, mock_kafka_consumer):
        """
        Regression test: _process_message must call BOTH store_offsets()
        AND commit() — the original bug staged the offset but never
        committed it, so restarts replayed the whole topic.
        """
        sink = MagicMock()
        consumer = StreamCoreConsumer(topics=["streamcore.video.views"], sink=sink)
        msg = _make_message(_payload())

        call_order = []
        mock_kafka_consumer.store_offsets.side_effect = (
            lambda *a, **k: call_order.append("store_offsets")
        )
        mock_kafka_consumer.commit.side_effect = lambda *a, **k: call_order.append("commit")

        consumer._process_message(msg)

        assert call_order == ["store_offsets", "commit"]
        mock_kafka_consumer.commit.assert_called_once_with(message=msg, asynchronous=False)

    def test_does_not_commit_when_sink_write_fails(self, mock_kafka_consumer):
        sink = MagicMock()
        sink.write.side_effect = RuntimeError("db unreachable")
        consumer = StreamCoreConsumer(topics=["streamcore.video.views"], sink=sink)

        with pytest.raises(RuntimeError):
            consumer._process_message(_make_message(_payload()))

        mock_kafka_consumer.store_offsets.assert_not_called()
        mock_kafka_consumer.commit.assert_not_called()

    def test_malformed_json_is_skipped_not_raised(self, mock_kafka_consumer):
        sink = MagicMock()
        consumer = StreamCoreConsumer(topics=["streamcore.video.views"], sink=sink)
        msg = MagicMock()
        msg.value.return_value = b"not valid json"
        msg.topic.return_value = "streamcore.video.views"
        msg.offset.return_value = 1

        consumer._process_message(msg)  # should not raise

        sink.write.assert_not_called()
        mock_kafka_consumer.store_offsets.assert_not_called()

    def test_missing_required_field_is_skipped_not_raised(self, mock_kafka_consumer):
        sink = MagicMock()
        consumer = StreamCoreConsumer(topics=["streamcore.video.views"], sink=sink)

        consumer._process_message(_make_message({"event_id": "e_1"}))  # no event_type/timestamp

        sink.write.assert_not_called()
        mock_kafka_consumer.store_offsets.assert_not_called()


class TestHandleError:
    def test_partition_eof_is_not_treated_as_an_error(self, mock_kafka_consumer):
        sink = MagicMock()
        consumer = StreamCoreConsumer(topics=["streamcore.video.views"], sink=sink)
        error = MagicMock()
        error.code.return_value = KafkaError._PARTITION_EOF

        consumer._handle_error(error)  # should not raise

    def test_other_kafka_errors_are_raised(self, mock_kafka_consumer):
        sink = MagicMock()
        consumer = StreamCoreConsumer(topics=["streamcore.video.views"], sink=sink)
        error = MagicMock()
        error.code.return_value = KafkaError._ALL_BROKERS_DOWN

        with pytest.raises(KafkaException):
            consumer._handle_error(error)


class TestShutdown:
    def test_shutdown_flushes_sink_and_closes_consumer(self, mock_kafka_consumer):
        sink = MagicMock()
        consumer = StreamCoreConsumer(topics=["streamcore.video.views"], sink=sink)

        consumer._shutdown()

        sink.flush.assert_called_once()
        mock_kafka_consumer.close.assert_called_once()
