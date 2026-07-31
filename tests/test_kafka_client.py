"""
Tests for KafkaProducerClient — verifies serialization, partition-key
derivation, and error-handling paths without needing a live Kafka broker.

confluent_kafka.Producer is a thin wrapper over a C client, so we patch
the class itself rather than spin up real infrastructure.
"""
import json
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException

from producers.core.kafka_client import KafkaProducerClient
from producers.events.schemas import DeviceType, VideoQuality, VideoViewEvent


def _view_event(user_id: str = "u_42") -> VideoViewEvent:
    return VideoViewEvent(
        event_id="e_test",
        user_id=user_id,
        session_id="s_test",
        device_type=DeviceType.MOBILE_IOS,
        ip_address="1.2.3.4",
        country_code="TR",
        video_id="v_test",
        video_title="Test Video",
        video_duration_seconds=300,
        initial_quality=VideoQuality.Q_720P,
    )


@pytest.fixture
def mock_producer():
    with patch("producers.core.kafka_client.Producer") as producer_cls:
        instance = MagicMock()
        producer_cls.return_value = instance
        yield instance


class TestKafkaProducerClient:
    def test_send_keys_message_by_user_id(self, mock_producer):
        client = KafkaProducerClient()
        client.send(topic="streamcore.video.views", event=_view_event(user_id="u_42"))

        _, kwargs = mock_producer.produce.call_args
        assert kwargs["topic"] == "streamcore.video.views"
        assert kwargs["key"] == b"u_42"

    def test_send_serializes_event_as_json(self, mock_producer):
        client = KafkaProducerClient()
        client.send(topic="streamcore.video.views", event=_view_event())

        _, kwargs = mock_producer.produce.call_args
        decoded = json.loads(kwargs["value"].decode("utf-8"))
        assert decoded["event_id"] == "e_test"
        assert decoded["video_id"] == "v_test"

    def test_send_triggers_nonblocking_poll(self, mock_producer):
        client = KafkaProducerClient()
        client.send(topic="streamcore.video.views", event=_view_event())
        mock_producer.poll.assert_called_once_with(0)

    def test_buffer_error_flushes_then_retries_once(self, mock_producer):
        mock_producer.produce.side_effect = [BufferError("queue full"), None]
        client = KafkaProducerClient()

        client.send(topic="streamcore.video.views", event=_view_event())

        assert mock_producer.produce.call_count == 2
        mock_producer.flush.assert_called_once_with(timeout=10)

    def test_kafka_exception_is_reraised(self, mock_producer):
        mock_producer.produce.side_effect = KafkaException("broker unreachable")
        client = KafkaProducerClient()

        with pytest.raises(KafkaException):
            client.send(topic="streamcore.video.views", event=_view_event())

    def test_flush_does_not_raise_when_messages_remain_undelivered(self, mock_producer):
        mock_producer.flush.return_value = 3
        client = KafkaProducerClient()
        client.flush(timeout=5)
        mock_producer.flush.assert_called_with(timeout=5)
