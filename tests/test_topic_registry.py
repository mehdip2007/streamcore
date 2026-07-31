"""
Tests for TopicRegistry — the single source of truth mapping event
types to Kafka topics.
"""
from producers.core.config import get_kafka_settings
from producers.core.topic_registry import TopicRegistry
from producers.events.schemas import (
    BaseEvent,
    DeviceType,
    VideoQuality,
    VideoViewEvent,
    WatchProgressEvent,
)


def _view_event() -> VideoViewEvent:
    return VideoViewEvent(
        event_id="e_test_view",
        user_id="u_test",
        session_id="s_test",
        device_type=DeviceType.MOBILE_IOS,
        ip_address="1.2.3.4",
        country_code="TR",
        video_id="v_test",
        video_title="Test Video",
        video_duration_seconds=300,
        initial_quality=VideoQuality.Q_720P,
    )


def _progress_event() -> WatchProgressEvent:
    return WatchProgressEvent(
        event_id="e_test_progress",
        user_id="u_test",
        session_id="s_test",
        device_type=DeviceType.MOBILE_IOS,
        ip_address="1.2.3.4",
        country_code="TR",
        video_id="v_test",
        position_seconds=10,
        quality=VideoQuality.Q_720P,
    )


class TestTopicRegistry:
    def test_routes_view_event_to_configured_topic(self):
        registry = TopicRegistry()
        settings = get_kafka_settings()
        assert registry.get_topic(_view_event()) == settings.topic_view_events

    def test_routes_progress_event_to_configured_topic(self):
        registry = TopicRegistry()
        settings = get_kafka_settings()
        assert registry.get_topic(_progress_event()) == settings.topic_watch_events

    def test_view_and_progress_use_different_topics(self):
        registry = TopicRegistry()
        assert registry.get_topic(_view_event()) != registry.get_topic(_progress_event())

    def test_all_topics_returns_every_registered_topic(self):
        registry = TopicRegistry()
        topics = registry.all_topics
        assert registry.get_topic(_view_event()) in topics
        assert registry.get_topic(_progress_event()) in topics
        assert len(topics) == 2

    def test_unregistered_event_type_raises_key_error(self):
        class UnregisteredEvent(BaseEvent):
            pass

        registry = TopicRegistry()
        unregistered = UnregisteredEvent(
            event_id="e_x",
            event_type="unregistered",
            user_id="u_x",
            session_id="s_x",
            device_type=DeviceType.WEB_DESKTOP,
            ip_address="1.2.3.4",
            country_code="US",
        )

        try:
            registry.get_topic(unregistered)
            raise AssertionError("Should have raised KeyError")
        except KeyError:
            pass
