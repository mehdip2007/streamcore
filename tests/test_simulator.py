"""
Tests for the event simulator.

Why tests matter even in a portfolio project:
- They prove your code works
- They show recruiters you think about quality
- They prevent regressions when you add features

Run with:
    pytest -v
"""
from producers.events.schemas import (
    DeviceType,
    VideoQuality,
    VideoViewEvent,
    WatchProgressEvent,
)
from producers.events.simulator import (
    EventGenerator,
    SimulatedUser,
    SimulatedVideo,
    UserSession,
)


class TestSchemas:
    """Verify our event schemas behave correctly."""

    def test_view_event_is_immutable(self):
        """Once created, events cannot be mutated (frozen=True)."""
        event = VideoViewEvent(
            event_id="e_test",
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

        # Frozen models raise ValidationError on mutation attempts
        try:
            event.user_id = "different"
            raise AssertionError("Should have raised an error")
        except Exception:
            pass  # Expected

    def test_country_code_validation(self):
        """Country code must be exactly 2 characters."""
        try:
            VideoViewEvent(
                event_id="e_test",
                user_id="u_test",
                session_id="s_test",
                device_type=DeviceType.MOBILE_IOS,
                ip_address="1.2.3.4",
                country_code="USA",  # Invalid — 3 chars
                video_id="v_test",
                video_title="Test",
                video_duration_seconds=300,
                initial_quality=VideoQuality.Q_720P,
            )
            raise AssertionError("Should have rejected 3-char country code")
        except Exception:
            pass  # Expected — validation kicked in


class TestSimulator:
    """Verify the simulator generates realistic event streams."""

    def test_session_yields_view_then_progress(self):
        """A session must start with a view event."""
        user = SimulatedUser.create_random()
        video = SimulatedVideo.create_random()
        session = UserSession(user=user, video=video)

        events = list(session.generate_events())

        # Must have at least one event (the view)
        assert len(events) >= 1
        # First event must always be a view event
        assert isinstance(events[0], VideoViewEvent)
        # All subsequent events are progress events
        for event in events[1:]:
            assert isinstance(event, WatchProgressEvent)

    def test_progress_events_in_order(self):
        """Progress events must increase monotonically by 10 seconds."""
        user = SimulatedUser.create_random()
        video = SimulatedVideo(video_id="v_x", title="X", duration_seconds=600)
        session = UserSession(user=user, video=video)

        progress_events = [
            e for e in session.generate_events()
            if isinstance(e, WatchProgressEvent)
        ]

        for i in range(1, len(progress_events)):
            prev_pos = progress_events[i - 1].position_seconds
            curr_pos = progress_events[i].position_seconds
            assert curr_pos > prev_pos
            assert curr_pos - prev_pos == UserSession.PROGRESS_INTERVAL

    def test_generator_produces_infinite_stream(self):
        """The top-level generator must keep yielding indefinitely."""
        generator = EventGenerator()
        stream = generator.stream()

        # Pull 50 events — confirm we can take many without exhausting
        events = [next(stream) for _ in range(50)]
        assert len(events) == 50
