"""
User behavior simulator.

This module generates REALISTIC event streams — not random noise.
Real users don't generate events uniformly. They:
  - Start a video (1 view event)
  - Watch for some time (many progress events)
  - Sometimes stop early (drop-off)
  - Sometimes change quality (network conditions)
  - Have device patterns (mobile users buffer more)

Modeling realistic behavior matters because downstream analytics
and ML models trained on this data need realistic patterns to be useful.

This is also a great learning exercise in clean OOP — notice how
each class has a single, clear responsibility.
"""
import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass

from faker import Faker

from producers.core.config import get_producer_settings
from producers.events.schemas import (
    DeviceType,
    VideoQuality,
    VideoViewEvent,
    WatchProgressEvent,
)

# Seed Faker for reproducibility during development.
# In production this would be removed.
fake = Faker()
Faker.seed(42)
random.seed(42)


@dataclass(frozen=True)
class SimulatedUser:
    """A simulated user with stable attributes across their session."""

    user_id: str
    device_type: DeviceType
    ip_address: str
    country_code: str

    @classmethod
    def create_random(cls) -> "SimulatedUser":
        """Factory method — creates a random but realistic user."""
        return cls(
            user_id=f"u_{uuid.uuid4().hex[:8]}",
            device_type=random.choice(list(DeviceType)),
            ip_address=fake.ipv4(),
            country_code=fake.country_code(),
        )


@dataclass(frozen=True)
class SimulatedVideo:
    """A simulated video in the catalog."""

    video_id: str
    title: str
    duration_seconds: int

    @classmethod
    def create_random(cls) -> "SimulatedVideo":
        return cls(
            video_id=f"v_{uuid.uuid4().hex[:8]}",
            title=fake.sentence(nb_words=4).rstrip("."),
            # Real video durations follow a long-tail distribution —
            # most short, some long. We model this with random.choices.
            duration_seconds=random.choices(
                population=[60, 300, 600, 1200, 3600],  # 1m, 5m, 10m, 20m, 1h
                weights=[20, 40, 25, 10, 5],            # weighted distribution
                k=1,
            )[0],
        )


class UserSession:
    """
    Models a single user's video-watching session.

    A session generates a stream of events:
      1. One VideoViewEvent at the start
      2. Many WatchProgressEvents as the user watches
      3. The session ends when the user stops watching

    This is a generator pattern — events are yielded lazily,
    one at a time, instead of building a giant list in memory.
    """

    # Tick interval — how often watch progress events fire (in seconds of video time)
    PROGRESS_INTERVAL = 10

    def __init__(self, user: SimulatedUser, video: SimulatedVideo) -> None:
        self.user = user
        self.video = video
        self.session_id = f"s_{uuid.uuid4().hex[:8]}"
        self.current_quality = self._pick_initial_quality()

        # How long will this user actually watch? Models drop-off behavior.
        # Most users watch most of the video, some drop off early.
        self.watch_duration = self._compute_watch_duration()

    def _pick_initial_quality(self) -> VideoQuality:
        """Mobile users get lower quality, smart TVs get high quality."""
        if self.user.device_type in {DeviceType.SMART_TV, DeviceType.WEB_DESKTOP}:
            return random.choices(
                [VideoQuality.Q_1080P, VideoQuality.Q_4K],
                weights=[70, 30],
            )[0]
        return random.choices(
            [VideoQuality.Q_480P, VideoQuality.Q_720P, VideoQuality.Q_1080P],
            weights=[20, 50, 30],
        )[0]

    def _compute_watch_duration(self) -> int:
        """
        Model drop-off: some users finish, some leave early.

        Beta distribution skewed right means most users watch most of the video,
        but there's a long tail of early drop-off.
        """
        completion_ratio = random.betavariate(5, 2)  # mean ~0.71
        return int(self.video.duration_seconds * completion_ratio)

    def generate_events(self) -> Iterator[VideoViewEvent | WatchProgressEvent]:
        """Yield all events for this session, in order."""
        # 1. The view event — fired once at the start
        yield VideoViewEvent(
            event_id=f"e_{uuid.uuid4().hex}",
            user_id=self.user.user_id,
            session_id=self.session_id,
            device_type=self.user.device_type,
            ip_address=self.user.ip_address,
            country_code=self.user.country_code,
            video_id=self.video.video_id,
            video_title=self.video.title,
            video_duration_seconds=self.video.duration_seconds,
            initial_quality=self.current_quality,
            referrer=random.choice(["homepage", "search", "recommendation", None]),
        )

        # 2. Progress events — fired every PROGRESS_INTERVAL seconds
        for position in range(
            self.PROGRESS_INTERVAL,
            self.watch_duration + 1,
            self.PROGRESS_INTERVAL,
        ):
            # 5% chance of buffering on any tick (mobile users buffer more)
            buffer_chance = 0.10 if self.user.device_type.startswith("mobile") else 0.03
            is_buffering = random.random() < buffer_chance

            yield WatchProgressEvent(
                event_id=f"e_{uuid.uuid4().hex}",
                user_id=self.user.user_id,
                session_id=self.session_id,
                device_type=self.user.device_type,
                ip_address=self.user.ip_address,
                country_code=self.user.country_code,
                video_id=self.video.video_id,
                position_seconds=position,
                quality=self.current_quality,
                is_buffering=is_buffering,
                playback_rate=random.choice([1.0, 1.0, 1.0, 1.25, 1.5, 2.0]),
            )


class EventGenerator:
    """
    Top-level generator that produces an infinite stream of events.

    Maintains a pool of simulated users and videos. Continuously
    starts new sessions and yields their events.
    """

    def __init__(self) -> None:
        settings = get_producer_settings()
        self.users = [
            SimulatedUser.create_random()
            for _ in range(settings.simulated_users)
        ]
        self.videos = [
            SimulatedVideo.create_random()
            for _ in range(settings.simulated_videos)
        ]

    def stream(self) -> Iterator[VideoViewEvent | WatchProgressEvent]:
        """Infinite stream of events from random user sessions."""
        while True:
            user = random.choice(self.users)
            video = random.choice(self.videos)
            session = UserSession(user=user, video=video)
            yield from session.generate_events()
