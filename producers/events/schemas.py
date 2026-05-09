"""
Event schema definitions.

These schemas are THE CONTRACT between producers and consumers.
Once an event format is published to a Kafka topic, downstream
consumers depend on it. Breaking the schema breaks production.

Why Pydantic?
-------------
1. Validation — malformed events caught at the producer, never written
2. Serialization — automatic conversion to/from JSON
3. Type safety — IDE autocomplete, mypy checks
4. Self-documenting — the schema IS the documentation

In production, these schemas would live in a Schema Registry (Avro/Protobuf).
For local development, JSON + Pydantic is more than enough.
"""
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class DeviceType(str, Enum):
    """Enum of supported device types. Closed set — no surprises."""

    MOBILE_IOS = "mobile_ios"
    MOBILE_ANDROID = "mobile_android"
    WEB_DESKTOP = "web_desktop"
    WEB_MOBILE = "web_mobile"
    SMART_TV = "smart_tv"
    TABLET = "tablet"


class VideoQuality(str, Enum):
    """Standard streaming quality levels."""

    Q_240P = "240p"
    Q_480P = "480p"
    Q_720P = "720p"
    Q_1080P = "1080p"
    Q_4K = "4k"


class BaseEvent(BaseModel):
    """
    Base class for all events. Every event has these fields.

    'frozen=True' makes events immutable after creation —
    you cannot mutate an event once it's been built. This
    prevents an entire class of bugs in concurrent code.
    """

    model_config = ConfigDict(frozen=True, use_enum_values=True)

    event_id: str = Field(description="Unique identifier for this event (UUID)")
    event_type: str = Field(description="Discriminator — tells consumers what type of event this is")
    event_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When the event occurred (UTC, ISO 8601)",
    )
    user_id: str = Field(description="User who generated the event")
    session_id: str = Field(description="User's current session")
    device_type: DeviceType
    ip_address: str
    country_code: str = Field(min_length=2, max_length=2, description="ISO 3166-1 alpha-2")


class VideoViewEvent(BaseEvent):
    """
    Fired when a user starts watching a video.

    This is the 'play button pressed' event.
    """

    event_type: str = Field(default="video_view", frozen=True)
    video_id: str
    video_title: str
    video_duration_seconds: int = Field(ge=0)
    initial_quality: VideoQuality
    referrer: str | None = Field(
        default=None,
        description="Where the user came from (homepage, search, recommendation)",
    )


class WatchProgressEvent(BaseEvent):
    """
    Fired periodically while a user watches a video.

    Sent every ~10 seconds during playback. This is the high-volume
    event — most of our throughput comes from these.
    """

    event_type: str = Field(default="watch_progress", frozen=True)
    video_id: str
    position_seconds: int = Field(ge=0, description="Current playback position")
    quality: VideoQuality
    is_buffering: bool = Field(default=False)
    playback_rate: float = Field(default=1.0, ge=0.25, le=2.0)
