"""
Kafka topic registry and event router.

Why centralize topic names?
---------------------------
Topic names are used in both producers AND consumers. If you hardcode
"streamcore.video.views" in 5 different files and then rename the topic,
you have 5 places to update and will miss one.

Central registry = single source of truth. Change it once, everywhere updates.

The router answers: "Given this event type, which topic does it go to?"
This keeps routing logic out of both the producer AND the simulator.
"""
from producers.core.config import get_kafka_settings
from producers.events.schemas import BaseEvent, VideoViewEvent, WatchProgressEvent


class TopicRegistry:
    """
    Maps event types to their Kafka topics.

    Adding a new event type?
    1. Add its schema to schemas.py
    2. Add its topic to .env.example
    3. Register it here
    That's the only 3 places you touch. Everything else just works.
    """

    def __init__(self) -> None:
        settings = get_kafka_settings()
        self._routes: dict[type[BaseEvent], str] = {
            VideoViewEvent: settings.topic_view_events,
            WatchProgressEvent: settings.topic_watch_events,
        }

    def get_topic(self, event: BaseEvent) -> str:
        """
        Return the Kafka topic for a given event.

        Raises KeyError if no topic is registered for the event type.
        This is intentional — fail loudly rather than silently dropping events.
        """
        topic = self._routes.get(type(event))
        if topic is None:
            raise KeyError(
                f"No Kafka topic registered for event type: {type(event).__name__}. "
                f"Register it in TopicRegistry._routes."
            )
        return topic

    @property
    def all_topics(self) -> list[str]:
        """Return all registered topic names — useful for consumer subscription."""
        return list(self._routes.values())
