"""
Entry point for the event producer.

Slice 2: Events now flow to Kafka instead of just console.

Flow:
    Simulator → KafkaProducerClient → Kafka topic
                                    → streamcore.video.views
                                    → streamcore.video.watch_progress

Run it:
    python -m scripts.run_producer

Or with custom rate:
    PRODUCER_EVENTS_PER_SECOND=50 python -m scripts.run_producer
"""
import time

from producers.core.config import get_producer_settings
from producers.core.kafka_client import KafkaProducerClient
from producers.core.logging_setup import configure_logging, get_logger
from producers.core.topic_registry import TopicRegistry
from producers.events.simulator import EventGenerator


def main() -> None:
    # Step 1: Configure structured logging FIRST
    configure_logging()
    log = get_logger(__name__)

    settings = get_producer_settings()
    sleep_interval = 1.0 / settings.events_per_second

    # Step 2: Initialize Kafka client and topic registry
    # These are created ONCE and reused for every event
    kafka = KafkaProducerClient()
    registry = TopicRegistry()
    generator = EventGenerator()

    log.info(
        "producer_starting",
        events_per_second=settings.events_per_second,
        simulated_users=settings.simulated_users,
        simulated_videos=settings.simulated_videos,
        topics=registry.all_topics,
    )

    event_count = 0
    start_time = time.time()

    try:
        for event in generator.stream():
            event_count += 1

            # Step 3: Route event to the correct topic and send
            topic = registry.get_topic(event)
            kafka.send(topic=topic, event=event)

            # Step 4: Log every event at DEBUG level
            # (use LOG_LEVEL=DEBUG in .env to see all events)
            log.debug(
                "event_sent",
                event_type=event.event_type,
                event_id=event.event_id,
                user_id=event.user_id,
                topic=topic,
            )

            # Step 5: Throughput report every 100 events at INFO level
            if event_count % 100 == 0:
                elapsed = time.time() - start_time
                actual_rate = event_count / elapsed
                log.info(
                    "throughput_report",
                    total_events=event_count,
                    elapsed_seconds=round(elapsed, 2),
                    events_per_second=round(actual_rate, 2),
                )

            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        log.info(
            "producer_stopping",
            total_events=event_count,
            duration_seconds=round(time.time() - start_time, 2),
        )

    finally:
        # ALWAYS flush on exit — never lose buffered messages
        kafka.close()
        log.info(
            "producer_stopped",
            total_events=event_count,
        )


if __name__ == "__main__":
    main()
