"""
Entry point for the Postgres consumer.

This script:
  1. Connects to Postgres
  2. Subscribes to all Kafka topics
  3. Reads events and writes them to the raw events table

Run in a SEPARATE terminal from the producer:
    python -m scripts.run_consumer

Both producer and consumer run simultaneously —
producer writes to Kafka, consumer reads from Kafka.
That's the streaming pipeline working end-to-end.
"""
from consumers.core.consumer import StreamCoreConsumer
from consumers.sinks.postgres_sink import PostgresSink
from producers.core.logging_setup import configure_logging, get_logger
from producers.core.topic_registry import TopicRegistry


def main() -> None:
    configure_logging()
    log = get_logger(__name__)

    log.info("consumer_starting")

    # Step 1: Connect to Postgres
    sink = PostgresSink()
    sink.connect()

    # Step 2: Get all registered topics from the registry
    # This automatically includes any new topics we add later
    registry = TopicRegistry()
    topics = registry.all_topics

    log.info("subscribing_to_topics", topics=topics)

    # Step 3: Start consuming — this blocks until Ctrl+C
    consumer = StreamCoreConsumer(topics=topics, sink=sink)

    try:
        consumer.start()
    finally:
        sink.close()
        log.info("consumer_stopped")


if __name__ == "__main__":
    main()
