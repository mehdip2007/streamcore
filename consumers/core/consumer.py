"""
Kafka Consumer — reads events from topics and routes to sinks.

Consumer Groups:
---------------
Every Kafka consumer belongs to a group (group.id).
Kafka uses this to track which messages have been processed.

Within a group, each partition is assigned to exactly one consumer.
This means you can scale consumers horizontally — add more instances
and Kafka automatically rebalances partitions between them.

Offset Management:
-----------------
An OFFSET is Kafka's bookmark — it tracks which messages a consumer
has processed. We use MANUAL offset commits (enable.auto.commit=false).

Why manual? Because auto-commit confirms messages as "processed" as
soon as they're received — BEFORE we've written them to Postgres.
If we crash between receiving and writing, messages are marked done
but never actually stored. Data loss.

Manual commit: receive → write to Postgres → THEN commit offset.
This is the AT-LEAST-ONCE delivery guarantee.
"""
from confluent_kafka import Consumer, KafkaError, KafkaException

from consumers.sinks.postgres_sink import KafkaMetadata, PostgresSink
from producers.core.logging_setup import get_logger

log = get_logger(__name__)


class StreamCoreConsumer:
    """
    Consumes events from Kafka and writes to a sink.

    The consumer is intentionally generic — it doesn't know or care
    whether the sink is Postgres, BigQuery, or S3. It just:
      1. Reads a message from Kafka
      2. Hands the payload to the sink
      3. Commits the offset after successful write

    Swapping Postgres for BigQuery in Slice 5 means changing
    one line: the sink passed into this class. Nothing else changes.

    This is the STRATEGY pattern — the sink is a pluggable strategy.
    """

    def __init__(
        self,
        topics: list[str],
        sink: PostgresSink,
        group_id: str = "streamcore-consumer-group",
    ) -> None:
        self._sink = sink
        self._topics = topics
        self._running = False

        kafka_config = {
            # For local dev we still use localhost — consumers run
            # on your laptop, outside Docker
            "bootstrap.servers": "localhost:9092",

            # Consumer group ID — Kafka tracks offsets per group
            "group.id": group_id,

            # Where to start reading if this group has no committed offset yet.
            # "earliest" = read from the very beginning of the topic.
            # "latest" = only read new messages (skip historical).
            # We use "earliest" so we process everything from startup.
            "auto.offset.reset": "earliest",

            # Critical: disable auto-commit so WE control when offsets advance
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }

        self._consumer = Consumer(kafka_config)
        log.info(
            "kafka_consumer_initialized",
            topics=topics,
            group_id=group_id,
        )

    def start(self) -> None:
        """
        Subscribe to topics and begin consuming.

        This is a BLOCKING loop — it runs until stop() is called.
        In production this runs in its own process or container.
        """
        self._consumer.subscribe(self._topics)
        self._running = True

        log.info("kafka_consumer_started", topics=self._topics)

        try:
            while self._running:
                # poll() blocks for up to 1 second waiting for a message.
                # timeout=1.0 means we check self._running every second,
                # so Ctrl+C response is snappy.
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    # No message arrived in the timeout window — normal, keep looping
                    continue

                if msg.error():
                    self._handle_error(msg.error())
                    continue

                self._process_message(msg)

        except KeyboardInterrupt:
            log.info("kafka_consumer_interrupted")

        finally:
            self._shutdown()

    def _process_message(self, msg) -> None:
        """
        Process a single Kafka message.

        Flow:
          1. Decode bytes → dict
          2. Write to sink
          3. Commit offset (only AFTER successful write)
        """
        import json

        try:
            payload = json.loads(msg.value().decode("utf-8"))

            kafka_meta = KafkaMetadata(
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

            self._sink.write(
                event_id=payload["event_id"],
                event_type=payload["event_type"],
                event_timestamp=payload["event_timestamp"],
                payload=payload,
                kafka_meta=kafka_meta,
            )

            # Commit AFTER successful write — at-least-once guarantee.
            # store_offsets() only stages the offset locally; without an
            # explicit commit() it is never sent to the broker, so a
            # restarted consumer group has no durable checkpoint and
            # falls back to auto.offset.reset (replaying the whole
            # topic). asynchronous=False keeps this call on the hot
            # path simple; move to a periodic async commit if commit
            # latency ever becomes a bottleneck.
            self._consumer.store_offsets(msg)
            self._consumer.commit(message=msg, asynchronous=False)

            log.debug(
                "message_processed",
                event_type=payload.get("event_type"),
                offset=msg.offset(),
                partition=msg.partition(),
            )

        except (json.JSONDecodeError, KeyError) as e:
            # Malformed message — log and skip.
            # In production: send to dead letter queue.
            log.error(
                "message_processing_failed",
                error=str(e),
                topic=msg.topic(),
                offset=msg.offset(),
            )

    def _handle_error(self, error: KafkaError) -> None:
        """Handle Kafka-level errors vs normal end-of-partition signals."""
        if error.code() == KafkaError._PARTITION_EOF:
            # Not an error — just means we've read all current messages.
            # New messages will arrive later. Keep polling.
            log.debug("partition_eof_reached")
        else:
            log.error("kafka_consumer_error", error=str(error))
            raise KafkaException(error)

    def stop(self) -> None:
        """Signal the consume loop to stop."""
        self._running = False

    def _shutdown(self) -> None:
        """Flush the sink and close the consumer cleanly."""
        log.info("kafka_consumer_shutting_down")
        self._sink.flush()
        self._consumer.close()
        log.info("kafka_consumer_closed")
