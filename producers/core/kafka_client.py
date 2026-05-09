"""
Kafka Producer Client.

This module wraps the confluent-kafka producer with:
  - Automatic serialization (Pydantic → JSON → bytes)
  - Delivery callbacks (did the message actually arrive?)
  - Graceful shutdown (flush pending messages before exit)
  - Structured error logging

Why wrap the client instead of using it directly?
-------------------------------------------------
The raw confluent-kafka API is low-level. It deals with bytes,
callbacks, and C-level errors. By wrapping it here, the rest of
our code works with clean Python objects (Pydantic models) and
never touches serialization or error handling directly.

This is the ADAPTER pattern — translate between our clean domain
(Pydantic events) and the external system (Kafka bytes).
"""
import json
from typing import Callable

from confluent_kafka import Producer
from confluent_kafka import KafkaException

from producers.core.config import get_kafka_settings
from producers.core.logging_setup import get_logger
from producers.events.schemas import BaseEvent

log = get_logger(__name__)


def _on_delivery(err, msg) -> None:
    """
    Delivery callback — called by Kafka for every message.

    Kafka is asynchronous by default. When you call producer.produce(),
    the message goes into a local buffer. The actual send happens in the
    background. This callback tells you whether it succeeded or failed.

    Two outcomes:
      - err is None  → message safely written to a broker partition
      - err is set   → message was lost — log it and alert

    In a financial system you'd also push failures to a dead letter queue.
    We'll add that in a later slice.
    """
    if err is not None:
        log.error(
            "kafka_delivery_failed",
            error=str(err),
            topic=msg.topic(),
            partition=msg.partition(),
        )
    else:
        log.debug(
            "kafka_delivery_confirmed",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


class KafkaProducerClient:
    """
    Thread-safe Kafka producer wrapper.

    Lifecycle:
        1. Instantiate once at application startup
        2. Call .send() for every event
        3. Call .close() on shutdown (flushes pending messages)

    Usage:
        client = KafkaProducerClient()
        client.send(topic="streamcore.video.views", event=view_event)
        client.close()
    """

    def __init__(
        self,
        on_delivery: Callable | None = None,
    ) -> None:
        settings = get_kafka_settings()

        # confluent-kafka uses a flat dict for configuration.
        # Full config reference: https://kafka.apache.org/documentation/#producerconfigs
        kafka_config = {
            # Where to find the brokers
            "bootstrap.servers": settings.bootstrap_servers,

            # Who we are — useful for monitoring and debugging
            "client.id": settings.client_id,

            # acks=all means ALL in-sync replicas must confirm the write.
            # This is the strongest durability guarantee Kafka offers.
            # Slower than acks=1, but zero data loss on broker failure.
            "acks": "all",

            # Retry on transient failures (network blip, leader election)
            # with exponential backoff
            "retries": 5,
            "retry.backoff.ms": 300,

            # Batching — wait up to 10ms to batch messages together.
            # Improves throughput at the cost of tiny latency increase.
            # Fine for our use case — events are not microsecond-sensitive.
            "linger.ms": 10,

            # Compress messages with snappy — good balance of speed/ratio
            "compression.type": "snappy",
        }

        self._producer = Producer(kafka_config)
        self._delivery_callback = on_delivery or _on_delivery

        log.info(
            "kafka_producer_initialized",
            bootstrap_servers=settings.bootstrap_servers,
            client_id=settings.client_id,
        )

    def send(self, topic: str, event: BaseEvent) -> None:
        """
        Serialize and send an event to a Kafka topic.

        The message key is the user_id. This is critical:
        Kafka uses the key to determine which partition receives the message.
        Same key → same partition → ordered delivery per user.

        Why does ordering per user matter?
        If user u_123's events land in random partitions, a consumer
        might process their 'stop watching' event before their 'start watching'
        event. Keying by user_id prevents this.
        """
        try:
            # Serialize the Pydantic model to JSON bytes
            # model_dump(mode="json") handles datetime → ISO string conversion
            payload_bytes = json.dumps(
                event.model_dump(mode="json")
            ).encode("utf-8")

            # The key determines partitioning
            key_bytes = event.user_id.encode("utf-8")

            self._producer.produce(
                topic=topic,
                key=key_bytes,
                value=payload_bytes,
                on_delivery=self._delivery_callback,
            )

            # poll(0) triggers delivery callbacks without blocking.
            # Without this, callbacks would only fire during flush().
            self._producer.poll(0)

        except KafkaException as e:
            log.error(
                "kafka_produce_error",
                topic=topic,
                event_id=event.event_id,
                error=str(e),
            )
            raise

        except BufferError:
            # Producer's internal queue is full — too many undelivered messages.
            # This means the broker is slow or unreachable.
            # We flush (wait for delivery) then retry.
            log.warning(
                "kafka_buffer_full_flushing",
                topic=topic,
            )
            self._producer.flush(timeout=10)
            self._producer.produce(
                topic=topic,
                key=key_bytes,
                value=payload_bytes,
                on_delivery=self._delivery_callback,
            )

    def flush(self, timeout: float = 30.0) -> None:
        """
        Wait for all pending messages to be delivered.

        Call this before shutting down to ensure no messages are lost.
        timeout=30 means we wait up to 30 seconds for pending deliveries.
        """
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            log.warning(
                "kafka_flush_incomplete",
                undelivered_messages=remaining,
            )
        else:
            log.info("kafka_flush_complete")

    def close(self) -> None:
        """Graceful shutdown — flush then close."""
        log.info("kafka_producer_closing")
        self.flush()
