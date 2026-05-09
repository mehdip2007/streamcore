"""
Postgres sink — writes consumed events to the database.

Architecture terminology:
  - SINK: A destination that receives data (Postgres, BigQuery, S3)
  - SOURCE: An origin that produces data (Kafka topic, API, file)

This sink writes raw events to the streamcore_raw.events table.
It follows the BRONZE LAYER pattern — store everything raw first,
transform later. Never discard data at ingestion time.

Why store as JSONB?
-------------------
Our events evolve over time. New fields get added. If we store events
in strict relational columns, adding a field requires an ALTER TABLE
which locks the table in production. JSONB absorbs schema evolution
gracefully — old and new events coexist without migration.

We'll apply strict schema in the SILVER layer (dbt models) later.
"""
import json
from contextlib import contextmanager
from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row

from producers.core.config import get_postgres_settings
from producers.core.logging_setup import get_logger

log = get_logger(__name__)


@dataclass
class KafkaMetadata:
    """Metadata from the Kafka message — tracked for debugging and replay."""

    topic: str
    partition: int
    offset: int


class PostgresSink:
    """
    Writes events to Postgres.

    Connection management:
    We use a single persistent connection per sink instance.
    For a single-consumer setup this is fine. In a multi-threaded
    consumer you'd use a connection pool (psycopg.pool.ConnectionPool).

    We'll upgrade to a pool when we add parallel consumers in Slice 3.
    """

    # How many events to batch before committing to Postgres.
    # Batch inserts are much faster than one-by-one inserts.
    # 100 events = one network round trip instead of 100.
    BATCH_SIZE = 100

    def __init__(self) -> None:
        settings = get_postgres_settings()
        self._dsn = settings.dsn
        self._conn: psycopg.Connection | None = None
        self._pending: list[dict] = []

        log.info("postgres_sink_initialized", host=settings.host, db=settings.db)

    def connect(self) -> None:
        """Open the database connection. Called once at startup."""
        self._conn = psycopg.connect(
            self._dsn,
            row_factory=dict_row,
            autocommit=False,  # We manage transactions manually
        )
        log.info("postgres_connected")

    @contextmanager
    def _transaction(self):
        """
        Context manager for database transactions.

        Usage:
            with self._transaction():
                cursor.execute(...)
                cursor.execute(...)
            # commits on exit, rolls back on exception

        Why manual transactions?
        psycopg's autocommit=False means every statement is in a transaction,
        but we want to batch multiple inserts in one transaction for performance.
        """
        try:
            yield
            self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            log.error("postgres_transaction_rolled_back", error=str(e))
            raise

    def write(
        self,
        event_id: str,
        event_type: str,
        event_timestamp: str,
        payload: dict,
        kafka_meta: KafkaMetadata,
    ) -> None:
        """
        Stage an event for writing.

        Events are buffered locally and flushed to Postgres in batches.
        This is called for every event consumed from Kafka.
        """
        self._pending.append({
            "event_id": event_id,
            "event_type": event_type,
            "event_timestamp": event_timestamp,
            "payload": json.dumps(payload),
            "kafka_topic": kafka_meta.topic,
            "kafka_partition": kafka_meta.partition,
            "kafka_offset": kafka_meta.offset,
        })

        # Flush when batch is full
        if len(self._pending) >= self.BATCH_SIZE:
            self.flush()

    def flush(self) -> None:
        """Write all pending events to Postgres in a single transaction."""
        if not self._pending:
            return

        count = len(self._pending)

        with self._transaction():
            with self._conn.cursor() as cur:
                # executemany sends all rows in one round trip —
                # dramatically faster than looping with execute()
                cur.executemany(
                    """
                    INSERT INTO streamcore_raw.events (
                        event_id,
                        event_type,
                        event_timestamp,
                        payload,
                        kafka_topic,
                        kafka_partition,
                        kafka_offset
                    ) VALUES (
                        %(event_id)s,
                        %(event_type)s,
                        %(event_timestamp)s,
                        %(payload)s::jsonb,
                        %(kafka_topic)s,
                        %(kafka_partition)s,
                        %(kafka_offset)s
                    )
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    self._pending,
                )

        log.info("postgres_batch_written", events=count)
        self._pending.clear()

    def close(self) -> None:
        """Flush remaining events and close the connection."""
        self.flush()
        if self._conn:
            self._conn.close()
            log.info("postgres_connection_closed")
