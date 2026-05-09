"""
Watch Aggregator — PySpark Structured Streaming Job.

This job reads from Kafka topics and computes three real-time aggregations:

  1. Concurrent viewers per video (30-second tumbling windows)
  2. Buffering rate by device type (1-minute tumbling windows)
  3. Top videos by view count (5-minute tumbling windows)

Key Concepts Used Here:
-----------------------

STRUCTURED STREAMING:
  PySpark treats a Kafka stream as an unbounded DataFrame.
  You write the same DataFrame operations you'd write for batch data
  (groupBy, agg, filter). Spark handles the streaming complexity.
  This is the "unified batch + streaming" philosophy.

WATERMARKING:
  Events arrive LATE. A user's phone might buffer events and send them
  2 minutes after they actually happened. Without watermarking, Spark
  must keep all windows open forever (infinite memory).

  Watermark = "how late can events arrive before we discard them?"
  .withWatermark("event_timestamp", "2 minutes") means:
    - Keep windows open for 2 extra minutes past their end time
    - Drop events older than 2 minutes late
    - Free the memory for closed windows

TUMBLING WINDOWS vs SLIDING WINDOWS:
  Tumbling: [0:00-0:30] [0:30-1:00] [1:00-1:30] — no overlap
  Sliding:  [0:00-0:30] [0:10-0:40] [0:20-0:50] — overlapping
  We use tumbling — simpler, less memory, right for dashboards.

OUTPUT MODES:
  - append:  Only write NEW rows to sink (can't update existing)
  - update:  Only write CHANGED rows (good for aggregations)
  - complete: Write ALL rows every trigger (expensive, avoid)
  We use 'update' for aggregations — only changed windows get written.

FOREACHBATCH:
  Spark's built-in Postgres sink doesn't exist. We use foreachBatch
  to get each micro-batch as a regular DataFrame and write it ourselves.
  This gives us full control — batch upserts, custom error handling.
"""
import json
from typing import TYPE_CHECKING

import psycopg
from psycopg.rows import dict_row
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from producers.core.config import get_kafka_settings, get_postgres_settings
from producers.core.logging_setup import get_logger
from streaming.core.spark_session import get_spark_session

log = get_logger(__name__)


# ============================================================
# SCHEMAS
# ============================================================
# We define explicit schemas for parsing the JSON from Kafka.
# Why not inferSchema? Because:
#   1. inferSchema samples data and can be wrong or slow
#   2. Explicit schemas fail fast on unexpected data
#   3. Required for streaming — can't scan all data first

VIEW_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("event_timestamp", TimestampType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("session_id", StringType(), nullable=False),
    StructField("device_type", StringType(), nullable=False),
    StructField("video_id", StringType(), nullable=False),
    StructField("video_title", StringType(), nullable=False),
    StructField("video_duration_seconds", StringType(), nullable=True),
    StructField("referrer", StringType(), nullable=True),
])

PROGRESS_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("event_timestamp", TimestampType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("session_id", StringType(), nullable=False),
    StructField("device_type", StringType(), nullable=False),
    StructField("video_id", StringType(), nullable=False),
    StructField("is_buffering", BooleanType(), nullable=False),
    StructField("quality", StringType(), nullable=True),
])


# ============================================================
# KAFKA READER
# ============================================================

def read_kafka_stream(spark: SparkSession, topic: str) -> DataFrame:
    """
    Read a Kafka topic as a Structured Streaming DataFrame.

    Kafka delivers each message as a row with these columns:
      key       (binary) — our user_id
      value     (binary) — our JSON event payload
      topic     (string)
      partition (int)
      offset    (long)
      timestamp (timestamp) — Kafka broker timestamp

    We only need key and value. The rest we grab when needed.
    """
    kafka_settings = get_kafka_settings()

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_settings.bootstrap_servers)
        .option("subscribe", topic)

        # "earliest" = read from the beginning of the topic.
        # For production restarts you'd use "latest" after initial load.
        .option("startingOffsets", "earliest")

        # Limit how many messages per micro-batch to prevent OOM
        # on large backlogs. 1000 per partition per trigger.
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )


def parse_json(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Parse the binary Kafka value column into structured columns.

    Kafka value arrives as binary bytes.
    Step 1: cast bytes → string (UTF-8 JSON)
    Step 2: from_json string → struct using our schema
    Step 3: select struct fields as top-level columns

    from_json returns null for malformed JSON — it doesn't crash.
    We filter nulls out afterwards.
    """
    return (
        df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                schema,
            ).alias("data")
        )
        .select("data.*")
        .filter(F.col("event_id").isNotNull())  # Drop malformed events
    )


# ============================================================
# AGGREGATION JOBS
# ============================================================

def build_concurrent_viewers_stream(spark: SparkSession) -> DataFrame:
    """
    Aggregation 1: Concurrent viewers per video.

    Uses watch_progress events — if a user is sending progress events
    for a video, they're actively watching it.

    Window: 30 seconds tumbling
    Key: video_id
    Metric: count distinct session_ids
    """
    kafka_settings = get_kafka_settings()

    raw = read_kafka_stream(spark, kafka_settings.topic_watch_events)
    events = parse_json(raw, PROGRESS_EVENT_SCHEMA)

    return (
        events
        # Watermark — tolerate events arriving up to 1 minute late
        .withWatermark("event_timestamp", "1 minute")
        .groupBy(
            # Tumbling window of 30 seconds
            F.window("event_timestamp", "30 seconds"),
            F.col("video_id"),
        )
        .agg(
            # Count unique sessions — each session = one viewer
            F.approx_count_distinct("session_id").alias("concurrent_viewers")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("video_id"),
            F.col("concurrent_viewers"),
        )
    )


def build_buffering_rate_stream(spark: SparkSession) -> DataFrame:
    """
    Aggregation 2: Buffering rate by device type.

    Window: 1 minute tumbling
    Key: device_type
    Metric: (buffering_events / total_events) * 100
    """
    kafka_settings = get_kafka_settings()

    raw = read_kafka_stream(spark, kafka_settings.topic_watch_events)
    events = parse_json(raw, PROGRESS_EVENT_SCHEMA)

    return (
        events
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(
            F.window("event_timestamp", "1 minute"),
            F.col("device_type"),
        )
        .agg(
            F.count("*").alias("total_events"),
            # sum(is_buffering) counts True values (True = 1, False = 0)
            F.sum(F.col("is_buffering").cast("int")).alias("buffering_events"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("device_type"),
            F.col("total_events"),
            F.col("buffering_events"),
            # Calculate percentage — round to 2 decimal places
            F.round(
                (F.col("buffering_events") / F.col("total_events")) * 100, 2
            ).alias("buffering_rate_pct"),
        )
    )


def build_top_videos_stream(spark: SparkSession) -> DataFrame:
    """
    Aggregation 3: Top videos by view count.

    Window: 5 minutes tumbling
    Key: video_id + video_title
    Metric: count of VideoViewEvents (not progress — actual clicks)
    """
    kafka_settings = get_kafka_settings()

    raw = read_kafka_stream(spark, kafka_settings.topic_view_events)
    events = parse_json(raw, VIEW_EVENT_SCHEMA)

    return (
        events
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(
            F.window("event_timestamp", "5 minutes"),
            F.col("video_id"),
            F.col("video_title"),
        )
        .agg(
            F.count("*").alias("view_count")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("video_id"),
            F.col("video_title"),
            F.col("view_count"),
        )
    )


# ============================================================
# POSTGRES WRITER (foreachBatch)
# ============================================================

def make_postgres_writer(table: str, primary_keys: list[str]):
    """
    Factory that creates a foreachBatch writer function for a given table.

    foreachBatch receives:
      - batch_df: a regular (non-streaming) DataFrame for this micro-batch
      - batch_id: monotonically increasing batch identifier

    We use INSERT ... ON CONFLICT DO UPDATE (upsert) because:
    Windows can be updated — late events can change aggregation results.
    We don't want duplicate rows — we want the latest value.
    """
    pg_settings = get_postgres_settings()

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        # Collect to driver — safe for aggregated data (small rows)
        # Never do this for raw event data (millions of rows)
        rows = batch_df.collect()

        conflict_cols = ", ".join(primary_keys)
        all_cols = batch_df.columns
        non_pk_cols = [c for c in all_cols if c not in primary_keys]

        # Build the upsert query dynamically from column names
        col_names = ", ".join(all_cols)
        placeholders = ", ".join([f"%({c})s" for c in all_cols])
        updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in non_pk_cols])

        query = f"""
            INSERT INTO {table} ({col_names})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_cols}) DO UPDATE SET
            {updates},
            written_at = NOW()
        """

        with psycopg.connect(pg_settings.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.executemany(query, [dict(zip(all_cols, row)) for row in rows])
            conn.commit()

        log.info(
            "streaming_batch_written",
            table=table,
            batch_id=batch_id,
            rows=len(rows),
        )

    return write_batch


# ============================================================
# JOB RUNNER
# ============================================================

class WatchAggregatorJob:
    """
    Orchestrates all three streaming aggregations.

    Each aggregation runs as a separate streaming query.
    Spark manages them concurrently in background threads.
    awaitAnyTermination() blocks until one of them stops
    (error or manual stop).
    """

    def __init__(self) -> None:
        self.spark = get_spark_session(app_name="StreamCore-WatchAggregator")

    def start(self) -> None:
        log.info("watch_aggregator_starting")

        # Build all three streaming DataFrames
        concurrent_viewers_df = build_concurrent_viewers_stream(self.spark)
        buffering_rate_df = build_buffering_rate_stream(self.spark)
        top_videos_df = build_top_videos_stream(self.spark)

        # Start query 1 — Concurrent Viewers
        q1 = (
            concurrent_viewers_df.writeStream
            .outputMode("update")
            .foreachBatch(make_postgres_writer(
                table="streamcore_aggregated.concurrent_viewers",
                primary_keys=["window_start", "video_id"],
            ))
            .trigger(processingTime="10 seconds")  # Run every 10 seconds
            .queryName("concurrent_viewers")
            .start()
        )

        # Start query 2 — Buffering Rate
        q2 = (
            buffering_rate_df.writeStream
            .outputMode("update")
            .foreachBatch(make_postgres_writer(
                table="streamcore_aggregated.buffering_rate",
                primary_keys=["window_start", "device_type"],
            ))
            .trigger(processingTime="10 seconds")
            .queryName("buffering_rate")
            .start()
        )

        # Start query 3 — Top Videos
        q3 = (
            top_videos_df.writeStream
            .outputMode("update")
            .foreachBatch(make_postgres_writer(
                table="streamcore_aggregated.top_videos",
                primary_keys=["window_start", "video_id"],
            ))
            .trigger(processingTime="10 seconds")
            .queryName("top_videos")
            .start()
        )

        log.info(
            "all_streaming_queries_started",
            queries=["concurrent_viewers", "buffering_rate", "top_videos"],
        )

        # Block until a query fails or is stopped
        self.spark.streams.awaitAnyTermination()
