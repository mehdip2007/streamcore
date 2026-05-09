"""
Entry point for the PySpark Structured Streaming job.

Run in a THIRD terminal (alongside producer and consumer):
    python -m scripts.run_streaming

First run downloads the Kafka-Spark connector JAR (~10MB).
Subsequent runs use the cached JAR.

You'll see Spark's startup logs (can't fully silence them),
then your structured logs when aggregations start writing.
"""
from producers.core.logging_setup import configure_logging, get_logger
from streaming.jobs.watch_aggregator import WatchAggregatorJob


def main() -> None:
    configure_logging()
    log = get_logger(__name__)

    log.info("streaming_job_starting")

    try:
        job = WatchAggregatorJob()
        job.start()
    except KeyboardInterrupt:
        log.info("streaming_job_stopped")


if __name__ == "__main__":
    main()
