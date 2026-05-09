"""
Structured logging setup for StreamCore.

Why structured logs?
--------------------
A traditional log line looks like:
    "2026-04-30 10:15:23 INFO Sent event to Kafka topic streamcore.video.views"

A structured log line looks like:
    {"timestamp": "2026-04-30T10:15:23Z", "level": "info",
     "event": "kafka_event_sent", "topic": "streamcore.video.views",
     "user_id": "u_4521", "latency_ms": 12}

The second one is queryable. You can ask "show me all events for user u_4521"
or "what's the p95 latency over the last hour?". This is how production
systems are operated.

Structlog is the modern standard for this in Python.
"""
import logging
import sys

import structlog

from producers.core.config import get_app_settings


def configure_logging() -> None:
    """
    Configure structured logging for the entire application.

    Call this ONCE at application startup. Every module after that
    can simply use `structlog.get_logger()` and it just works.
    """
    settings = get_app_settings()

    # Standard library logging — structlog wraps this
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=settings.log_level.upper(),
    )

    # Choose renderer based on environment:
    #   - Local development: pretty colored console output
    #   - Production: JSON for log aggregation (ELK, Datadog, etc.)
    if settings.app_env == "local":
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[
            # Add log level to each entry
            structlog.stdlib.add_log_level,
            # Add timestamp in ISO 8601 format
            structlog.processors.TimeStamper(fmt="iso"),
            # Capture stack info on exceptions
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # Final renderer — console or JSON
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.log_level.upper())
        ),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """
    Get a logger instance.

    Usage:
        log = get_logger(__name__)
        log.info("event_sent", topic="views", user_id="u_123")
    """
    return structlog.get_logger(name)
