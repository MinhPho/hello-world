"""Structured logging setup using structlog.

All pipeline code should use `get_logger(__name__)` to obtain a logger.
This gives consistent JSON output in deployed environments and
human-readable output locally.
"""

from __future__ import annotations

import os
import sys

import structlog


def configure_logging() -> None:
    """Configure structlog processors based on the deployment environment."""
    is_local = os.environ.get("DAGSTER_DEPLOYMENT", "local") == "local"

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if is_local:
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer()
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a structlog logger bound with the given module name."""
    return structlog.get_logger(name)
