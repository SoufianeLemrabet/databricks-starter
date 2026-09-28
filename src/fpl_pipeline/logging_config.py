# src/fpl_pipeline/logging_config.py
import logging
import sys
import functools
import time
from typing import Any
import structlog


def configure_logging(json_logs: bool = False) -> None:
    """
    Configure structlog pour le projet.
    json_logs=True pour un format JSON (utile en prod/Databricks Job),
    False pour un format lisible en console (dev local).
    """
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.INFO,
    )

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    if json_logs:
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=shared_processors + [renderer],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    return structlog.get_logger(module=name)


def log_step(event: str, **fields: Any):
    """Logue started / completed / failed (avec durée) autour d'une fonction."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            log = get_logger(func.__module__)
            log.info(f"{event}_started", **fields)
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                log.error(
                    f"{event}_failed",
                    duration_s=round(time.perf_counter() - start, 2),
                    error_type=type(e).__name__,
                    error=str(e),
                    **fields,
                )
                raise  # on ne masque jamais l'erreur, on la logue et on la relance
            log.info(
                f"{event}_completed",
                duration_s=round(time.perf_counter() - start, 2),
                **fields,
            )
            return result
        return wrapper
    return decorator