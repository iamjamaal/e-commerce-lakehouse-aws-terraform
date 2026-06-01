"""
Structured logging for the Lakehouse ETL pipeline.

Provides consistent log formatting across all ETL jobs. In production on
AWS Glue, these logs would be captured by CloudWatch. The structured format
(with dataset name, step, and row counts) makes it easy to set up CloudWatch
alarms and dashboards.
"""

import logging
from datetime import datetime


def get_logger(job_name: str) -> logging.Logger:
    """
    Create a logger with consistent formatting for an ETL job.

    Args:
        job_name: Name of the ETL job (e.g., 'etl_products').

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(job_name)
    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers if called multiple times
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def log_step(logger: logging.Logger, step: str, message: str, **kwargs) -> None:
    """
    Log a pipeline step with optional key-value context.

    Example output:
        2025-04-01 12:00:00 | etl_products | INFO | [VALIDATE] 1000 rows passed, 0 rejected

    Args:
        logger:  Logger instance.
        step:    Pipeline step name (e.g., 'READ', 'VALIDATE', 'MERGE', 'ARCHIVE').
        message: Human-readable log message.
        **kwargs: Additional context to append (e.g., row_count=1000).
    """
    context = " | ".join(f"{k}={v}" for k, v in kwargs.items())
    full_message = f"[{step}] {message}"
    if context:
        full_message += f" | {context}"
    logger.info(full_message)


def log_job_start(logger: logging.Logger, dataset: str) -> None:
    """Log the start of an ETL job run."""
    log_step(logger, "START", f"Beginning ETL for {dataset}", timestamp=datetime.now().isoformat())


def log_job_end(logger: logging.Logger, dataset: str, status: str = "SUCCESS") -> None:
    """Log the end of an ETL job run."""
    log_step(logger, "END", f"ETL for {dataset} completed: {status}", timestamp=datetime.now().isoformat())
