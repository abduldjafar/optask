"""
Centralized logging configuration for the data pipeline.

This module provides consistent logging setup across all pipeline components.
"""

import logging
import sys
from typing import Optional


def setup_logging(
    level: str = "INFO",
    log_format: Optional[str] = None,
    include_timestamp: bool = True
) -> None:
    """
    Configure logging for the pipeline.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Custom format string (optional)
        include_timestamp: Whether to include timestamp in logs
    """
    if log_format is None:
        if include_timestamp:
            log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        else:
            log_format = "%(name)s - %(levelname)s - %(message)s"

    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Set polars logging to WARNING to reduce noise
    logging.getLogger("polars").setLevel(logging.WARNING)

    # Set deltalake logging to WARNING
    logging.getLogger("deltalake").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name.

    Args:
        name: Logger name (typically __name__ of the module)

    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


# Simple format for Airflow (since Airflow already adds timestamps)
AIRFLOW_FORMAT = "%(name)s - %(levelname)s - %(message)s"

# Detailed format for debugging
DEBUG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"

# Production format with all details
PRODUCTION_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
