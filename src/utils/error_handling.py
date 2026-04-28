"""
Centralized error handling utilities for robust pipeline execution.

This module provides error handling wrappers and utilities to make the pipeline
resilient to unexpected errors with proper fallback behavior.
"""

import logging
import functools
from typing import Callable, Any, Optional

logger = logging.getLogger(__name__)


def with_fallback(
    fallback_value: Any = None,
    fallback_fn: Optional[Callable] = None,
    log_error: bool = True,
    raise_after_fallback: bool = False
):
    """
    Decorator to wrap a function with fallback behavior on exception.

    Args:
        fallback_value: Value to return if function fails (if fallback_fn not provided)
        fallback_fn: Function to call if main function fails
        log_error: Whether to log the error
        raise_after_fallback: Whether to re-raise exception after fallback

    Example:
        @with_fallback(fallback_value=None)
        def risky_operation():
            return do_something_risky()

        @with_fallback(fallback_fn=lambda: safe_alternative())
        def another_risky_op():
            return do_something_else()
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_error:
                    logger.error(f"Error in {func.__name__}: {str(e)}")
                    logger.error(f"  Args: {args}")
                    logger.error(f"  Kwargs: {kwargs}")

                # Execute fallback
                if fallback_fn:
                    logger.warning(f"Executing fallback function for {func.__name__}")
                    result = fallback_fn()
                else:
                    logger.warning(f"Returning fallback value for {func.__name__}: {fallback_value}")
                    result = fallback_value

                # Re-raise if requested
                if raise_after_fallback:
                    raise

                return result
        return wrapper
    return decorator


def retry_on_failure(
    max_retries: int = 3,
    retry_delay_seconds: int = 1,
    backoff_multiplier: float = 2.0,
    retry_on: tuple = (Exception,)
):
    """
    Decorator to retry a function on failure with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        retry_delay_seconds: Initial delay between retries
        backoff_multiplier: Multiplier for delay on each retry
        retry_on: Tuple of exception types to retry on

    Example:
        @retry_on_failure(max_retries=3, retry_delay_seconds=2)
        def flaky_network_call():
            return fetch_data_from_api()
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            import time

            delay = retry_delay_seconds
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_on as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(f"Attempt {attempt + 1}/{max_retries + 1} failed for {func.__name__}: {str(e)}")
                        logger.warning(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                        delay *= backoff_multiplier
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed for {func.__name__}")
                        raise last_exception

            # Should never reach here, but just in case
            if last_exception:
                raise last_exception
        return wrapper
    return decorator


def safe_cast_date(df_column, cutoff_date):
    """
    Safely cast a column to Date type and compare with cutoff date.
    Handles various data types gracefully.

    Args:
        df_column: Polars column/expression
        cutoff_date: datetime.date object for comparison

    Returns:
        Polars expression for filtering

    Example:
        df = df.filter(safe_cast_date(pl.col("updated_at"), cutoff_date))
    """
    import polars as pl

    # Get column dtype if possible
    try:
        col_dtype = df_column.dtype
    except AttributeError:
        # If dtype not available, try generic cast
        return df_column.cast(pl.Date) > cutoff_date

    # Handle different types
    if col_dtype == pl.Date:
        # Already Date type
        return df_column > cutoff_date
    elif col_dtype == pl.Datetime:
        # Datetime type, cast to Date
        return df_column.cast(pl.Date) > cutoff_date
    elif col_dtype == pl.Utf8:
        # String type, parse as datetime first
        return df_column.str.to_datetime().cast(pl.Date) > cutoff_date
    else:
        # Unknown type, try generic cast
        logger.warning(f"Unexpected date column type: {col_dtype}, attempting generic cast")
        return df_column.cast(pl.Date) > cutoff_date


def validate_dataframe(
    df,
    min_rows: int = 0,
    required_columns: Optional[list] = None,
    name: str = "DataFrame"
):
    """
    Validate a DataFrame meets basic requirements.

    Args:
        df: Polars DataFrame to validate
        min_rows: Minimum number of rows required
        required_columns: List of column names that must be present
        name: Name for logging purposes

    Raises:
        ValueError: If validation fails

    Example:
        validate_dataframe(
            df,
            min_rows=1,
            required_columns=["student_id", "class_id"],
            name="students"
        )
    """
    # Check row count
    if len(df) < min_rows:
        raise ValueError(f"{name} has {len(df)} rows, expected at least {min_rows}")

    # Check required columns
    if required_columns:
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"{name} missing required columns: {missing_cols}")

    logger.info(f"✓ {name} validated: {len(df):,} rows, {len(df.columns)} columns")


class ErrorContext:
    """
    Context manager for better error reporting.

    Example:
        with ErrorContext("Processing students table"):
            df = process_students()

        # On error, logs:
        # ❌ Error in: Processing students table
        # Details: <exception message>
    """

    def __init__(self, context: str):
        self.context = context

    def __enter__(self):
        logger.debug(f"→ {self.context}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logger.error(f"❌ Error in: {self.context}")
            logger.error(f"   Details: {exc_val}")
        return False  # Don't suppress exception
