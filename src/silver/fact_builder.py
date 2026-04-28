import polars as pl
import logging
from datetime import date, datetime, timedelta
from typing import Optional
from utils.storage import read_delta_safe, write_delta_safe, upsert_delta_safe
from utils.audit import log_execution, should_retry_execution, get_last_execution_status, get_last_successful_date
from utils.monitoring import MetricsCollector
from utils.data_quality import DataQualityRunner, get_default_fact_checks
from silver.fact_config import FactTableConfig

logger = logging.getLogger(__name__)


def _filter_by_date(df: pl.DataFrame, date_col: str, cutoff_date: date) -> pl.DataFrame:
    """
    Helper function to filter DataFrame by date column.
    Handles different column types (Date, Datetime, String).

    Args:
        df: DataFrame to filter
        date_col: Name of date column
        cutoff_date: Cutoff date (keep rows > this date)

    Returns:
        Filtered DataFrame
    """
    if date_col not in df.columns:
        return df

    col_dtype = df[date_col].dtype

    if col_dtype == pl.Date:
        return df.filter(pl.col(date_col) > cutoff_date)
    elif col_dtype == pl.Datetime:
        return df.filter(pl.col(date_col).cast(pl.Date) > cutoff_date)
    elif col_dtype == pl.Utf8:
        # Handle string with potential time component
        return df.filter(pl.col(date_col).str.to_datetime().cast(pl.Date) > cutoff_date)
    else:
        logger.warning(f"Unexpected type {col_dtype} for {date_col}, attempting cast")
        return df.filter(pl.col(date_col).cast(pl.Date) > cutoff_date)


def build_fact_table_generic(config: FactTableConfig, snapshot_date: Optional[str] = None, incremental: bool = False):
    """
    Generic fact table builder that works from configuration.
    Supports both detail fact tables (joins) and aggregate fact tables (group by).

    Args:
        config: FactTableConfig defining how to build the fact table
        snapshot_date: Optional snapshot date (defaults to today)
        incremental: If True, append/upsert instead of overwrite
    """
    if snapshot_date is None:
        snapshot_date = str(date.today())

    # Check if previous execution failed
    if should_retry_execution(config.table_name, "silver_fact"):
        last_exec = get_last_execution_status(config.table_name, "silver_fact")
        logger.warning(f"Retrying failed execution for {config.table_name}: {last_exec['message']}")

    try:
        with MetricsCollector(config.table_name, "silver") as metrics:
            # Step 1: Load primary table
            df = read_delta_safe(config.primary_table)
            metrics.add_dataframe_stats(df, "source")

            # Apply incremental filter if enabled and date_column configured
            incremental_applied = False
            cutoff_date_obj = None
            if incremental and config.date_column:
                last_success_date = get_last_successful_date(config.table_name, "silver_fact")
                if last_success_date:
                    date_col = config.date_column
                    if date_col and date_col in df.columns:
                        try:
                            cutoff_date = datetime.strptime(last_success_date, "%Y-%m-%d") - timedelta(days=1)
                            cutoff_date_obj = cutoff_date.date()
                            before_count = len(df)

                            # Use helper function for filtering
                            df = _filter_by_date(df, date_col, cutoff_date_obj)
                            after_count = len(df)

                            # Check if incremental filter resulted in 0 rows
                            if after_count == 0:
                                logger.info(f"No new data since {last_success_date}, skipping processing")
                                log_execution(
                                    table_name=config.table_name,
                                    layer_type="silver_fact",
                                    status="success",
                                    message=f"No new data to process (last run: {last_success_date})"
                                )
                                return
                            else:
                                logger.info(f"Incremental: {before_count:,} → {after_count:,} rows (after {last_success_date})")
                                metrics.add("incremental_filtered", before_count - after_count)
                                incremental_applied = True

                        except Exception as e:
                            logger.warning(f"Incremental filter failed: {str(e)}, using full refresh")
                            df = read_delta_safe(config.primary_table)
                            cutoff_date_obj = None  # Reset cutoff for join tables
                            metrics.add("incremental_fallback", True)

            if not incremental_applied:
                metrics.add("incremental_mode", False)

            # Step 2: Process joins
            if config.joins:
                for idx, join_spec in enumerate(config.joins, 1):
                    join_df = read_delta_safe(join_spec.source_table)
                    join_before_count = len(join_df)

                    # Apply incremental filter to join table if cutoff_date is set
                    # Try common date columns: attendance_date, assessment_date, created_at, updated_at
                    if cutoff_date_obj is not None and incremental_applied:
                        date_cols_to_try = ['attendance_date', 'assessment_date', 'created_at', 'updated_at', 'date']
                        filtered = False
                        for date_col in date_cols_to_try:
                            if date_col in join_df.columns:
                                join_df = _filter_by_date(join_df, date_col, cutoff_date_obj)
                                join_after_count = len(join_df)
                                if join_after_count < join_before_count:
                                    logger.info(f"Join table {idx} incremental: {join_before_count:,} → {join_after_count:,} rows")
                                    metrics.add(f"join_{idx}_incremental_filtered", join_before_count - join_after_count)
                                    filtered = True
                                break
                        if not filtered:
                            metrics.add(f"join_{idx}_no_date_column", True)

                    # Apply pre-aggregation if specified
                    if join_spec.pre_aggregate:
                        agg_exprs = [agg.to_polars_expr() for agg in join_spec.pre_aggregate]
                        join_df = join_df.group_by(join_spec.join_on).agg(agg_exprs)

                    # Apply column selection if specified
                    if join_spec.select_cols:
                        cols_to_select = list(set(join_spec.join_on + join_spec.select_cols))
                        join_df = join_df.select(cols_to_select)

                    # Apply rename if specified
                    if join_spec.rename_map:
                        join_df = join_df.rename(join_spec.rename_map)

                    # Perform join
                    df = df.join(join_df, on=join_spec.join_on, how=join_spec.join_type)

                    metrics.add(f"join_{idx}_rows", len(df))

            # Step 3: Apply filters
            if config.filters:
                for idx, filter_fn in enumerate(config.filters, 1):
                    before_count = len(df)
                    df = filter_fn(df)
                    after_count = len(df)
                    metrics.add(f"filter_{idx}_removed", before_count - after_count)

            # Step 4: Apply aggregations
            if config.group_by and config.aggregations:
                agg_exprs = [agg.to_polars_expr() for agg in config.aggregations]
                df = df.group_by(config.group_by).agg(agg_exprs)
                metrics.add("aggregated_rows", len(df))

            # Step 5: Post-processing
            if config.post_process:
                df = config.post_process(df)

            # Add snapshot date
            df = df.with_columns(pl.lit(snapshot_date).alias("snapshot_date"))

            # Data quality checks
            dq_checks = get_default_fact_checks(config.primary_keys)
            dq_runner = DataQualityRunner(config.table_name, dq_checks)
            if not dq_runner.run(df):
                raise ValueError(f"Data quality checks failed for {config.table_name}")

            metrics.add_dataframe_stats(df, "output")

            # Write to silver
            target_path = f"s3://datalake/silver/{config.table_name}"

            # Determine write mode
            if config.mode == "upsert" or incremental:
                # Upsert mode: merge based on primary keys + snapshot_date
                upsert_keys = config.primary_keys + ["snapshot_date"]
                upsert_delta_safe(df, target_path, primary_keys=upsert_keys, partition_by=config.partition_by)
                metrics.add("write_mode", "upsert")
            else:
                # Other modes: overwrite, append
                write_delta_safe(df, target_path, mode=config.mode, partition_by=config.partition_by)
                metrics.add("write_mode", config.mode)

            log_execution(
                table_name=config.table_name,
                layer_type="silver_fact",
                status="success",
                message=f"Built {len(df)} rows"
            )

    except Exception as e:
        error_msg = f"Failed to build {config.table_name}: {str(e)}"
        logger.error(error_msg)
        log_execution(
            table_name=config.table_name,
            layer_type="silver_fact",
            status="failed",
            message=error_msg[:500]
        )
        raise


# Convenience functions for backward compatibility
def build_fact_student_performance(snapshot_date: Optional[str] = None):
    """Build fact_student_performance table"""
    from silver.fact_config import SILVER_FACT_TABLES
    config = SILVER_FACT_TABLES["fact_student_performance"]
    build_fact_table_generic(config, snapshot_date)


def build_fact_class_summary(snapshot_date: Optional[str] = None):
    """Build fact_class_summary table"""
    from silver.fact_config import SILVER_FACT_TABLES
    config = SILVER_FACT_TABLES["fact_class_summary"]
    build_fact_table_generic(config, snapshot_date)


def build_fact_daily_attendance(snapshot_date: Optional[str] = None):
    """Build fact_daily_attendance — daily grain attendance aggregated per class x date."""
    from silver.fact_config import SILVER_FACT_TABLES
    config = SILVER_FACT_TABLES["fact_daily_attendance"]
    build_fact_table_generic(config, snapshot_date)


def build_fact_daily_assessment(snapshot_date: Optional[str] = None):
    """Build fact_daily_assessment — daily grain assessment aggregated per class x date."""
    from silver.fact_config import SILVER_FACT_TABLES
    config = SILVER_FACT_TABLES["fact_daily_assessment"]
    build_fact_table_generic(config, snapshot_date)
