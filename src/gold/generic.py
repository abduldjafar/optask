"""
Generic processor for Gold layer aggregations.
Config-driven - add new gold tables by editing gold/config.py.
"""
import polars as pl
import logging
from datetime import datetime, timedelta
from utils.storage import read_delta_safe, write_delta_safe, upsert_delta_safe
from utils.audit import log_execution, should_retry_execution, get_last_execution_status, get_last_successful_date
from utils.monitoring import MetricsCollector
from gold.config import GOLD_TABLES, GoldTableConfig

logger = logging.getLogger(__name__)


def _filter_by_date(df: pl.DataFrame, date_col: str, cutoff_date) -> pl.DataFrame:
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


def process_gold_table(table_name: str, incremental: bool = True, full_refresh: bool = False):
    """
    Generic function to build a gold aggregation table from config.

    Args:
        table_name: Name of the gold table (must be in GOLD_TABLES config)
        incremental: If True, only process new data since last successful run
        full_refresh: If True, ignore incremental and process all data
    """
    # Check if previous execution failed
    if should_retry_execution(table_name, "silver_to_gold"):
        last_exec = get_last_execution_status(table_name, "silver_to_gold")
        logger.warning(f"Retrying failed execution for {table_name}: {last_exec['message']}")

    mode = "INCREMENTAL" if (incremental and not full_refresh) else "FULL REFRESH"

    if table_name not in GOLD_TABLES:
        raise ValueError(f"Configuration for gold table '{table_name}' not found in GOLD_TABLES.")

    config = GOLD_TABLES[table_name]

    try:
        with MetricsCollector(table_name, "gold") as metrics:
            # Step 1: Load source tables
            source_dfs = {}
            for alias, path in config.source_tables.items():
                df = read_delta_safe(path)

                # Remove snapshot_date if it exists (added by fact_builder, not needed)
                if "snapshot_date" in df.columns:
                    df = df.drop("snapshot_date")

                source_dfs[alias] = df
                metrics.add(f"source_{alias}_rows", len(df))

            # Step 2: Apply incremental filter if enabled
            incremental_applied = False
            if incremental and not full_refresh and config.date_column:
                last_success_date = get_last_successful_date(table_name, "silver_to_gold")
                if last_success_date:
                    try:
                        cutoff_date = (datetime.strptime(last_success_date, "%Y-%m-%d") - timedelta(days=1)).date()

                        # Filter all source tables that have the date column
                        # Use helper function to handle different column types
                        filtered_any = False
                        for alias, df in source_dfs.items():
                            if config.date_column in df.columns:
                                before_count = len(df)
                                df_filtered = _filter_by_date(df, config.date_column, cutoff_date)
                                after_count = len(df_filtered)

                                source_dfs[alias] = df_filtered
                                metrics.add(f"incremental_{alias}_filtered", before_count - after_count)

                                if after_count > 0:
                                    filtered_any = True

                        # Check if all filtered tables are empty
                        if not filtered_any:
                            logger.info(f"No new data since {last_success_date}, skipping processing")
                            log_execution(
                                table_name=table_name,
                                layer_type="silver_to_gold",
                                status="success",
                                message=f"No new data to process (last run: {last_success_date})"
                            )
                            return

                        incremental_applied = True
                        logger.info(f"Incremental filter applied (after {last_success_date})")

                    except Exception as e:
                        logger.warning(f"Incremental filter failed: {str(e)}, using full refresh")
                        # Re-read all data
                        for alias, path in config.source_tables.items():
                            df = read_delta_safe(path)
                            if "snapshot_date" in df.columns:
                                df = df.drop("snapshot_date")
                            source_dfs[alias] = df
                        metrics.add("incremental_fallback", True)
                else:
                    logger.info("First run, processing all data")

            if not incremental_applied:
                metrics.add("incremental_mode", False)

            # Step 3: Build joins according to config
            result_df = None
            for join_config in config.join_specs:
                base_alias = join_config["base"]
                result_df = source_dfs[base_alias]

                for join_spec in join_config.get("joins", []):
                    join_table_alias = join_spec["table"]
                    join_df = source_dfs[join_table_alias]

                    # Select specific columns if specified
                    if "select" in join_spec:
                        join_df = join_df.select(join_spec["select"])

                    # Perform join
                    result_df = result_df.join(
                        join_df,
                        on=join_spec["on"],
                        how=join_spec.get("how", "left")
                    )

                    metrics.add(f"join_{join_table_alias}_rows", len(result_df))

            # Step 4: Apply transformations
            if config.transformations:
                transform_exprs = [expr.alias(col_name) for col_name, expr in config.transformations]
                result_df = result_df.with_columns(transform_exprs)
                metrics.add("transformations_applied", len(config.transformations))

            # Step 5: Select final columns
            if config.select_columns:
                result_df = result_df.select(config.select_columns)

            # Step 6: Write to gold layer
            target_path = f"s3://datalake/gold/{table_name}"

            if incremental and not full_refresh and config.primary_keys:
                upsert_delta_safe(result_df, target_path, primary_keys=config.primary_keys, partition_by=config.partition_by)
                write_mode = "upsert"
            else:
                write_delta_safe(result_df, target_path, mode="overwrite", partition_by=config.partition_by)
                write_mode = "overwrite"

            metrics.add("write_mode", write_mode)
            metrics.add_dataframe_stats(result_df, "output")

            log_execution(
                table_name=table_name,
                layer_type="silver_to_gold",
                status="success",
                message=f"{len(result_df)} rows ({mode})"
            )

            logger.info(f"Gold {table_name}: {len(result_df):,} rows written ({write_mode})")

    except Exception as e:
        error_msg = f"Failed to process {table_name}: {str(e)}"
        logger.error(error_msg)
        log_execution(
            table_name=table_name,
            layer_type="silver_to_gold",
            status="failed",
            message=error_msg[:500]
        )
        raise
