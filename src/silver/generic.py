import polars as pl
import logging
from datetime import datetime, timedelta
from utils.storage import read_delta_safe, write_delta_safe
from utils.audit import log_execution, should_retry_execution, get_last_execution_status, get_last_successful_date
from utils.monitoring import MetricsCollector
from utils.data_quality import DataQualityRunner, get_default_dim_checks
from silver.config import SILVER_DIM_TABLES

logger = logging.getLogger(__name__)


def process_dim_to_silver(table_name: str, incremental: bool = True, full_refresh: bool = False):
    """
    Generic function to process a dimension table from Bronze to Silver
    based on its configuration in silver/config.py.

    This function performs standard cleaning:
    1. Checks for failed previous executions and retries if needed
    2. Reads from the configured bronze source (incremental if enabled)
    3. Selects and casts columns to the defined schema
    4. Drops rows with nulls in critical key columns
    5. Deduplicates records to get the latest version (SCD Type 1)
    6. Runs data quality checks
    7. Writes the result to the silver layer (merge mode for incremental)
    8. Logs metrics for monitoring

    Args:
        table_name: Name of the table to process
        incremental: If True, only process new data since last successful run
        full_refresh: If True, ignore incremental and process all data
    """
    # Check if previous execution failed
    if should_retry_execution(table_name, "bronze_to_silver"):
        last_exec = get_last_execution_status(table_name, "bronze_to_silver")
        logger.warning(f"Retrying failed execution for {table_name}: {last_exec['message']}")

    mode = "INCREMENTAL" if (incremental and not full_refresh) else "FULL REFRESH"

    if table_name not in SILVER_DIM_TABLES:
        raise ValueError(f"Configuration for silver table '{table_name}' not found in SILVER_DIM_TABLES.")

    config = SILVER_DIM_TABLES[table_name]

    try:
        with MetricsCollector(table_name, "silver") as metrics:
            # Read from Bronze source
            df = read_delta_safe(config["source_table"])
            source_rows = len(df)
            metrics.add_dataframe_stats(df, "source")

            # Cast columns to schema
            select_exprs = [pl.col(name).cast(dtype) for name, dtype in config["columns"].items()]
            df = df.select(select_exprs)
            metrics.add("columns_casted", len(config['columns']))

            # 2b. Apply incremental filter AFTER casting (so datetime columns are properly typed)
            incremental_applied = False
            if incremental and not full_refresh:
                last_success_date = get_last_successful_date(table_name, "bronze_to_silver")
                if last_success_date:
                    # Get date column from config (or fallback to auto-detect)
                    date_col = config.get("date_column")

                    # Fallback: auto-detect if not specified in config
                    if not date_col:
                        for col_name in ["updated_at", "created_at", "ingestion_time", "processed_at"]:
                            if col_name in df.columns:
                                date_col = col_name
                                break

                    if date_col:
                        try:
                            # Filter for records updated/created after last successful run
                            # Add 1 day buffer to catch any late-arriving data
                            cutoff_date = datetime.strptime(last_success_date, "%Y-%m-%d") - timedelta(days=1)
                            before_count = len(df)

                            # Handle different column types gracefully
                            col_dtype = df[date_col].dtype

                            if col_dtype == pl.Date:
                                # Already Date type, compare directly
                                df = df.filter(pl.col(date_col) > cutoff_date.date())
                            elif col_dtype == pl.Datetime:
                                # Datetime type, cast to Date for comparison
                                df = df.filter(pl.col(date_col).cast(pl.Date) > cutoff_date.date())
                            elif col_dtype == pl.Utf8:
                                # String type, parse datetime first
                                df = df.filter(
                                    pl.col(date_col).str.to_datetime().cast(pl.Date) > cutoff_date.date()
                                )
                            else:
                                # Unknown type, try generic cast
                                logger.warning(f"  ⚠️  Unexpected type {col_dtype} for {date_col}, attempting cast")
                                df = df.filter(pl.col(date_col).cast(pl.Date) > cutoff_date.date())

                            after_count = len(df)
                            filtered_count = before_count - after_count

                            # Check if incremental filter resulted in 0 rows (no new data)
                            if after_count == 0:
                                logger.info(f"No new data since {last_success_date}, skipping processing")
                                log_execution(
                                    table_name=table_name,
                                    layer_type="bronze_to_silver",
                                    status="success",
                                    message=f"No new data to process (last run: {last_success_date})"
                                )
                                return
                            else:
                                logger.info(f"Incremental: {before_count:,} → {after_count:,} rows (after {last_success_date})")
                                metrics.add("incremental_filtered", filtered_count)
                                metrics.add("incremental_mode", True)
                                metrics.add("incremental_cutoff_date", str(cutoff_date.date()))
                                incremental_applied = True

                        except Exception as e:
                            # Incremental filter failed, fallback to full refresh
                            logger.warning(f"Incremental filter failed: {str(e)}, using full refresh")
                            # Re-read data to get all records
                            df = read_delta_safe(config["source_table"])
                            select_exprs = [pl.col(name).cast(dtype) for name, dtype in config["columns"].items()]
                            df = df.select(select_exprs)
                            metrics.add("incremental_mode", False)
                            metrics.add("incremental_fallback", True)
                    else:
                        logger.warning(f"No date column found (config missing 'date_column'), processing all data")
                        metrics.add("incremental_mode", False)
                        metrics.add("no_date_column", True)
                else:
                    metrics.add("incremental_mode", False)
                    metrics.add("first_run", True)

            if not incremental_applied:
                metrics.add("incremental_mode", False)

            # Drop nulls
            if config.get("not_null_cols"):
                before_count = len(df)
                df = df.drop_nulls(subset=config["not_null_cols"])
                nulls_dropped = before_count - len(df)
                if nulls_dropped > 0:
                    logger.warning(f"Dropped {nulls_dropped:,} rows with nulls")
                    metrics.add("nulls_dropped", nulls_dropped)

            # Deduplicate
            if config.get("dedup_keys") and config.get("dedup_sort_col"):
                before_count = len(df)
                df = df.sort(config["dedup_sort_col"], descending=True).unique(
                    subset=config["dedup_keys"],
                    keep="first"
                )
                duplicates_removed = before_count - len(df)
                if duplicates_removed > 0:
                    logger.info(f"Removed {duplicates_removed:,} duplicates")
                    metrics.add("duplicates_removed", duplicates_removed)

            # Data Quality Checks
            dq_checks = get_default_dim_checks(config)
            dq_runner = DataQualityRunner(table_name, dq_checks)
            if not dq_runner.run(df):
                raise ValueError(f"Data quality checks failed for {table_name}")

            # Write to Silver
            target_path = f"s3://datalake/silver/{table_name}"

            if incremental and not full_refresh and config.get("dedup_keys"):
                from utils.storage import upsert_delta_safe
                upsert_delta_safe(df, target_path, primary_keys=config["dedup_keys"])
                write_mode = "upsert"
            else:
                write_delta_safe(df, target_path, mode="overwrite")
                write_mode = "overwrite"

            metrics.add("write_mode", write_mode)
            metrics.add_dataframe_stats(df, "output")
            metrics.add("records_processed", source_rows)
            metrics.add("records_output", len(df))

            log_execution(
                table_name=table_name,
                layer_type="bronze_to_silver",
                status="success",
                message=f"Processed {source_rows} → {len(df)} rows ({mode})"
            )

    except Exception as e:
        error_msg = f"Failed to process {table_name}: {str(e)}"
        logger.error(error_msg)
        log_execution(
            table_name=table_name,
            layer_type="bronze_to_silver",
            status="failed",
            message=error_msg[:500]
        )
        raise