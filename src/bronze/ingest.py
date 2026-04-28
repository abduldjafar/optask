import polars as pl
import logging
from datetime import datetime
from utils.storage import write_delta_safe, upsert_delta_safe, read_parquet_safe
from utils.audit import log_execution, should_retry_execution, get_last_execution_status, get_next_s3_file_for_bronze
from bronze.config import BRONZE_TABLES

logger = logging.getLogger(__name__)


def ingest_file_to_bronze(table_name: str, source_path: str, mode: str = "upsert"):
    """
    Ingests raw Parquet files into the Bronze Delta Lake layer.

    Behavior:
    - If source_path is an S3 DIRECTORY (no glob): loops through ALL unprocessed
      files in one task run (backfill-safe). Same concept as raw ingestion.
    - If source_path contains a glob (*): legacy mode, reads all matching files at once.

    On first run (empty audit log), all existing files are backfilled.
    On subsequent runs, only files not yet successfully processed are ingested.
    """
    if table_name not in BRONZE_TABLES:
        raise ValueError(f"Table {table_name} not found in configuration.")

    config = BRONZE_TABLES[table_name]
    file_type = config["file_type"]
    expected_columns = config["columns"]
    primary_keys = config["primary_key"]

    # Check if previous execution failed
    if should_retry_execution(table_name, "raw_to_bronze"):
        last_exec = get_last_execution_status(table_name, "raw_to_bronze")
        logger.warning(f"Retrying previous failed execution for {table_name}: {last_exec['message']}")

    is_directory_mode = "*" not in source_path

    if not is_directory_mode:
        # Legacy glob mode — reads all matching files at once (single pass)
        _ingest_single_path(table_name, source_path, source_path, "-",
                            file_type, expected_columns, primary_keys, mode)
        return

    # --- Directory mode: loop through ALL unprocessed files ---
    files_processed = 0
    logger.info(f"Bronze ingestion for {table_name} — scanning {source_path} for unprocessed files...")

    while True:
        next_file, next_path = get_next_s3_file_for_bronze(table_name, source_path)

        if next_file is None:
            if files_processed == 0:
                logger.info(f"No new raw files to ingest for {table_name}. Already up to date.")
            else:
                logger.info(f"Finished: {files_processed} file(s) ingested to Bronze for {table_name}.")
            return

        logger.info(f"[{files_processed + 1}] Bronze ← {next_file}")
        _ingest_single_path(table_name, next_path, next_path, next_file,
                            file_type, expected_columns, primary_keys, mode)
        files_processed += 1


def _ingest_single_path(table_name: str, source_path: str, effective_path: str,
                        file_name: str, file_type: str, expected_columns: list,
                        primary_keys: list, mode: str):
    """Internal helper: reads one Parquet file/glob, validates schema, upserts to Bronze."""
    try:
        if file_type != "parquet":
            raise ValueError("Bronze layer must strictly read from Raw Parquet data!")

        df = read_parquet_safe(effective_path)

        # Inject ingestion_date from filename (e.g., "2026-04-29.parquet" → date 2026-04-29)
        # This allows Silver to reliably detect new records even when domain
        # date columns (updated_at, created_at) are stale or historical.
        if file_name and file_name != "-":
            try:
                date_str = file_name.split(".")[0]  # "2026-04-29"
                ingestion_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                df = df.with_columns(
                    pl.lit(str(ingestion_date)).str.to_date().alias("ingestion_date")
                )
            except ValueError:
                pass  # filename not a date pattern, skip ingestion_date

        # Strict Schema Validation
        if expected_columns != ["*"]:
            missing_cols = [col for col in expected_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Schema Validation Failed: Missing columns {missing_cols} in {table_name}")
            df = df.select(expected_columns)

        path = f"s3://datalake/bronze/{table_name}"

        if mode == "upsert":
            upsert_delta_safe(df=df, path=path, primary_keys=primary_keys)
        else:
            write_delta_safe(df=df, path=path, mode=mode)

        log_execution(table_name=table_name, layer_type="raw_to_bronze", status="success",
                      file_name=file_name, source_path=effective_path)

    except Exception as e:
        log_execution(table_name=table_name, layer_type="raw_to_bronze", status="failed",
                      file_name=file_name, source_path=effective_path, message=str(e))
        raise e
