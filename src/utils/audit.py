import polars as pl
from datetime import datetime
import os
import uuid
from deltalake import DeltaTable
from utils.storage import STORAGE_OPTIONS, write_delta_safe, write_parquet_safe, read_parquet_safe

LOG_PATH = "s3://datalake/system/pipeline_logs"

def log_execution(table_name: str, layer_type: str, status: str, file_name: str = "-", source_path: str = "-", message: str = ""):
    """
    Logs the pipeline execution state to an individual Parquet file in S3/MinIO.
    Each call writes a unique file to avoid race conditions during parallel task execution.
    """
    latest_date = file_name.split('.')[0] if file_name != "-" else datetime.now().strftime("%Y-%m-%d")
        
    df = pl.DataFrame({
        "table_name": [table_name],
        "layer_type": [layer_type],
        "file_name": [file_name],
        "source_path": [source_path],
        "latest_date": [latest_date],
        "execution_time": [datetime.now()],
        "status": [status],
        "message": [message]
    }).with_columns(
        pl.col("execution_time").cast(pl.Datetime("us")),
        pl.col("latest_date").cast(pl.Utf8)
    )
    
    # Write to a unique file — no lock conflicts, no race conditions
    unique_id = uuid.uuid4().hex
    log_file_path = f"{LOG_PATH}/{table_name}_{layer_type}_{unique_id}.parquet"
    write_parquet_safe(df, log_file_path)


def get_last_execution_status(table_name: str, layer_type: str) -> dict:
    """
    Get the status of the last execution for a specific table and layer.

    Returns:
        dict with keys: status, latest_date, execution_time, message
        None if no previous execution found
    """
    try:
        logs_df = read_parquet_safe(f"{LOG_PATH}/*.parquet")

        if logs_df.is_empty():
            return None

        # Filter for this table and layer, get latest execution
        table_logs = logs_df.filter(
            (pl.col("table_name") == table_name) &
            (pl.col("layer_type") == layer_type)
        ).sort("execution_time", descending=True)

        if len(table_logs) == 0:
            return None

        latest = table_logs.row(0, named=True)
        return {
            "status": latest["status"],
            "latest_date": latest["latest_date"],
            "execution_time": latest["execution_time"],
            "message": latest.get("message", ""),
            "file_name": latest.get("file_name", "-")
        }
    except Exception:
        # No logs exist yet
        return None


def should_retry_execution(table_name: str, layer_type: str) -> bool:
    """
    Check if the last execution failed and should be retried.

    Returns:
        True if last execution failed, False otherwise
    """
    last_exec = get_last_execution_status(table_name, layer_type)
    if last_exec is None:
        return False

    return last_exec["status"] == "failed"


def get_last_successful_date(table_name: str, layer_type: str) -> str:
    """
    Get the latest date that was successfully processed.
    Used for incremental processing.

    Returns:
        Date string (YYYY-MM-DD) or None if no successful execution found
    """
    try:
        logs_df = read_parquet_safe(f"{LOG_PATH}/*.parquet")

        if logs_df.is_empty():
            return None

        # Filter for successful executions of this table/layer
        success_logs = logs_df.filter(
            (pl.col("table_name") == table_name) &
            (pl.col("layer_type") == layer_type) &
            (pl.col("status") == "success")
        ).sort("latest_date", descending=True)

        if len(success_logs) == 0:
            return None

        return success_logs.row(0, named=True)["latest_date"]
    except Exception:
        return None


def get_next_file(table_name: str, base_path: str) -> str:
    """
    Determines the next local file to process based on the audit log.
    If the last run failed, it returns that file to retry.
    If success, it returns the next file in alphabetical order.
    Returns None if no new files are available.
    """
    import re
    available_files = [f for f in os.listdir(base_path) if f.endswith('.csv') or f.endswith('.json')]
    
    # Filter strictly for YYYY-MM-DD pattern
    valid_files = []
    for f in available_files:
        if re.match(r"^\d{4}-\d{2}-\d{2}\.(csv|json)$", f):
            valid_files.append(f)
            
    # Sort by actual date parsing to be 100% safe
    def date_sort_key(filename):
        date_str = filename.split('.')[0]
        return datetime.strptime(date_str, "%Y-%m-%d")
        
    available_files = sorted(valid_files, key=date_sort_key)
    
    if not available_files:
        return None

    try:
        logs_df = read_parquet_safe(f"{LOG_PATH}/*.parquet")
        
        if logs_df.is_empty():
            return available_files[0]
        
        # Filter for this table AND only the raw ingestion layer
        # (other layers also log with the same table_name but with file_name="-"
        #  which would corrupt the "next file" tracking logic)
        table_logs = (
            logs_df
            .filter(
                (pl.col("table_name") == table_name) &
                (pl.col("layer_type") == "local_to_raw")
            )
            .sort("latest_date", descending=True)
        )

        if len(table_logs) == 0:
            return available_files[0]
            
        latest_log = table_logs.row(0, named=True)

        if latest_log["status"] == "failed":
            failed_file = latest_log["file_name"]
            # Only retry if it's a valid filename (not the placeholder "-")
            if failed_file != "-" and failed_file in available_files:
                return failed_file
            # Otherwise, fall through to return the first file
            return available_files[0]

        # If success, find the next file after the last successful file
        last_success_file = latest_log["file_name"]

        # Handle case where last success file is the placeholder "-"
        if last_success_file == "-":
            return available_files[0]
        
        try:
            idx = available_files.index(last_success_file)
            if idx + 1 < len(available_files):
                return available_files[idx + 1]
            else:
                return None # Up to date
        except ValueError:
            return available_files[0]
            
    except Exception:
        # Log directory doesn't exist yet, meaning this is the first run
        return available_files[0]


def get_next_s3_file_for_bronze(table_name: str, s3_dir: str) -> tuple:
    """
    Lists parquet files in an S3/MinIO directory and returns the next file
    that hasn't been successfully ingested to Bronze yet.

    Uses the same sequential file-tracking pattern as get_next_file(),
    but operates on S3 instead of local filesystem.

    Args:
        table_name: Name of the table (e.g., "students")
        s3_dir: S3 directory path (e.g., "s3://datalake/raw/students")

    Returns:
        Tuple of (filename, full_s3_path) or (None, None) if up to date.
    """
    import re
    import pyarrow.fs as pafs
    from utils.storage import get_pyarrow_fs

    # List files in S3 directory
    fs = get_pyarrow_fs()
    path_no_s3 = s3_dir.replace("s3://", "")

    try:
        file_infos = fs.get_file_info(pafs.FileSelector(path_no_s3, recursive=False))
        available_files = [
            fi.base_name for fi in file_infos
            if fi.type == pafs.FileType.File
            and re.match(r"^\d{4}-\d{2}-\d{2}\.parquet$", fi.base_name)
        ]
    except Exception as e:
        print(f"Warning: Could not list S3 files at {s3_dir}: {e}")
        return None, None

    if not available_files:
        return None, None

    # Sort by date ascending
    def date_sort_key(filename):
        return datetime.strptime(filename.split('.')[0], "%Y-%m-%d")

    available_files = sorted(available_files, key=date_sort_key)

    try:
        logs_df = read_parquet_safe(f"{LOG_PATH}/*.parquet")

        if logs_df.is_empty():
            first = available_files[0]
            return first, f"{s3_dir}/{first}"

        # Only consider successful raw_to_bronze logs for this table
        success_logs = logs_df.filter(
            (pl.col("table_name") == table_name) &
            (pl.col("layer_type") == "raw_to_bronze") &
            (pl.col("status") == "success")
        )

        if len(success_logs) == 0:
            first = available_files[0]
            return first, f"{s3_dir}/{first}"

        # Find latest successfully processed file
        processed = [
            f for f in success_logs["file_name"].to_list()
            if f != "-" and f in available_files
        ]

        if not processed:
            first = available_files[0]
            return first, f"{s3_dir}/{first}"

        last_processed = sorted(processed, key=date_sort_key)[-1]

        try:
            idx = available_files.index(last_processed)
            if idx + 1 < len(available_files):
                next_file = available_files[idx + 1]
                return next_file, f"{s3_dir}/{next_file}"
            else:
                return None, None  # Already up to date
        except ValueError:
            first = available_files[0]
            return first, f"{s3_dir}/{first}"

    except Exception:
        # No logs yet → first run
        first = available_files[0]
        return first, f"{s3_dir}/{first}"

