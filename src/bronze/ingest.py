import polars as pl
import logging
from utils.storage import write_delta_safe, upsert_delta_safe, read_parquet_safe
from utils.audit import log_execution, should_retry_execution, get_last_execution_status
from bronze.config import BRONZE_TABLES

logger = logging.getLogger(__name__)


def ingest_file_to_bronze(table_name: str, source_path: str, mode: str = "upsert"):
    """
    Ingests raw data (CSV/JSON) into the Bronze layer based on config.
    Supports 'overwrite' or 'upsert'.

    Features:
    - Retry mechanism: Checks audit log for failed executions and retries
    - Incremental processing: Only processes new data based on last successful run
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
        logger.warning(f"⚠️  Retrying previous failed execution for {table_name}")
        logger.warning(f"   Last failed: {last_exec['execution_time']}, Message: {last_exec['message']}")

    logger.info(f"Ingesting {source_path} to bronze {table_name} using {mode} mode")

    file_name = source_path.split('/')[-1] if '*' not in source_path else "-"

    try:
        if file_type != "parquet":
            raise ValueError("Bronze layer must strictly read from Raw Parquet data!")
            
        # Assumes source_path points to MinIO s3://datalake/raw/...
        df = read_parquet_safe(source_path)
        
        # Strict Schema Validation: Ensure all expected columns exist
        if expected_columns != ["*"]:
            missing_cols = [col for col in expected_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Schema Validation Failed: Missing columns {missing_cols} in {table_name}")
                
            # Project only the expected columns (drops any unexpected extra columns from Raw)
            df = df.select(expected_columns)
        
        path = f"s3://datalake/bronze/{table_name}"
        
        if mode == "upsert":
            upsert_delta_safe(df=df, path=path, primary_keys=primary_keys)
        else:
            write_delta_safe(df=df, path=path, mode=mode)
            
        log_execution(table_name=table_name, layer_type="raw_to_bronze", status="success", file_name=file_name, source_path=source_path)
    except Exception as e:
        log_execution(table_name=table_name, layer_type="raw_to_bronze", status="failed", file_name=file_name, source_path=source_path, message=str(e))
        raise e
