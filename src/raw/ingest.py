import polars as pl
import logging
from utils.storage import write_parquet_safe
from utils.audit import log_execution
from raw.readers import BaseReader

logger = logging.getLogger(__name__)

def ingest_to_raw(reader: BaseReader, table_name: str):
    """
    Reads the next available raw data using the provided Reader,
    and uploads it to MinIO as Parquet. Logs the status.
    """
    logger.info(f"Checking for new data for {table_name} using {reader.__class__.__name__}...")
    df, next_file, source_path = reader.read_next(table_name)

    if df is None:
        logger.info(f"No new data to ingest for {table_name}. Up to date.")
        return

    logger.info(f"Uploading {source_path} to MinIO raw/{table_name}")

    try:
        # Extract filename without extension for destination
        filename = next_file.split('.')[0]

        # Write to S3 as parquet
        dest_path = f"s3://datalake/raw/{table_name}/{filename}.parquet"
        write_parquet_safe(df, dest_path)

        # Log success
        log_execution(table_name=table_name, layer_type="local_to_raw", status="success", file_name=next_file, source_path=source_path)
        logger.info(f"Successfully uploaded to {dest_path}")

    except Exception as e:
        # Log failure and re-raise so Airflow catches it
        log_execution(table_name=table_name, layer_type="local_to_raw", status="failed", file_name=next_file, source_path=source_path, message=str(e))
        logger.error(f"Failed to ingest {source_path}: {e}")
        raise e
