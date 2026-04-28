import polars as pl
import logging
from utils.storage import write_parquet_safe
from utils.audit import log_execution
from raw.readers import BaseReader

logger = logging.getLogger(__name__)

def ingest_to_raw(reader: BaseReader, table_name: str):
    """
    Reads ALL unprocessed raw data files using the provided Reader
    and uploads each to MinIO as Parquet.

    On first run (empty audit log), all existing files are backfilled.
    On subsequent runs, only new files since last success are processed.
    """
    logger.info(f"Checking for new data for {table_name} using {reader.__class__.__name__}...")

    files_processed = 0

    while True:
        df, next_file, source_path = reader.read_next(table_name)

        if df is None:
            if files_processed == 0:
                logger.info(f"No new data for {table_name}. Already up to date.")
            else:
                logger.info(f"Finished: {files_processed} file(s) ingested for {table_name}.")
            return

        logger.info(f"[{files_processed + 1}] Uploading {source_path} → MinIO raw/{table_name}")

        try:
            filename = next_file.split('.')[0]
            dest_path = f"s3://datalake/raw/{table_name}/{filename}.parquet"
            write_parquet_safe(df, dest_path)
            log_execution(table_name=table_name, layer_type="local_to_raw", status="success",
                          file_name=next_file, source_path=source_path)
            logger.info(f"Uploaded → {dest_path}")
            files_processed += 1

        except Exception as e:
            log_execution(table_name=table_name, layer_type="local_to_raw", status="failed",
                          file_name=next_file, source_path=source_path, message=str(e))
            logger.error(f"Failed to ingest {source_path}: {e}")
            raise e

