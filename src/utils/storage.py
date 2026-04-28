import os
import polars as pl
from deltalake import write_deltalake, DeltaTable

MINIO_URL = os.getenv("MINIO_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "AWS_ENDPOINT_URL": MINIO_URL,
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
}

def get_pyarrow_fs():
    from pyarrow.fs import S3FileSystem
    return S3FileSystem(
        access_key=AWS_ACCESS_KEY_ID,
        secret_key=AWS_SECRET_ACCESS_KEY,
        endpoint_override=MINIO_URL.replace("http://", "").replace("https://", ""),
        scheme="http"
    )

def read_delta_safe(path: str) -> pl.DataFrame:
    """
    Safely reads a Delta table from S3/MinIO using deltalake package,
    then converts to Polars DataFrame to avoid native Polars schema iteration bugs.
    """
    try:
        dt = DeltaTable(path, storage_options=STORAGE_OPTIONS)
        return pl.from_arrow(dt.to_pyarrow_table())
    except Exception as e:
        error_msg = str(e).lower()
        if "no table version found" in error_msg or "not a delta table" in error_msg or "no log files" in error_msg or "deltatable" in error_msg:
            print(f"Warning: Delta table at {path} does not exist or has no logs. Returning empty DataFrame.")
            return pl.DataFrame()
        raise e

def write_parquet_safe(df: pl.DataFrame, path: str):
    """
    Writes a Polars DataFrame to standard Parquet format in S3/MinIO.
    """
    import pyarrow.parquet as pq
    fs = get_pyarrow_fs()
    path_no_s3 = path.replace("s3://", "")
    pq.write_table(df.to_arrow(), path_no_s3, filesystem=fs)

def read_parquet_safe(path: str) -> pl.DataFrame:
    """
    Reads standard Parquet files from S3/MinIO. Supports globs like *.parquet
    """
    import pyarrow.dataset as ds
    fs = get_pyarrow_fs()
    path_no_s3 = path.replace("s3://", "")
    # Remove wildcard for pyarrow dataset if it's a directory
    if path_no_s3.endswith("/*.parquet"):
        path_no_s3 = path_no_s3.replace("/*.parquet", "")
    
    dataset = ds.dataset(path_no_s3, format="parquet", filesystem=fs)
    return pl.scan_pyarrow_dataset(dataset).collect()

def write_delta_safe(df: pl.DataFrame, path: str, mode: str = "overwrite", partition_by: list = None):
    """
    Writes a Polars DataFrame to a Delta table in S3/MinIO.

    Args:
        df: DataFrame to write
        path: S3 path to Delta table
        mode: Write mode ("overwrite", "append")
        partition_by: Optional list of columns to partition by (e.g., ["date"])
    """
    try:
        write_deltalake(
            path,
            df.to_arrow(),
            mode=mode,
            storage_options=STORAGE_OPTIONS,
            partition_by=partition_by
        )
    except Exception as e:
        error_msg = str(e).lower()
        # Only force overwrite when we explicitly want to overwrite (not append)
        # to avoid accidentally wiping data during parallel task logging.
        if mode != "append" and ("already exists" in error_msg or "no log files" in error_msg):
            print(f"Warning: Conflict when writing to {path} with mode={mode}. Forcing overwrite with schema evolution.")
            write_deltalake(
                path,
                df.to_arrow(),
                mode="overwrite",
                overwrite_schema=True,
                storage_options=STORAGE_OPTIONS,
                partition_by=partition_by
            )
        else:
            raise e

def upsert_delta_safe(df: pl.DataFrame, path: str, primary_keys: list, partition_by: list = None):
    """
    Upserts a Polars DataFrame into an existing Delta table based on primary keys.
    If the table does not exist, it falls back to creating it.

    Args:
        df: DataFrame to upsert
        path: S3 path to Delta table
        primary_keys: List of columns forming the composite key for upsert
        partition_by: Optional list of columns to partition by (e.g., ["date"])
    """
    try:
        dt = DeltaTable(path, storage_options=STORAGE_OPTIONS)

        # Build merge predicate: e.g., "source.id = target.id AND source.date = target.date"
        predicate = " AND ".join([f"source.{k} = target.{k}" for k in primary_keys])

        dt.merge(
            source=df.to_arrow(),
            predicate=predicate,
            source_alias="source",
            target_alias="target"
        ).when_matched_update_all().when_not_matched_insert_all().execute()

    except Exception as e:
        error_msg = str(e).lower()
        if "no table version found" in error_msg or "not a delta table" in error_msg or "no log files" in error_msg or "deltatable" in error_msg:
            print(f"Table at {path} does not exist, creating initially with partitioning.")
            write_delta_safe(df, path, mode="overwrite", partition_by=partition_by)
        else:
            raise e
