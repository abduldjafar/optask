import polars as pl
import logging
from datetime import datetime
from typing import Dict, Any
from utils.storage import write_parquet_safe
import uuid

logger = logging.getLogger(__name__)
METRICS_PATH = "s3://datalake/system/pipeline_metrics"


def log_metrics(
    table_name: str,
    layer: str,
    metrics: Dict[str, Any],
    run_id: str = None
):
    """
    Log pipeline metrics for monitoring and observability.

    Args:
        table_name: Name of the table being processed
        layer: bronze, silver, gold
        metrics: Dictionary of metric_name -> metric_value
        run_id: Optional run identifier for grouping related metrics
    """
    if run_id is None:
        run_id = uuid.uuid4().hex[:8]

    timestamp = datetime.now()

    # Convert metrics dict to rows
    rows = []
    for metric_name, metric_value in metrics.items():
        rows.append({
            "run_id": run_id,
            "timestamp": timestamp,
            "table_name": table_name,
            "layer": layer,
            "metric_name": metric_name,
            "metric_value": str(metric_value),
            "metric_type": type(metric_value).__name__
        })

    df = pl.DataFrame(rows).with_columns(
        pl.col("timestamp").cast(pl.Datetime("us"))
    )

    # Write to unique file to avoid conflicts
    unique_id = uuid.uuid4().hex
    metrics_file = f"{METRICS_PATH}/{table_name}_{layer}_{unique_id}.parquet"
    write_parquet_safe(df, metrics_file)

    logger.info(f"📊 Metrics logged for {table_name} ({layer}): {len(metrics)} metrics")


class MetricsCollector:
    """Context manager for collecting and logging pipeline metrics"""

    def __init__(self, table_name: str, layer: str):
        self.table_name = table_name
        self.layer = layer
        self.metrics = {}
        self.run_id = uuid.uuid4().hex[:8]
        self.start_time = None

    def __enter__(self):
        self.start_time = datetime.now()
        return self

    def __exit__(self, exc_type, _exc_val, _exc_tb):
        # Calculate duration
        duration_seconds = (datetime.now() - self.start_time).total_seconds()
        self.metrics["duration_seconds"] = round(duration_seconds, 2)
        self.metrics["success"] = exc_type is None

        # Log all collected metrics
        log_metrics(
            table_name=self.table_name,
            layer=self.layer,
            metrics=self.metrics,
            run_id=self.run_id
        )

        return False  # Don't suppress exceptions

    def add(self, metric_name: str, value: Any):
        """Add a metric to be logged"""
        self.metrics[metric_name] = value

    def add_dataframe_stats(self, df: pl.DataFrame, prefix: str = ""):
        """Add common dataframe statistics"""
        prefix = f"{prefix}_" if prefix else ""
        self.add(f"{prefix}row_count", len(df))
        self.add(f"{prefix}column_count", len(df.columns))
        self.add(f"{prefix}size_bytes", df.estimated_size())

        # Null counts per column
        for col in df.columns:
            null_count = df[col].null_count()
            if null_count > 0:
                self.add(f"{prefix}nulls_{col}", null_count)
