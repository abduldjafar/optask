"""
Configuration for Gold layer aggregations.
Gold tables are pre-aggregated, optimized for BI/analytics.
"""
import polars as pl
from typing import Dict, List, Optional


class GoldTableConfig:
    """Configuration for building a gold aggregation table"""

    def __init__(
        self,
        table_name: str,
        source_tables: Dict[str, str],  # {alias: path}
        join_specs: List[Dict],  # List of join configurations
        date_column: Optional[str] = None,  # Column for incremental filtering
        primary_keys: Optional[List[str]] = None,  # For upsert mode
        select_columns: Optional[List[str]] = None,  # Final columns to select
        transformations: Optional[List] = None  # Additional transformations
    ):
        """
        Args:
            table_name: Name of the gold table
            source_tables: Source tables {alias: s3_path}
            join_specs: Join specifications
            date_column: Column to use for incremental filtering
            primary_keys: Composite key for upsert (usually includes date)
            select_columns: Final columns in output (None = all)
            transformations: Additional column transformations
        """
        self.table_name = table_name
        self.source_tables = source_tables
        self.join_specs = join_specs
        self.date_column = date_column
        self.primary_keys = primary_keys or []
        self.select_columns = select_columns
        self.transformations = transformations or []


# Configuration for all gold tables
GOLD_TABLES: Dict[str, GoldTableConfig] = {
    "class_daily_performance": GoldTableConfig(
        table_name="class_daily_performance",

        # Source tables from Silver layer
        source_tables={
            "attendance": "s3://datalake/silver/fact_daily_attendance",
            "assessment": "s3://datalake/silver/fact_daily_assessment",
            "class_summary": "s3://datalake/silver/fact_class_summary"
        },

        # Join specifications
        join_specs=[
            {
                "base": "attendance",  # Start with attendance (has class_id + date)
                "joins": [
                    {
                        "table": "assessment",
                        "on": ["class_id", "date"],
                        "how": "outer"  # Outer join to get all dates
                    },
                    {
                        "table": "class_summary",
                        "on": ["class_id"],
                        "how": "left",
                        "select": ["class_id", "total_students", "active_students"]
                    }
                ]
            }
        ],

        # Incremental configuration
        date_column="date",
        primary_keys=["class_id", "date"],

        # Final column selection (optional, None = all)
        select_columns=[
            "class_id",
            "date",
            "total_students",
            "active_students",
            "students_with_attendance",
            "present_count",
            "absent_count",
            "attendance_rate",
            "students_with_assessment",
            "assessment_count",
            "avg_score"
        ],

        # Column transformations (fill nulls, calculate rates, etc.)
        # NOTE: attendance_rate references present_count and students_with_attendance inline
        # so we use fill_null() directly in the expression rather than relying on
        # a previous fill_null transformation (Polars evaluates all exprs in parallel).
        transformations=[
            ("total_students", pl.col("total_students").fill_null(0)),
            ("active_students", pl.col("active_students").fill_null(0)),
            ("students_with_attendance", pl.col("students_with_attendance").fill_null(0)),
            ("present_count", pl.col("present_count").fill_null(0)),
            ("absent_count", pl.col("absent_count").fill_null(0)),
            ("students_with_assessment", pl.col("students_with_assessment").fill_null(0)),
            ("assessment_count", pl.col("assessment_count").fill_null(0)),
            ("avg_score", pl.col("avg_score").fill_null(0.0)),
            # attendance_rate: use fill_null inline so division is always numeric
            ("attendance_rate",
             (pl.col("present_count").fill_null(0) /
              pl.col("students_with_attendance").fill_null(1)  # avoid div-by-zero → 0/1=0
             ).fill_nan(0.0))
        ]
    )
}
