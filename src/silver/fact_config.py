import polars as pl
from typing import Dict, List, Optional, Callable


class AggregationRule:
    """Defines how to aggregate a column"""

    def __init__(self, source_col: str, agg_func: str, alias: str = None, expr: Optional[Callable] = None):
        """
        Args:
            source_col: Column to aggregate
            agg_func: 'sum', 'mean', 'count', 'min', 'max', 'first', 'last', or 'custom'
            alias: Output column name (defaults to source_col)
            expr: Custom polars expression function (used when agg_func='custom')
        """
        self.source_col = source_col
        self.agg_func = agg_func
        self.alias = alias or source_col
        self.expr = expr

    def to_polars_expr(self) -> pl.Expr:
        """Convert to polars aggregation expression"""
        if self.agg_func == "custom" and self.expr:
            return self.expr().alias(self.alias)

        col_expr = pl.col(self.source_col)

        if self.agg_func == "sum":
            return col_expr.sum().alias(self.alias)
        elif self.agg_func == "mean":
            return col_expr.mean().alias(self.alias)
        elif self.agg_func == "count":
            return pl.len().alias(self.alias)
        elif self.agg_func == "min":
            return col_expr.min().alias(self.alias)
        elif self.agg_func == "max":
            return col_expr.max().alias(self.alias)
        elif self.agg_func == "first":
            return col_expr.first().alias(self.alias)
        elif self.agg_func == "last":
            return col_expr.last().alias(self.alias)
        else:
            raise ValueError(f"Unknown aggregation function: {self.agg_func}")


class JoinSpec:
    """Specification for joining two dataframes"""

    def __init__(
        self,
        source_table: str,
        join_on: List[str] | str,
        join_type: str = "left",
        select_cols: Optional[List[str]] = None,
        rename_map: Optional[Dict[str, str]] = None,
        pre_aggregate: Optional[List[AggregationRule]] = None
    ):
        """
        Args:
            source_table: Path to source delta table (e.g., "s3://datalake/silver/students")
            join_on: Column(s) to join on
            join_type: "left", "inner", "outer", "right"
            select_cols: Columns to select from joined table (None = all)
            rename_map: Rename columns after join {old_name: new_name}
            pre_aggregate: Aggregations to apply before joining (group by join_on)
        """
        self.source_table = source_table
        self.join_on = [join_on] if isinstance(join_on, str) else join_on
        self.join_type = join_type
        self.select_cols = select_cols
        self.rename_map = rename_map or {}
        self.pre_aggregate = pre_aggregate or []


class FactTableConfig:
    """
    Configuration for building a fact table.
    Supports both detail fact tables (FK joins) and aggregate fact tables (group by + agg).
    """

    def __init__(
        self,
        table_name: str,
        primary_table: str,
        primary_keys: List[str],
        joins: Optional[List[JoinSpec]] = None,
        group_by: Optional[List[str]] = None,
        aggregations: Optional[List[AggregationRule]] = None,
        filters: Optional[List[Callable[[pl.DataFrame], pl.DataFrame]]] = None,
        post_process: Optional[Callable[[pl.DataFrame], pl.DataFrame]] = None,
        mode: str = "overwrite",
        date_column: Optional[str] = None,
        partition_by: Optional[List[str]] = None
    ):
        """
        Args:
            table_name: Name of the fact table
            primary_table: Main source table to start from
            primary_keys: Columns that uniquely identify a row
            joins: List of join specifications (optional)
            group_by: Columns to group by for aggregation (optional)
            aggregations: List of aggregation rules (optional)
            filters: List of filter functions to apply
            post_process: Function to apply final transformations
            mode: Write mode - "overwrite", "append", or "upsert"
            date_column: Column for incremental filtering (optional)
            partition_by: Columns to partition by (e.g., ["date"] for daily facts)
        """
        self.table_name = table_name
        self.primary_table = primary_table
        self.primary_keys = primary_keys
        self.joins = joins or []
        self.group_by = group_by
        self.aggregations = aggregations or []
        self.filters = filters or []
        self.post_process = post_process
        self.mode = mode
        self.date_column = date_column
        self.partition_by = partition_by


# Configuration for all fact tables
SILVER_FACT_TABLES: Dict[str, FactTableConfig] = {
    "fact_student_performance": FactTableConfig(
        table_name="fact_student_performance",
        primary_table="s3://datalake/silver/students",
        primary_keys=["student_id"],
        date_column="updated_at",  # Column for incremental filtering
        joins=[
            # Join with aggregated attendance stats
            JoinSpec(
                source_table="s3://datalake/silver/attendance",
                join_on="student_id",
                join_type="left",
                pre_aggregate=[
                    AggregationRule("attendance_id", "count", "total_attendance"),
                    AggregationRule("status", "custom", "present_count",
                                    expr=lambda: (pl.col("status") == "PRESENT").sum()),
                    AggregationRule("status", "custom", "absent_count",
                                    expr=lambda: (pl.col("status") == "ABSENT").sum()),
                    AggregationRule("status", "custom", "attendance_rate",
                                    expr=lambda: ((pl.col("status") == "PRESENT").sum() / pl.len()).fill_nan(0.0)),
                ]
            ),
            # Join with aggregated assessment stats
            JoinSpec(
                source_table="s3://datalake/silver/assessments",
                join_on="student_id",
                join_type="left",
                pre_aggregate=[
                    AggregationRule("assessment_id", "count", "total_assessments"),
                    AggregationRule("score", "mean", "avg_score"),
                    AggregationRule("score", "custom", "avg_score_pct",
                                    expr=lambda: (pl.col("score") / pl.col("max_score")).mean().fill_nan(0.0)),
                ]
            )
        ],
        mode="upsert",
        partition_by=["snapshot_date"]  # Partition by snapshot date
    ),

    "fact_class_summary": FactTableConfig(
        table_name="fact_class_summary",
        primary_table="s3://datalake/silver/fact_student_performance",
        primary_keys=["class_id"],
        date_column="snapshot_date",  # Column for incremental filtering
        joins=[
            # Join with students to get active students count per class
            JoinSpec(
                source_table="s3://datalake/silver/students",
                join_on="class_id",
                join_type="left",
                pre_aggregate=[
                    AggregationRule("student_id", "custom", "active_students",
                                    expr=lambda: (pl.col("enrollment_status") == "ACTIVE").sum()),
                ]
            )
        ],
        group_by=["class_id"],
        aggregations=[
            AggregationRule("student_id", "count", "total_students"),
            AggregationRule("attendance_rate", "mean", "avg_attendance_rate"),
            AggregationRule("avg_score", "mean", "avg_score"),
            AggregationRule("avg_score_pct", "mean", "avg_score_pct"),
            AggregationRule("grade_level", "first", "grade_level"),
            AggregationRule("active_students", "first", "active_students"),  # Pass through from join
        ],
        mode="upsert",
        partition_by=["snapshot_date"]  # Partition by snapshot date
    ),

    "fact_daily_attendance": FactTableConfig(
        table_name="fact_daily_attendance",
        primary_table="s3://datalake/silver/attendance",
        primary_keys=["class_id", "date"],
        date_column="attendance_date",  # For incremental filtering
        joins=[
            # Join to get class_id per student
            JoinSpec(
                source_table="s3://datalake/silver/students",
                join_on="student_id",
                join_type="left",
                select_cols=["class_id"]
            )
        ],
        group_by=["class_id", "attendance_date"],
        aggregations=[
            AggregationRule("student_id", "count", "students_with_attendance"),
            AggregationRule("status", "custom", "present_count",
                            expr=lambda: (pl.col("status") == "PRESENT").sum()),
            AggregationRule("status", "custom", "absent_count",
                            expr=lambda: (pl.col("status") == "ABSENT").sum()),
        ],
        post_process=lambda df: df.rename({"attendance_date": "date"}),
        mode="upsert",
        partition_by=["date"]  # Partition by date (after rename in post_process)
    ),

    "fact_daily_assessment": FactTableConfig(
        table_name="fact_daily_assessment",
        primary_table="s3://datalake/silver/assessments",
        primary_keys=["class_id", "date"],
        date_column="assessment_date",  # For incremental filtering
        joins=[
            # Join to get class_id per student
            JoinSpec(
                source_table="s3://datalake/silver/students",
                join_on="student_id",
                join_type="left",
                select_cols=["class_id"]
            )
        ],
        group_by=["class_id", "assessment_date"],
        aggregations=[
            AggregationRule("student_id", "custom", "students_with_assessment",
                            expr=lambda: pl.col("student_id").n_unique()),
            AggregationRule("assessment_id", "count", "assessment_count"),
            AggregationRule("score", "mean", "avg_score"),
        ],
        post_process=lambda df: df.rename({"assessment_date": "date"}),
        mode="upsert",
        partition_by=["date"]  # Partition by date (after rename in post_process)
    ),
}
