import polars as pl

# Configuration for standard Bronze-to-Silver dimension table processing.
# This allows for a generic transformation function, making it easy to add 100s of tables.
SILVER_DIM_TABLES = {
    "students": {
        "source_table": "s3://datalake/bronze/students",
        "columns": {
            "student_id": pl.Utf8,
            "student_name": pl.Utf8,
            "class_id": pl.Utf8,
            "grade_level": pl.Int32,
            "enrollment_status": pl.Utf8,
            "updated_at": pl.Datetime
        },
        "date_column": "updated_at",  # Column for incremental filtering
        "dedup_keys": ["student_id"],
        "dedup_sort_col": "updated_at",
        "not_null_cols": ["student_id", "class_id"]
    },
    "attendance": {
        "source_table": "s3://datalake/bronze/attendance",
        "columns": {
            "attendance_id": pl.Utf8,
            "student_id": pl.Utf8,
            "attendance_date": pl.Date,
            "status": pl.Utf8,
            "created_at": pl.Datetime
        },
        "date_column": "created_at",  # Column for incremental filtering
        "dedup_keys": ["attendance_id"],
        "dedup_sort_col": "created_at",
        "not_null_cols": ["student_id", "attendance_date", "status"],
        "partition_by": ["attendance_date"]  # Partition by date for query performance
    },
    "assessments": {
        "source_table": "s3://datalake/bronze/assessments",
        "columns": {
            "assessment_id": pl.Utf8,
            "student_id": pl.Utf8,
            "subject": pl.Utf8,
            "score": pl.Float64,
            "max_score": pl.Float64,
            "assessment_date": pl.Date,
            "created_at": pl.Datetime
        },
        "date_column": "created_at",  # Column for incremental filtering
        "dedup_keys": ["assessment_id"],
        "dedup_sort_col": "created_at",
        "not_null_cols": ["student_id", "assessment_date", "score"],
        "partition_by": ["assessment_date"]  # Partition by date for query performance
    }
}