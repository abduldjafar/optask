import sys
import os

# Ensure modular src packages are discoverable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from silver.generic import process_dim_to_silver
from silver.transform import build_fact_student_performance, build_fact_class_summary

# Configuration for the daily performance pipeline
# Each dictionary represents an entity that needs to be ingested to Bronze and transformed to Silver
DAILY_PIPELINE_TABLES = [
    {
        "table_name": "students",
        "raw_source_path": "s3://datalake/raw/students",  # directory → incremental per-file
        "silver_callable": process_dim_to_silver
    },
    {
        "table_name": "attendance",
        "raw_source_path": "s3://datalake/raw/attendance",  # directory → incremental per-file
        "silver_callable": process_dim_to_silver
    },
    {
        "table_name": "assessments",
        "raw_source_path": "s3://datalake/raw/assessments",  # directory → incremental per-file
        "silver_callable": process_dim_to_silver
    }
]

# Fact tables built after all base Silver tables are ready
SILVER_FACT_TABLES = [
    {
        "table_name": "fact_student_performance",
        "callable": build_fact_student_performance,
    },
    {
        "table_name": "fact_class_summary",
        "callable": build_fact_class_summary,
    },
]
