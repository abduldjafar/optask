from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ensure modular src packages are discoverable by Airflow
dags_dir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.join(dags_dir, '../src')))

from bronze.ingest import ingest_file_to_bronze
from gold.aggregate import aggregate_class_daily_performance
from silver.transform import build_fact_student_performance, build_fact_class_summary
from silver.fact_builder import build_fact_daily_attendance, build_fact_daily_assessment
from utils.alerts import failure_callback, send_daily_summary

# Import the centralized dynamic config
from pipeline_config import DAILY_PIPELINE_TABLES

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False, # Disable default ugly email
    'email_on_retry': False,
    'on_failure_callback': failure_callback, # Use our custom beautiful HTML email
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'daily_performance_pipeline',
    default_args=default_args,
    description='End-to-end scalable data pipeline for class daily performance',
    schedule_interval='0 5 * * *',  # Every day at 05:00 UTC (12:00 WIB), after raw ingestion
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['analytics', 'performance'],
) as dag:

    # 3. Gold Layer (Aggregation)
    aggregate_gold = PythonOperator(
        task_id='aggregate_gold_layer',
        python_callable=aggregate_class_daily_performance,
    )

    # 4. Reporting (Email)
    email_summary = PythonOperator(
        task_id='send_daily_summary_email',
        python_callable=send_daily_summary,
    )
    
    aggregate_gold >> email_summary

    silver_tasks = []
    for table_config in DAILY_PIPELINE_TABLES:
        table_name = table_config["table_name"]
        
        # 1. Bronze Layer Task (Raw Ingestion)
        ingest_task = PythonOperator(
            task_id=f'ingest_{table_name}_bronze',
            python_callable=ingest_file_to_bronze,
            op_kwargs={
                'source_path': table_config['raw_source_path'], 
                'table_name': table_name, 
                'mode': 'upsert'
            },
        )

        # 2. Silver Layer Task (Cleaning & Typing)
        transform_task = PythonOperator(
            task_id=f'transform_{table_name}_silver',
            python_callable=table_config["silver_callable"],
            op_kwargs={'table_name': table_name},
        )

        ingest_task >> transform_task
        silver_tasks.append(transform_task)

    # 2b. Silver Fact Tables — sequential: fact_student first, then fact_class (which reads from fact_student)
    fact_student_task = PythonOperator(
        task_id='build_fact_student_performance',
        python_callable=build_fact_student_performance,
    )
    fact_class_task = PythonOperator(
        task_id='build_fact_class_summary',
        python_callable=build_fact_class_summary,
    )

    # 2c. Daily-grain fact tables (parallel, both depend on base silver)
    fact_daily_att_task = PythonOperator(
        task_id='build_fact_daily_attendance',
        python_callable=build_fact_daily_attendance,
    )
    fact_daily_ass_task = PythonOperator(
        task_id='build_fact_daily_assessment',
        python_callable=build_fact_daily_assessment,
    )

    # Dependency chain:
    # base silver → fact_student → fact_class (snapshot facts)
    # base silver → fact_daily_att + fact_daily_ass (daily facts, parallel)
    # fact_class + daily facts → Gold
    silver_tasks >> fact_student_task >> fact_class_task
    for daily_task in [fact_daily_att_task, fact_daily_ass_task]:
        silver_tasks >> daily_task

    # 3. Gold depends on BOTH snapshot facts and daily facts
    [fact_class_task, fact_daily_att_task, fact_daily_ass_task] >> aggregate_gold
