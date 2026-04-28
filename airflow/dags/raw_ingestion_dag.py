from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ensure modular src packages are discoverable by Airflow
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from raw.ingest import ingest_to_raw
from raw.readers import LocalCSVReader, LocalJSONReader
from utils.alerts import failure_callback, send_daily_summary

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
    'raw_ingestion_pipeline',
    default_args=default_args,
    description='Pipeline to upload local files to MinIO Raw Layer',
    schedule_interval='0 1 * * *',  # Every day at 01:00 UTC (08:00 WIB)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ingestion', 'raw'],
) as dag:

    ingest_students_raw = PythonOperator(
        task_id='ingest_students_raw',
        python_callable=ingest_to_raw,
        op_kwargs={'reader': LocalCSVReader('/opt/airflow/raw_data'), 'table_name': 'students'},
    )

    ingest_attendance_raw = PythonOperator(
        task_id='ingest_attendance_raw',
        python_callable=ingest_to_raw,
        op_kwargs={'reader': LocalCSVReader('/opt/airflow/raw_data'), 'table_name': 'attendance'},
    )

    ingest_assessments_raw = PythonOperator(
        task_id='ingest_assessments_raw',
        python_callable=ingest_to_raw,
        op_kwargs={'reader': LocalJSONReader('/opt/airflow/raw_data'), 'table_name': 'assessments'},
    )

    email_summary = PythonOperator(
        task_id='send_raw_summary_email',
        python_callable=send_daily_summary,
    )

    [ingest_students_raw, ingest_attendance_raw, ingest_assessments_raw] >> email_summary
