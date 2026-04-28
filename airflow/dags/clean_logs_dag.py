from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import os

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def clean_old_logs():
    """
    Connects to MinIO and deletes Airflow log files older than 14 days.
    """
    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv("MINIO_URL", "http://minio:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )
    
    bucket_name = "datalake"
    prefix = "system/airflow_logs/"
    
    cutoff_date = datetime.now().astimezone() - timedelta(days=14)
    print(f"Cleaning logs older than {cutoff_date}")
    
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    
    deleted_count = 0
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                last_modified = obj['LastModified']
                if last_modified < cutoff_date:
                    s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                    deleted_count += 1
                    
    print(f"Successfully deleted {deleted_count} old log files.")

with DAG(
    'clean_airflow_logs_dag',
    default_args=default_args,
    description='Pipeline to delete Airflow remote logs older than 14 days',
    schedule_interval='@monthly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['maintenance', 'logs'],
) as dag:

    clean_logs_task = PythonOperator(
        task_id='clean_old_logs_minio',
        python_callable=clean_old_logs,
    )

    clean_logs_task
