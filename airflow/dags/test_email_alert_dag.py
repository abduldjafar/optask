from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['abdul.haris.djafar@gmail.com'],
    'retries': 0, # No retries so it fails immediately
}

def force_fail():
    print("Mencoba mengirim email notifikasi ke haris.de.morata@gmail.com...")
    raise ValueError("🚨 INI ADALAH TEST ALERT AIRFLOW. JIKA EMAIL INI MASUK, KONFIGURASI SMTP BERHASIL! 🚨")

with DAG(
    'test_email_alert',
    default_args=default_args,
    description='A simple DAG that always fails to test SMTP Email Alerting',
    schedule_interval=None, # Only triggered manually
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['test', 'alerting'],
) as dag:

    test_failure_task = PythonOperator(
        task_id='trigger_intentional_failure',
        python_callable=force_fail,
    )
