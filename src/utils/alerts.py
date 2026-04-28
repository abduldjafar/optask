from airflow.utils.email import send_email
from datetime import datetime
import polars as pl
from utils.storage import read_parquet_safe

RECIPIENT_EMAIL = "abdul.haris.djafar@gmail.com"

def failure_callback(context):
    """
    Custom Airflow failure callback to send a detailed HTML email.
    """
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    log_url = ti.log_url

    subject = f"❌ DAG Failed: {dag_id}.{task_id}"
    
    html_content = f"""
    <h2 style="color: red;">Task Failed Alert</h2>
    <p><strong>DAG:</strong> {dag_id}</p>
    <p><strong>Task:</strong> {task_id}</p>
    <p><strong>Execution Date:</strong> {execution_date}</p>
    <hr>
    <h3>Error Details:</h3>
    <pre style="background-color: #f4f4f4; padding: 10px; border-left: 4px solid red; font-family: monospace;">
{str(exception)}
    </pre>
    <br>
    <a href="{log_url}" style="background-color: #d9534f; color: white; padding: 10px 15px; text-decoration: none; border-radius: 4px;">View Full Airflow Logs</a>
    """
    
    send_email(to=RECIPIENT_EMAIL, subject=subject, html_content=html_content)


def send_daily_summary(**kwargs):
    """
    Reads the pipeline audit logs and the gold table, and sends a beautifully formatted summary email.
    Can be used as a PythonOperator callable at the end of the pipeline.
    """
    # Extract Airflow context to determine which DAG called this
    ti = kwargs.get('ti')
    dag_id = ti.dag_id if ti else "daily_performance_pipeline"
    execution_date = kwargs.get('execution_date', datetime.now())
    # Assuming Airflow is running on localhost:8080 based on docker-compose
    from urllib.parse import quote
    exec_date_iso = execution_date.strftime('%Y-%m-%dT%H:%M:%S+00:00') if hasattr(execution_date, 'strftime') else str(execution_date)
    dag_url = f"http://localhost:8080/graph?dag_id={dag_id}&execution_date={quote(exec_date_iso, safe='')}"

    # 1. Fetch Audit Logs
    try:
        logs_df = read_parquet_safe("s3://datalake/system/pipeline_logs/*.parquet")
        
        # Filter logs based on the pipeline that triggered the email
        if dag_id == "raw_ingestion_pipeline":
            logs_df = logs_df.filter(pl.col("layer_type") == "local_to_raw")
        else:
            logs_df = logs_df.filter(pl.col("layer_type").is_in(["raw_to_bronze", "bronze_to_silver", "silver_to_gold"]))

        if logs_df.is_empty():
            logs_html = "<p>No logs available today.</p>"
        else:
            # Deduplicate by table_name, file_name, and layer_type
            dedup_logs = (
                logs_df.sort("execution_time", descending=True)
                .group_by(["table_name", "layer_type", "file_name"])
                .first()
                .sort("execution_time", descending=True)
            )
            # Take the top 10 latest unique files processed
            recent_logs = dedup_logs.head(10)
            
            # Create HTML Table
            rows = ""
            for row in recent_logs.iter_rows(named=True):
                color = "green" if row['status'] == "success" else "red"
                rows += f"<tr><td>{row['table_name']}</td><td>{row.get('layer_type', '-')}</td><td>{row['file_name']}</td><td style='color:{color}; font-weight:bold;'>{row['status']}</td><td>{row['execution_time']}</td></tr>"
            
            logs_html = f"""
            <table border="1" cellpadding="5" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #f2f2f2;"><th>Table</th><th>Layer</th><th>File</th><th>Status</th><th>Execution Time</th></tr>
                {rows}
            </table>
            """
    except Exception as e:
        logs_html = f"<p>Error fetching logs: {str(e)}</p>"

    # 3. Format overall email
    subject = f"✅ Pipeline Summary: {dag_id} - {datetime.now().strftime('%Y-%m-%d')}"
    
    html_content = f"""
    <h2 style="color: #2e6c80;">Data Engineering Daily Report</h2>
    <p>The <strong>{dag_id}</strong> has completed successfully. Here is the summary of recent ingestion activities:</p>
    
    <h3>Audit Logs (Last 10 Unique Files Processed)</h3>
    {logs_html}
    
    <br>
    <a href="{dag_url}" style="background-color: #2e6c80; color: white; padding: 10px 15px; text-decoration: none; border-radius: 4px; display: inline-block; margin-top: 10px;">View Airflow DAG Run</a>
    <br><br>
    <p style="color: gray; font-size: 12px;">This is an automated message from your Airflow Data Platform.</p>
    """
    
    send_email(to=RECIPIENT_EMAIL, subject=subject, html_content=html_content)

