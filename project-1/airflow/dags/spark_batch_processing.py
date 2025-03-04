from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG Configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 4),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spark_batch_processing",
    default_args=default_args,
    description="Run Spark job to process and load data into PostgreSQL",
    schedule_interval="*/10 * * * *",  # Run every 10 minutes
)

# Task: Run Spark Job
run_spark_job = BashOperator(
    task_id="run_spark_job",
    bash_command="spark-submit --master spark://spark:7077 /opt/spark-apps/write_to_postgres.py",
    dag=dag,
)

run_spark_job
