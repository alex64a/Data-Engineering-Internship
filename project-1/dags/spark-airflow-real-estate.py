from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 6),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "spark-airflow-real-estate",
    default_args=default_args,
    description="Run Spark batch processing steps in order",
    schedule_interval="@once", 
)

# Task 1: Run transform_data.py
transform_write_pSQL = BashOperator(
    task_id="real_estate_transform_and_write_to_postgres",
    bash_command="spark-submit --master spark://spark:7077 /opt/spark-apps/real-estate/transform_write_pSQL.py",
    dag=dag,
)

transform_write_pSQL