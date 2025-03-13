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
    "spark-airflow-youtube",
    default_args=default_args,
    description="Run Spark batch processing steps in order",
    schedule_interval="@once", 
)

# Task 1: Run clean_data.py
clean_data = BashOperator(
    task_id="youtube_clean_data",
    bash_command="spark-submit --master spark://spark:7077 /opt/spark-apps/youtube-api/clean_data.py",
    dag=dag,
)

# Task 2: Run transform_data.py (only after clean_data.py completes)
transform_data = BashOperator(
    task_id="youtube_transform_data",
    bash_command="spark-submit --master spark://spark:7077 /opt/spark-apps/youtube-api/transform_data.py",
    dag=dag,
)

# Task 3: Run write_to_postgres.py (only after transform_data.py completes)
write_to_postgres = BashOperator(
    task_id="youtube_write_to_postgres",
    bash_command="spark-submit --master spark://spark:7077 /opt/spark-apps/youtube-api/write_to_postgres.py",
    dag=dag,
)

# Define task dependencies
clean_data >> transform_data >> write_to_postgres