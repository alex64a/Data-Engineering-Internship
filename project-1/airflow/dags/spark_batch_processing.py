from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "spark_batch_processing",
    default_args=default_args,
    description="Run Spark batch processing steps in order",
    schedule_interval="*/10 * * * *",  # Runs every 10 minutes 
)

# Task 1: Run clean_data.py
clean_data = BashOperator(
    task_id="clean_data",
    bash_command="docker exec project-1-spark-1 ./bin/spark-submit /opt/spark-apps/clean_data.py",
    dag=dag,
)

# Task 2: Run transform_data.py (only after clean_data.py completes)
transform_data = BashOperator(
    task_id="transform_data",
    bash_command="docker exec project-1-spark-1 ./bin/spark-submit /opt/spark-apps/transform_data.py",
    dag=dag,
)

# Task 3: Run write_to_postgres.py (only after transform_data.py completes)
write_to_postgres = BashOperator(
    task_id="write_to_postgres",
    bash_command="docker exec project-1-spark-1 ./bin/spark-submit /opt/spark-apps/write_to_postgres.py",
    dag=dag,
)

# Define task dependencies
clean_data >> transform_data >> write_to_postgres
