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
    "spark-airflow",
    default_args=default_args,
    description="Run Spark batch processing steps in order",
    schedule_interval="@once", 
)

# Task 1: Run clean_data.py
clean_data = BashOperator(
    task_id="clean_data",
    bash_command="spark-submit --master spark://spark:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk:1.11.375 \
    --conf spark.hadoop.fs.s3a.access.key=minioadminaleksa\
    --conf spark.hadoop.fs.s3a.secret.key=minioadminaleksa\
    --conf spark.hadoop.fs.s3a.endpoint=minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    /opt/spark-apps/clean_data.py",
    dag=dag,
)

# Task 2: Run transform_data.py (only after clean_data.py completes)
transform_data = BashOperator(
    task_id="transform_data",
    bash_command="spark-submit --master spark://spark:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901,io.delta:delta-core_2.12:2.4.0 \
    --conf spark.hadoop.fs.s3a.access.key=minioadminaleksa \
    --conf spark.hadoop.fs.s3a.secret.key=minioadminaleksa \
    --conf spark.hadoop.fs.s3a.endpoint=minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    /opt/spark-apps/transform_data.py",
    dag=dag,
)

# Task 3: Run write_to_postgres.py (only after transform_data.py completes)
write_to_postgres = BashOperator(
    task_id="write_to_postgres",
    bash_command="spark-submit --master spark://spark:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901,io.delta:delta-core_2.12:2.4.0 \
    --conf spark.hadoop.fs.s3a.access.key=minioadminaleksa\
    --conf spark.hadoop.fs.s3a.secret.key=minioadminaleksa\
    --conf spark.hadoop.fs.s3a.endpoint=minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    /opt/spark-apps/write_to_postgres.py",
    dag=dag,
)

# Define task dependencies
clean_data >> transform_data >> write_to_postgres