import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from dotenv import load_dotenv

load_dotenv()
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
FILE_NAME="youtube_data.parquet"
READ_DATA_PATH="s3a://youtube-data/cleaned/"
WRITE_DATA_PATH="s3a://youtube-data/aggregated/"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TransformData") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

df = spark.read.parquet(f"{READ_DATA_PATH}{FILE_NAME}")

df_aggregated = df.groupBy("author").agg(
    {"views": "sum", "likes": "avg", "comments": "sum"}
).withColumnRenamed("sum(views)", "total_views") \
 .withColumnRenamed("avg(likes)", "avg_likes") \
 .withColumnRenamed("sum(comments)", "total_comments")

# Save to minio
df_aggregated.write.mode("overwrite").parquet(f"{WRITE_DATA_PATH}{FILE_NAME}")


