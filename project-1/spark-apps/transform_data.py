import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from dotenv import load_dotenv

load_dotenv()
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
# Initialize Spark session
spark = SparkSession.builder \
    .appName("TransformData") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

df = spark.read.parquet("s3a://youtube-data/cleaned/youtube_data.parquet")

df_aggregated = df.groupBy("author").agg(
    {"views": "sum", "likes": "avg", "comments": "sum"}
).withColumnRenamed("sum(views)", "total_views") \
 .withColumnRenamed("avg(likes)", "avg_likes") \
 .withColumnRenamed("sum(comments)", "total_comments")

# Save to minio
df_aggregated.write.mode("overwrite").parquet("s3a://youtube-data/aggregated/youtube_data.parquet")

# Write to postgres
df_aggregated.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/youtube_data_transformed") \
    .option("dbtable", "youtube_aggregated") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .mode("append") \
    .save()


