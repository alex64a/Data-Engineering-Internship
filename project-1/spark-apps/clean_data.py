import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from dotenv import load_dotenv

load_dotenv()
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataCleaning") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Load raw data from MinIO
df = spark.read.json("s3a://youtube-data/*")

# Remove unnecessary columns
columns_to_keep = ["video_id", "title", "author"]
df_cleaned = df.select(columns_to_keep)

df_cleaned = df_cleaned.dropna()

# Save cleaned data
df_cleaned.write.mode("overwrite").parquet("s3a://youtube-data/cleaned/youtube_data.parquet")
df_cleaned.show()

spark.stop()
