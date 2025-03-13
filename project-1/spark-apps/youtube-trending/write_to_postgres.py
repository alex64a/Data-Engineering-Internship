import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
POSTGRES_CONTAINER="postgres-database"
POSTGRES_DATABASE="processed_data"
POSTGRES_TABLE="youtube_trending"
READ_DATA_PATH="s3a://youtube-trending/aggregated/youtube_trending.parquet"

# PostgreSQL connection properties
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_CONTAINER}:5432/{POSTGRES_DATABASE}"

POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Initialize Spark with PostgreSQL JDBC dependency
spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.5.0.jar") \
    .getOrCreate()


# Read data from MinIO
df = spark.read.parquet(READ_DATA_PATH)

# Write transformed data to PostgreSQL
df.write \
    .mode("overwrite") \
    .jdbc(POSTGRES_URL, POSTGRES_TABLE, properties=POSTGRES_PROPERTIES)

print("✅ Data successfully written to PostgreSQL!")