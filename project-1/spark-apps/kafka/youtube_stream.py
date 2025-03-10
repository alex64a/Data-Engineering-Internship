from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Postgres credentials from environment
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("YouTubeDataProcessing") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

# Read streaming data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "youtube_data") \
    .option("startingOffsets", "latest") \
    .load()

# Define schema for parsing JSON data
schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("author", StringType(), True),
    StructField("title", StringType(), True),
    StructField("views", IntegerType(), True),
    StructField("likes", IntegerType(), True),
    StructField("comments", IntegerType(), True),
])

# Parse JSON data from Kafka
json_df = kafka_df.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), schema).alias("data"), "timestamp") \
    .select("data.*", "timestamp")

# Clean data by removing rows with missing values
cleaned_df = json_df.filter(
    (col("video_id").isNotNull()) &
    (col("author").isNotNull()) &
    (col("title").isNotNull()) &
    (col("views").isNotNull()) &
    (col("likes").isNotNull()) &
    (col("comments").isNotNull())
)

# Process data to calculate engagement rate and identify potential spam
processed_df = cleaned_df.withColumn("engagement_rate", col("likes") / col("views"))
processed_df = processed_df.withColumn(
    "potential_spam",
    when((col("likes") > 10000) & (col("comments") < 10), True).otherwise(False)
)

# Define the logic to write processed data to PostgreSQL
def write_to_postgres(df, epoch_id, table_name):
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://localhost:5432/processed_data") \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("append") \
        .save()

# Write processed data to PostgreSQL
processed_df.writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "cleaned_videos")) \
    .option("checkpointLocation", "/tmp/checkpoints/processed") \
    .start()

# Aggregated DataFrame: Sum views, likes, and count videos by author
aggregated_df = cleaned_df.groupBy("author").agg(
    expr("sum(views) as total_views"),
    expr("sum(likes) as total_likes"),
    expr("count(video_id) as total_videos")
)

# Apply watermark on Kafka `timestamp` column for late data handling
aggregated_df_with_watermark = aggregated_df.withWatermark("timestamp", "10 minutes")

# Write aggregated data to PostgreSQL
aggregated_df_with_watermark.writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "aggregated_videos")) \
    .option("checkpointLocation", "/tmp/checkpoints/aggregated") \
    .start()

# Keep the streaming queries running
spark.streams.awaitAnyTermination()
