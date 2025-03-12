from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Create Spark Session
spark = SparkSession.builder \
    .appName("YouTubeDataProcessing") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.7.4") \
    .getOrCreate()

# Read streaming data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "youtube_data") \
    .option("startingOffsets", "latest") \
    .load()

# Define schema for JSON parsing
schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("author", StringType(), True),
    StructField("title", StringType(), True),
    StructField("views", IntegerType(), True),
    StructField("likes", IntegerType(), True),
    StructField("comments", IntegerType(), True)
])

# Parse JSON from Kafka messages
json_df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

# Clean and transform data
cleaned_df = json_df.filter(
    col("video_id").isNotNull() &
    col("author").isNotNull() &
    col("title").isNotNull() &
    col("views").isNotNull() &
    col("likes").isNotNull() &
    col("comments").isNotNull()
).withColumn("engagement_rate", col("likes") / col("views"))

# Function to write batches to PostgreSQL
def write_to_postgres(df, epochId):
    try:
        row_count = df.count()
        print(f"Processing batch {epochId}, row count: {row_count}")

        if row_count == 0:
            print("No data in this batch, skipping write to PostgreSQL.")
            return

        # Write DataFrame to PostgreSQL
        df.write.format("jdbc") \
            .option("url", "jdbc:postgresql://postgres-database:5432/processed_data") \
            .option("dbtable", "cleaned_videos") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print(f"Batch {epochId} written to PostgreSQL successfully.")

    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")

# Write to PostgreSQL in micro-batches
query = cleaned_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .outputMode("append") \
    .start()

query.awaitTermination()
