from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, create_map, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_CONTAINER = "postgres-database"
POSTGRES_DATABASE = "processed_data"
POSTGRES_TABLE = "youtube_trending_stream"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("YouTubeDataProcessing") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
            "org.postgresql:postgresql:42.7.4") \
    .getOrCreate()

# Read Kafka data
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "youtube_trending") \
    .option("startingOffsets", "earliest") \
    .load()

# Define schema for JSON data
schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("category", StringType(), True),
    StructField("views", StringType(), True),  
    StructField("likes", StringType(), True),  
    StructField("comments", StringType(), True),
    StructField("published_date", StringType(), True)
])

# Parse JSON data
json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert string fields to integers
cleaned_df = json_df.withColumn("views", col("views").cast("int")) \
    .withColumn("likes", col("likes").cast("int")) \
    .withColumn("comments", col("comments").cast("int"))

# Remove null values
cleaned_df = cleaned_df.dropna()

# Category Mapping
category_mapping = {
    "1": "Film & Animation", "2": "Autos & Vehicles", "10": "Music", "15": "Pets & Animals",
    "17": "Sports", "18": "Short Movies", "19": "Travel & Events", "20": "Gaming",
    "21": "Videoblogging", "22": "People & Blogs", "23": "Comedy", "24": "Entertainment",
    "25": "News & Politics", "26": "Howto & Style", "27": "Education", "28": "Science & Technology",
    "29": "Nonprofits & Activism", "30": "Movies", "31": "Anime/Animation", "32": "Action/Adventure",
    "33": "Classics", "34": "Comedy", "35": "Documentary", "36": "Drama", "37": "Family",
    "38": "Foreign", "39": "Horror", "40": "Sci-Fi/Fantasy", "41": "Thriller", "42": "Shorts",
    "43": "Shows", "44": "Trailers"
}

# Convert dictionary to key-value pairs for Spark mapping
category_map = create_map([lit(x) for pair in category_mapping.items() for x in pair])

# Apply category mapping
processed_df = cleaned_df.withColumn("category", category_map[col("category")])

# Function to handle batch write to PostgreSQL
def write_to_postgres(df, epochId):
    try:
        df.write.format("jdbc") \
            .option("url", f"jdbc:postgresql://{POSTGRES_CONTAINER}:5432/{POSTGRES_DATABASE}") \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")

# Initiate the streaming query
query = processed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/checkpoints/youtube_postgres") \
    .start()

# Wait for the query to terminate
query.awaitTermination()
