import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, create_map, lit
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
READ_DATA_PATH = "s3a://youtube-trending/cleaned/"
WRITE_DATA_PATH = "s3a://youtube-trending/aggregated/"
FILE_NAME="youtube_trending.parquet"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TransformData") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Read Parquet file from MinIO
#df = spark.read.option("mergeSchema", "true").parquet(READ_DATA_PATH)
df = spark.read.parquet(f"{READ_DATA_PATH}{FILE_NAME}")

# Define category mapping
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
df_transformed = df.withColumn("category", category_map[col("category")])

# Save transformed data to MinIO
df_transformed.write.mode("overwrite").parquet(f"{WRITE_DATA_PATH}{FILE_NAME}")

