import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row

# Initialize Spark session with Hive support
spark = SparkSession.builder \
        .appName("Update Country Table") \
        .enableHiveSupport() \
        .getOrCreate()

# Define file paths
FILE_NAME = "country_updates.txt"
hdfs_raw_path = f"hdfs://namenode:9000/data/countries/raw/{FILE_NAME}"
hdfs_processed_path = "hdfs://namenode:9000/data/countries/daily/"

# Debugging: Print the HDFS path
print(f"Reading from HDFS path: {hdfs_raw_path}")

# Define schema
schema = StructType([
    StructField("CountryID", StringType(), True),
    StructField("CountryName", StringType(), True)
])

# Read country updates from HDFS as RDD
country_updates_rdd = spark.sparkContext.textFile(hdfs_raw_path)

# Convert RDD to DataFrame with correct schema
def parse_line(line):
    parts = line.split(",")
    return Row(CountryID=parts[0].strip(), CountryName=parts[1].strip())

country_updates_rdd = country_updates_rdd.map(parse_line)

# Convert RDD to DataFrame
country_updates_df = spark.createDataFrame(country_updates_rdd, schema=schema)

# Write data to HDFS in Parquet format (Hive-compatible)
country_updates_df.write.mode("overwrite").format("parquet").save(hdfs_processed_path)

print("✅ Country table has been updated and written to HDFS in Parquet format!")
