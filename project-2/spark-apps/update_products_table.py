import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import Row

# Initialize Spark session with Hive support
spark = SparkSession.builder \
        .appName("Update Products Table") \
        .enableHiveSupport() \
        .getOrCreate()

# Define file paths
FILE_NAME = "product_updates.txt"
hdfs_raw_path = f"hdfs://namenode:9000/data/products/raw/{FILE_NAME}"
hdfs_processed_path = "hdfs://namenode:9000/data/products/daily/"

# Debugging: Print the HDFS path
print(f"Reading from HDFS path: {hdfs_raw_path}")

# Define schema
schema = StructType([
    StructField("StockCode", StringType(), True),
    StructField("ProductDescription", StringType(), True),
    StructField("UnitPrice", DoubleType(), True),  # Ensure it's stored as a double
    StructField("ProductDate", StringType(), True)
])

# Read product updates from HDFS as RDD
product_updates_rdd = spark.sparkContext.textFile(hdfs_raw_path)

# Convert RDD to DataFrame with correct schema
def parse_line(line):
    parts = line.split(",")
    return Row(
        StockCode=parts[0].strip(),
        ProductDescription=parts[1].strip(),
        UnitPrice=float(parts[2].strip()),  # Convert to float
        ProductDate=parts[3].strip()
    )

product_updates_rdd = product_updates_rdd.map(parse_line)

# Convert RDD to DataFrame
product_updates_df = spark.createDataFrame(product_updates_rdd, schema=schema)

# Write data to HDFS in Parquet format (Hive-compatible)
product_updates_df.write.mode("overwrite").format("parquet").save(hdfs_processed_path)

print("✅ Product table has been updated and written to HDFS in Parquet format!")
