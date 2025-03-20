import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row

# Initialize Spark session with Hive support
spark = SparkSession.builder \
        .appName("Process logs") \
        .enableHiveSupport() \
        .getOrCreate()

# Define schema
schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Quantity", IntegerType(), True),  # Ensure it's IntegerType
    StructField("InvoiceDate", StringType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True)
])

# Read raw text files from HDFS
logs_rdd = spark.sparkContext.textFile("hdfs://namenode:9000/data/logs/raw/*.txt")

# Convert each line into a Row object with correct types
def parse_line(line):
    parts = line.split(",")
    return Row(
        InvoiceNo=parts[0],
        StockCode=parts[1],
        Quantity=int(parts[2].strip()), 
        InvoiceDate=parts[3],
        CustomerID=parts[4],
        Country=parts[5]
    )

logs_rdd = logs_rdd.map(parse_line)

# Convert RDD to DataFrame
logs_df = spark.createDataFrame(logs_rdd, schema=schema)

# Recast columns explicitly in DataFrame
logs_df = logs_df.withColumn("Quantity", logs_df["Quantity"].cast(IntegerType()))

# Write logs to HDFS in Parquet format (Hive-compatible)
logs_df.write.mode("overwrite").format("parquet").option("compression", "snappy").save("hdfs://namenode:9000/data/logs/processed/")

print("✅ Logs have been successfully written to HDFS!")
