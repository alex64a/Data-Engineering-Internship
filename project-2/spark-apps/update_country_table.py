import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row
from pyspark.sql.functions import col, substring, when

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("Process Country Table") \
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
    return Row(
        CountryID=parts[0].strip(),
        CountryName=parts[1].strip()
    )

country_updates_rdd = country_updates_rdd.map(parse_line)
country_updates_df = spark.createDataFrame(country_updates_rdd, schema=schema)

# Extract ContinentCode (First two digits of CountryID)
country_updates_df = country_updates_df.withColumn("ContinentCode", substring(col("CountryID"), 1, 2))

# Map ContinentCode to ContinentName
continent_mapping = {
    "11": "Europe",
    "22": "Africa",
    "33": "Asia",
    "44": "Australia & Oceania",
    "55": "North America",
    "66": "South America",
    "13": "Europe & Asia",  # Turkey
    "10": "Europe (Worldwide Territories)",  # France, UK
    "50": "North America (Worldwide)"  # USA
}

# Apply mapping
country_updates_df = country_updates_df.withColumn(
    "ContinentName",
    when(col("ContinentCode") == "11", "Europe")
    .when(col("ContinentCode") == "22", "Africa")
    .when(col("ContinentCode") == "33", "Asia")
    .when(col("ContinentCode") == "44", "Australia & Oceania")
    .when(col("ContinentCode") == "55", "North America")
    .when(col("ContinentCode") == "66", "South America")
    .when(col("ContinentCode") == "13", "Europe & Asia")
    .when(col("ContinentCode") == "10", "Europe (Worldwide Territories)")
    .when(col("ContinentCode") == "50", "North America (Worldwide)")
    .otherwise("Unknown")
)

# Write data to HDFS in Parquet format (Hive-compatible)
country_updates_df.write.mode("overwrite").format("parquet").save(hdfs_processed_path)

print("✅ Country table has been updated and written to HDFS in Parquet format!")
