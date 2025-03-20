from pyspark.sql import SparkSession

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("Update Country Table") \
    .enableHiveSupport() \
    .getOrCreate()

# Define the path to the country updates file in HDFS
FILE_NAME = "country_updates.txt"
hdfs_country_updates_path = f"hdfs://namenode:9000/countries/{FILE_NAME}"

# Debugging: Print the HDFS path
print(f"Reading from HDFS path: {hdfs_country_updates_path}")

# Read country updates from HDFS as RDD
country_updates_rdd = spark.sparkContext.textFile(hdfs_country_updates_path)

# Debugging: Print the first few lines of the RDD
print("Sample data:")
country_updates_rdd.take(5).foreach(print)

# Convert RDD to DataFrame
country_updates_df = country_updates_rdd.map(lambda line: line.split(",")).toDF(["CountryID", "CountryName"])

# Write to Hive table
country_updates_df.write.mode("append").saveAsTable("country_table")

print("Country table has been updated.")