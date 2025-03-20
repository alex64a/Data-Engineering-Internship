from pyspark.sql import SparkSession

# Initialize Spark session with Hive support
spark = SparkSession.builder \
        .appName("Process logs") \
        .enableHiveSupport() \
        .getOrCreate()


# Read all files from HDFS directory
logs_rdd = spark.sparkContext.textFile("hdfs://namenode:9000/log_data/*.txt")

# Convert RDD to DataFrame
logs_df = logs_rdd.map(lambda line: line.split(",")).toDF([
    "InvoiceNo", "StockCode", "Quantity", "InvoiceDate", "CustomerID", "Country"
])

# Write logs to Hive table
logs_df.write.mode("append").saveAsTable("logs_table")

print("Logs have been successfully written to Hive!")
