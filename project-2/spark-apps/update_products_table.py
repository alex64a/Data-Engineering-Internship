from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("Update Products Table") \
        .enableHiveSupport() \
        .getOrCreate()

FILE_NAME = "products.txt"
hdfs_products_update_path = f"hdfs://namenode:9000/products/{FILE_NAME}"

product_updates_rdd = spark.sparkContext.textFile(hdfs_product_updates_path)

# Convert to DataFrame
product_updates_df = product_updates_rdd.map(lambda line :  line.split(",")).toDF(["StockCode", "ProductDescription", "UnitPrice", "ProductDate"])


# Write to Hive table
product_updates_df.write.mode("append").saveAsTable("product_table")

print("Product table has been updated.")

