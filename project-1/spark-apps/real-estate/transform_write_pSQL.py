import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, when, avg
from pyspark.sql.types import IntegerType
from dotenv import load_dotenv

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_CONTAINER="postgres-database"
POSTGRES_DATABASE="processed_data"
POSTGRES_TABLE_100m2="properties_larger_than_100m2"
POSTGRES_TABLE_AVG_PRICE="properties_avg_price"

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_CONTAINER}:5432/{POSTGRES_DATABASE}"

POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("RealEstateBatchProcessing") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .getOrCreate()

# Load CSV into Spark DataFrame
file_path = "/opt/data/world_real_estate_data(147k).csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Clean 'apartment_total_area' by extracting numerical values
df = df.withColumn("apartment_total_area", regexp_extract(col("apartment_total_area"), r"(\d+)", 1).cast("float"))

# Compute 'price_per_sqm' (handling division by zero)
df = df.withColumn("price_per_sqm", when(col("apartment_total_area") > 0, col("price_in_USD") / col("apartment_total_area")))

# Convert 'building_construction_year' to integer
df = df.withColumn("building_construction_year", col("building_construction_year").cast(IntegerType()))

# Fill missing values in 'building_construction_year' with median
median_year = df.approxQuantile("building_construction_year", [0.5], 0.0)[0]
df = df.fillna({"building_construction_year": median_year})

# Filter properties with area > 100 sqm
df_filtered = df.filter(col("apartment_total_area") > 100)

# Aggregate average price per sqm by country
df_avg_price = df_filtered.groupBy("country").agg(avg("price_per_sqm").alias("avg_price_per_sqm"))

# Write transformed data to PostgreSQL
df_filtered.write \
    .mode("overwrite") \
    .jdbc(POSTGRES_URL, POSTGRES_TABLE_100m2, properties=POSTGRES_PROPERTIES)

df_avg_price.write \
    .mode("overwrite") \
    .jdbc(POSTGRES_URL, POSTGRES_TABLE_AVG_PRICE, properties=POSTGRES_PROPERTIES)

print("✅ Data successfully written to PostgreSQL!")
