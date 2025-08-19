# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# -------------------------------
# 1. Create Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("Climate Data Analysis") \
    .getOrCreate()

# -------------------------------
# 2. Read CSV from HDFS
# -------------------------------
# Replace "yourname" with your Hadoop username
file_path = "https://raw.githubusercontent.com/MuhammadTaha201/Hadoop107/refs/heads/main/preprocessed_climate_data.csv"

df = spark.read.csv(
    file_path,
    header=True,       # first row is header
    inferSchema=True   # detect column types automatically
)

# -------------------------------
# 3. Show Data
# -------------------------------
print("First 10 rows of dataset:")
df.show(10)

print("Schema of dataset:")
df.printSchema()

# -------------------------------
# 4. Example Analysis
# -------------------------------
# Average temperature per month
avg_temp = df.groupBy("Month").agg(avg("Temperature").alias("Avg_Temperature"))
print("Average Temperature per Month:")
avg_temp.show()

# Average CO2 levels per location
avg_co2 = df.groupBy("Location").agg(avg("Co2_levels").alias("Avg_CO2"))
print("Average CO2 per Location:")
avg_co2.show()

# -------------------------------
# 5. Stop Spark
# -------------------------------
spark.stop()
