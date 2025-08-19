# -*- coding: utf-8 -*-
import pandas as pd
from hdfs import InsecureClient

# -------------------------------
# 1. Connect to HDFS
# -------------------------------
# Change the URL to your Hadoop NameNode host & port
client = InsecureClient('http://localhost:9870', user='maria_dev')

# Path of CSV file in HDFS
hdfs_path = '/user/maria_dev/preprocessed_climate_data.csv'

# -------------------------------
# 2. Read CSV from HDFS into Pandas
# -------------------------------
with client.read(hdfs_path, encoding='utf-8') as reader:
    df = pd.read_csv(reader)

# -------------------------------
# 3. Show Data
# -------------------------------
print("First 10 rows of dataset:")
print(df.head(10))

print("\nDataset Info:")
print(df.info())

# -------------------------------
# 4. Example Analysis
# -------------------------------
# Average temperature per month
avg_temp = df.groupby("Month")["Temperature"].mean().reset_index()
print("\nAverage Temperature per Month:")
print(avg_temp)

# Average CO2 per location
avg_co2 = df.groupby("Location")["Co2_levels"].mean().reset_index()
print("\nAverage CO2 per Location:")
print(avg_co2)

# -------------------------------
# 5. Save Results back to HDFS
# -------------------------------
local_temp_file = "avg_temp_per_month.csv"
local_co2_file = "avg_co2_per_location.csv"

# Save locally first
avg_temp.to_csv(local_temp_file, index=False)
avg_co2.to_csv(local_co2_file, index=False)

# Upload results back to HDFS
client.upload("/user/maria_dev/avg_temp_per_month.csv", local_temp_file, overwrite=True)
client.upload("/user/maria_dev/avg_co2_per_location.csv", local_co2_file, overwrite=True)

print("\nResults uploaded to HDFS: /user/maria_dev/avg_temp_per_month.csv and avg_co2_per_location.csv")
