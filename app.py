# -*- coding: utf-8 -*-
import pandas as pd

# -------------------------------
# 1. Load CSV (local file)
# -------------------------------
file_path = "preprocessed_climate_data.csv"   # make sure CSV is in same folder

df = pd.read_csv(file_path)

# -------------------------------
# 2. Show Data
# -------------------------------
print("First 10 rows of dataset:")
print(df.head(10))

print("\nDataset Info:")
print(df.info())

# -------------------------------
# 3. Example Analysis
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
# 4. Save Results
# -------------------------------
avg_temp.to_csv("avg_temp_per_month.csv", index=False)
avg_co2.to_csv("avg_co2_per_location.csv", index=False)

print("\nResults saved as CSV files: avg_temp_per_month.csv, avg_co2_per_location.csv")
