from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, count, col, lit, month

spark = SparkSession.builder \
    .appName("FurnitureSalesAnalysis") \
    .getOrCreate()

df = spark.read.csv("hdfs://localhost:9000/user/<your_name>/clean_data.csv", header=True, inferSchema=True)


output_path = "hdfs://localhost:9000/user/<your_name>/analysis_results/final_analysis.csv"

# 1. Top-selling category of items per country
top_selling_category = df.withColumn("total_sale", col("qty") * col("price")) \
    .groupBy("product_category", "country") \
    .agg(sum("total_sale").alias("total_sales_amount")) \
    .orderBy(col("total_sales_amount").desc()) \
    .withColumn("analysis_type", lit("Top-selling category per country"))
print("Top Selling Categories Per Country:")
top_selling_category.show(20, truncate=False)


# 2. Popularity of products throughout the year per country
countries = ["USA", "Canada", "Germany", "Australia"]

# Calculate total quantity sold per product, per month, per country
popularity_by_month_country = df.withColumn("month", month("datetime")) \
    .groupBy("country", "month") \
    .agg(F.sum("qty").alias("total_qty_sold")) \
    .orderBy(col("month").asc()) \
    .withColumn("analysis_type", lit("Popularity by month and country"))

# Display a separate table for each country
for country in countries:
    print(f"Popularity of Products by Month in {country}:")
    popularity_by_month_country.filter(col("country") == country).show(truncate=False)
print("Popularity of Products by Month and Country:")
popularity_by_month_country.show(50, truncate=False)

# 3. Locations with the highest traffic of sales
highest_traffic_locations = df.groupBy("city", "country") \
    .agg(count("*").alias("total_sales")) \
    .orderBy(col("total_sales").desc()) \
    .withColumn("analysis_type", lit("Highest traffic locations"))
print("Locations with Highest Traffic of Sales:")
highest_traffic_locations.show(20, truncate=False)


# 4. Times with the highest traffic of sales per country

traffic_by_hour_country = df.withColumn("hour", hour("datetime")) \
    .groupBy("country", "hour") \
    .agg(count("*").alias("sales_count")) \
    .orderBy(col("hour").asc()) \
    .withColumn("analysis_type", lit("Traffic by hour per country"))

for country in countries:
    print(f"Traffic of Sales by Hour in {country}:")
    traffic_by_hour_country.filter(col("country") == country).show(24, truncate=False)
print("Traffic of Sales by Hour Per Country:")
traffic_by_hour_country.show(96, truncate=False)

# Combine all DataFrames into a single DataFrame
consolidated_df = top_selling_category \
    .unionByName(popularity_by_month_country, allowMissingColumns=True) \
    .unionByName(highest_traffic_locations, allowMissingColumns=True) \
    .unionByName(traffic_by_hour_country, allowMissingColumns=True)


consolidated_df.write.csv(output_path, header=True, mode="overwrite")

spark.stop()   