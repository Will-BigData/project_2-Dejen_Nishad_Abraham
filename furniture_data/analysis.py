from pyspark.sql import SparkSession
from pyspark.sql.functions import month, hour, col, sum, count, lit

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FurnitureSalesAnalysis") \
    .getOrCreate()

# Load the data from HDFS
df = spark.read.csv("hdfs://localhost:9000/user/dejtes/final_data.csv", header=True, inferSchema=True)

# Define the output file path
output_path = "hdfs://localhost:9000/user/dejtes/analysis_results/final_analysis.csv"

# 1. Top-selling category of items per country
top_selling_category = df.groupBy("product_category", "country") \
    .agg(sum("qty").alias("total_qty_sold")) \
    .withColumn("analysis_type", lit("Top-selling category per country"))
print("Top Selling Categories Per Country:")
top_selling_category.show()

# 2. Popularity of products throughout the year per country
popularity_by_month_country = df.withColumn("month", month("datetime")) \
    .groupBy("product_name", "country", "month") \
    .agg(sum("qty").alias("total_qty_sold")) \
    .withColumn("analysis_type", lit("Popularity by month and country"))
print("Popularity of Products by Month and Country:")
popularity_by_month_country.show()

# 3. Locations with the highest traffic of sales
highest_traffic_locations = df.groupBy("city", "country") \
    .agg(count("*").alias("total_sales")) \
    .orderBy(col("total_sales").desc()) \
    .withColumn("analysis_type", lit("Highest traffic locations"))
print("Locations with Highest Traffic of Sales:")
highest_traffic_locations.show()

# 4. Times with the highest traffic of sales per country
traffic_by_hour_country = df.withColumn("hour", hour("datetime")) \
    .groupBy("country", "hour") \
    .agg(count("*").alias("sales_count")) \
    .withColumn("analysis_type", lit("Traffic by hour per country"))
print("Traffic of Sales by Hour Per Country:")
traffic_by_hour_country.show()

# Combine all DataFrames into a single DataFrame
consolidated_df = top_selling_category \
    .unionByName(popularity_by_month_country, allowMissingColumns=True) \
    .unionByName(highest_traffic_locations, allowMissingColumns=True) \
    .unionByName(traffic_by_hour_country, allowMissingColumns=True)

# Write the consolidated DataFrame to a single CSV file
consolidated_df.write.csv(output_path, header=True, mode="overwrite")

# Stop Spark session
spark.stop()
