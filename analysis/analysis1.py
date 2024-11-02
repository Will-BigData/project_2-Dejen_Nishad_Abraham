from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, month, desc

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EcommerceDataAnalysis") \
    .getOrCreate()

# Load the data
df = spark.read.csv("./terending_data.csv", header=True, inferSchema=True)

# Extract time-based features
df = df.withColumn("hour", hour("datetime")) \
       .withColumn("day_of_week", dayofweek("datetime")) \
       .withColumn("month", month("datetime"))

# 1. Top-Selling Categories
top_categories = df.groupBy("product_category") \
                   .sum("qty") \
                   .withColumnRenamed("sum(qty)", "total_qty") \
                   .orderBy(desc("total_qty"))
print("Top-Selling Categories:")
top_categories.show()

# 2. Top Products in Each Category
top_products_by_category = df.groupBy("product_category", "product_name") \
                             .sum("qty") \
                             .withColumnRenamed("sum(qty)", "total_qty") \
                             .orderBy("product_category", desc("total_qty"))
print("Top Products by Category:")
top_products_by_category.show()

# 3. Analysis of Peak Times
peak_times = df.groupBy("hour") \
               .sum("qty") \
               .withColumnRenamed("sum(qty)", "total_qty") \
               .orderBy(desc("total_qty"))
print("Peak Sales Times by Hour:")
peak_times.show()

# 4. High Demand Locations
top_locations = df.groupBy("city") \
                  .sum("qty") \
                  .withColumnRenamed("sum(qty)", "total_qty") \
                  .orderBy(desc("total_qty"))
print("High Demand Locations by City:")
top_locations.show()

# 5. Product Sales Over Time (for seasonality analysis)
monthly_sales = df.groupBy("month", "product_category") \
                  .sum("qty") \
                  .withColumnRenamed("sum(qty)", "total_qty") \
                  .orderBy("month", desc("total_qty"))
print("Monthly Sales by Product Category:")
monthly_sales.show()

# Additional Analysis

# 6. Transaction Failure Analysis by Reason
failed_transactions = df.filter(col("payment_transaction_success") == "N") \
                        .groupBy("failure_reason") \
                        .count() \
                        .orderBy(desc("count"))
print("Transaction Failure Count by Reason:")
failed_transactions.show()

# 7. Average Price by Product Category
avg_price_by_category = df.groupBy("product_category") \
                          .avg("price") \
                          .withColumnRenamed("avg(price)", "average_price") \
                          .orderBy(desc("average_price"))
print("Average Price by Product Category:")
avg_price_by_category.show()

# 8. Quantity Analysis by Country
qty_by_country = df.groupBy("country") \
                   .sum("qty") \
                   .withColumnRenamed("sum(qty)", "total_qty") \
                   .orderBy(desc("total_qty"))
print("Total Quantity Sold by Country:")
qty_by_country.show()

# 9. Negative Quantity Outlier Detection
negative_qty = df.filter(col("qty") <= 0)
print("Records with Negative or Zero Quantity:")
negative_qty.show()

# 10. Popular Products on Each E-Commerce Platform
popular_products_by_platform = df.groupBy("ecommerce_website_name", "product_name") \
                                 .sum("qty") \
                                 .withColumnRenamed("sum(qty)", "total_qty") \
                                 .orderBy("ecommerce_website_name", desc("total_qty"))
print("Popular Products by E-Commerce Platform:")
popular_products_by_platform.show()

# Stop Spark session
spark.stop()
