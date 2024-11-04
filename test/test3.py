from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, when

# Initialize Spark session
spark = SparkSession.builder.appName("ECommerceAnalysis").getOrCreate()


df = spark.read.csv("hdfs://localhost:9000/user/dejtes/contains_bad_data.csv", header=True, inferSchema=True)

df = spark.read.csv("hdfs://localhost:9000/user/nispri/final_data.csv", header=True, inferSchema=True)

# Calculate total sales (price * qty) and filter out records with invalid quantities (e.g., negative values)
df = df.filter(col("qty") > 0)

# Replace NULL values in 'price' and 'qty' with 0
df = df.withColumn("price", when(col("price").isNull(), 0).otherwise(col("price")))
df = df.withColumn("qty", when(col("qty").isNull(), 0).otherwise(col("qty")))

# Calculate total sales
df = df.withColumn("total_sale", col("price") * col("qty"))
total_sales = df.select(sum("total_sale").alias("Total Sales"))
total_sales.show()

# Top-selling products based on quantity sold
top_selling_products = df.groupBy("product_name").agg(sum("qty").alias("total_qty_sold")).orderBy(desc("total_qty_sold")).limit(10)
top_selling_products.show()

# Top 10 purchasing countries by category based on purchase count
top_countries_by_category = df.groupBy("country", "product_category").agg(sum("qty").alias("total_purchases")).orderBy(desc("total_purchases"))
top_countries_by_category.show(10)

