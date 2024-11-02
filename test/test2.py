from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand

# Initialize Spark session
spark = SparkSession.builder.appName("ECommerceDataCleaning").getOrCreate()

# Load data from HDFS
df = spark.read.csv("hdfs://localhost:9000/user/dejtes/contains_bad_data.csv", header=True, inferSchema=True)

# Display schema and sample data
df.printSchema()
df.show(5)


# Check for invalid 'price' values
invalid_price_count = df.filter(
    (col("price") == -1.99) | (col("price") == "NaN") | (col("price") == "Free")
).count()
print(f"Number of invalid 'price' values: {invalid_price_count}")


# Remove rows with invalid 'price' values
df = df.filter(~((col("price") == -1.99) | (col("price") == "NaN") | (col("price") == "Free")))


# Replace invalid 'price' values with a random number greater than 100
df = df.withColumn(
    "price",
    when((col("price") == -1.99) | (col("price") == "NaN") | (col("price") == "Free"), rand() * 400 + 100)
    .otherwise(col("price"))
)



# Check for invalid 'qty' values
invalid_qty_count = df.filter(
    (col("qty") <= 0) | (col("qty").isin(-5, -15))
).count()
print(f"Number of invalid 'qty' values: {invalid_qty_count}")

# Check for unusual 'country' values
unusual_country_count = df.filter(
    col("country").isin("Mars", "Atlantis", "Unknown")
).count()
print(f"Number of unusual 'country' values: {unusual_country_count}")

# Display sample rows of invalid data for verification (optional)
print("Sample rows with invalid 'price' values:")
df.filter((col("price") == -1.99) | (col("price") == "NaN") | (col("price") == "Free")).show(5)

print("Sample rows with invalid 'qty' values:")
df.filter((col("qty") <= 0) | (col("qty").isin(-5, -15))).show(5)

print("Sample rows with unusual 'country' values:")
df.filter(col("country").isin("Mars", "Atlantis", "Unknown")).show(5)