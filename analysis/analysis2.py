from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EcommerceDataHandling") \
    .getOrCreate()

# Load the data
df = spark.read.csv("./trending_data.csv", header=True, inferSchema=True)

# 1. Identifying Bad Data

# Filter and show records with invalid country (assuming a known list of valid countries)
invalid_country = df.filter(~col("country").isin("USA", "UK", "India", "Germany", "Australia", "Canada"))
print("Records with Invalid Country Names:")
invalid_country.show()

# Filter and show records with invalid price (non-numeric or non-positive values)
# Using rlike() for non-numeric detection and a simple comparison for non-positive values
invalid_price = df.filter(~col("price").rlike("^[0-9]+(\\.[0-9]+)?$") | (col("price") <= 0))
print("Records with Invalid Price Values:")
invalid_price.show()

# Filter and show records with invalid qty (negative or zero values)
invalid_qty = df.filter(col("qty") <= 0)
print("Records with Negative or Zero Quantity:")
invalid_qty.show()

# 2. Replacing Bad Data


# Replace invalid country values with "Unknown"
df = df.withColumn("country", when(~col("country").isin("USA", "UK", "India", "Germany", "Australia", "Canada"), "Unknown").otherwise(col("country")))

# Replace invalid price values with the median price
# Calculate median price
median_price = df.approxQuantile("price", [0.5], 0.01)[0]  # This computes an approximate median
df = df.withColumn("price", when(~col("price").rlike("^[0-9]+(\\.[0-9]+)?$") | (col("price") <= 0), lit(median_price)).otherwise(col("price")))

# Replace invalid quantity values with a default value (e.g., 1)
df = df.withColumn("qty", when(col("qty") <= 0, lit(1)).otherwise(col("qty")))

# Show Data after Replacement

df.show()

# Stop Spark session
spark.stop()
