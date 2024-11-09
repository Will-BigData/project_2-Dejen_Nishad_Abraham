# This code identifys the bad data and removes them

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RemoveNullValues") \
    .getOrCreate()


df = spark.read.csv("./furniture_data.csv", header=True, inferSchema=True)

# Define valid countries list
valid_countries = ["USA", "Canada", "Germany", "Australia"]


# Identify rows with null in `product_id`
null_product_ids = df.filter(col("product_id").isNull()).select("product_id", "country", "city")
null_product_ids.show()

# Identify rows with blank or null values in `country`
blank_countries = df.filter((col("country") == "") | col("country").isNull()).select("product_id", "country", "city")
blank_countries.show()


# Filter out rows with NULL or empty product_id
df_filtered = df.filter(col("product_id").isNotNull())



# Filter out rows with NULL, empty, or invalid country values
df_filtered = df_filtered.filter((col("country").isin(valid_countries)) & (col("country") != ""))

# Show the cleaned DataFrame with product_id, country, and city columns
df_filtered.select("product_id", "country", "city").show()

# 3. Verification Query to ensure no NULL or invalid values remain
df_filtered.createOrReplaceTempView("cleaned_furniture_data")
verification_query = spark.sql("""
    SELECT COUNT(*) as invalid_count
    FROM cleaned_furniture_data
    WHERE product_id IS NULL OR country NOT IN ('USA', 'India', 'UK', 'Canada', 'Germany', 'Australia')
""")
verification_query.show()

# Save the cleaned DataFrame
df_filtered.write.csv("hdfs://localhost:9000/user/dejtes/cleaned_furniture_data.csv", header=True, mode="overwrite")


spark.stop()
