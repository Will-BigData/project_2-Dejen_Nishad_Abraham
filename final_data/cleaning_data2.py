# This code checks every column if there is bad data

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("RemoveNullValues") \
    .getOrCreate()


df = spark.read.csv("./furniture_data.csv", header=True, inferSchema=True)

# Define valid values for certain columns
valid_countries = ["USA", "Canada", "Germany", "Australia"]
valid_payment_types = ["Card", "Internet Banking", "UPI", "Wallet"]  

# Identify rows with null in `product_id`
null_product_ids = df.filter(col("product_id").isNull()).select("product_id", "country", "city")
null_product_ids.show()

# Identify rows with blank or null values in `country`
blank_countries = df.filter((col("country") == "") | col("country").isNull()).select("product_id", "country", "city")
blank_countries.show()

# Identify rows with null or empty values in `city`
blank_cities = df.filter((col("city") == "") | col("city").isNull()).select("product_id", "country", "city")
blank_cities.show()

# Identify rows with null or out-of-range values in `qty` (should be positive)
invalid_qty = df.filter((col("qty").isNull()) | (col("qty") <= 0)).select("product_id", "country", "city", "qty")
invalid_qty.show()

# Identify rows with null or out-of-range values in `price` (should be non-negative)
invalid_price = df.filter((col("price").isNull()) | (col("price") < 0)).select("product_id", "country", "city", "price")
invalid_price.show()

# Identify rows with null or empty values in `payment_type`
blank_payment_types = df.filter((col("payment_type") == "") | col("payment_type").isNull()).select("product_id", "country", "city", "payment_type")
blank_payment_types.show()

# Identify rows with null or empty values in `failure_reason`
blank_failure_reasons = df.filter((col("failure_reason") == "") | col("failure_reason").isNull()).select("product_id", "country", "city", "failure_reason")
blank_failure_reasons.show()


# Filter out rows with NULL or empty `product_id`
df_filtered = df.filter(col("product_id").isNotNull())

# Filter out rows with NULL, empty, or invalid `country` values
df_filtered = df_filtered.filter((col("country").isin(valid_countries)) & (col("country") != ""))

# Filter out rows with NULL or empty `city`
df_filtered = df_filtered.filter((col("city") != "") & col("city").isNotNull())

# Filter out rows with NULL or out-of-range `qty`
df_filtered = df_filtered.filter((col("qty").isNotNull()) & (col("qty") > 0))

# Filter out rows with NULL or out-of-range `price`
df_filtered = df_filtered.filter((col("price").isNotNull()) & (col("price") >= 0))

# Filter out rows with NULL, empty, or invalid `payment_type` values
df_filtered = df_filtered.filter((col("payment_type").isin(valid_payment_types)) & (col("payment_type") != ""))

# Filter out rows with NULL or empty `failure_reason` if necessary for analysis
df_filtered = df_filtered.filter((col("failure_reason") != "") & col("failure_reason").isNotNull())

# Show the cleaned DataFrame with relevant columns
df_filtered.select("product_id", "country", "city", "qty", "price", "payment_type", "failure_reason").show()

# 3. Verification Query to ensure no NULL or invalid values remain
df_filtered.createOrReplaceTempView("cleaned_furniture_data")
verification_query = spark.sql("""
    SELECT 
        COUNT(*) AS total_rows,
        COUNT(IF(product_id IS NULL, 1, NULL)) AS null_product_id_count,
        COUNT(IF(country IS NULL OR country NOT IN ('USA', 'India', 'UK', 'Canada', 'Germany', 'Australia'), 1, NULL)) AS invalid_country_count,
        COUNT(IF(city IS NULL OR city = '', 1, NULL)) AS null_city_count,
        COUNT(IF(qty IS NULL OR qty <= 0, 1, NULL)) AS invalid_qty_count,
        COUNT(IF(price IS NULL OR price < 0, 1, NULL)) AS invalid_price_count,
        COUNT(IF(payment_type IS NULL OR payment_type NOT IN ('Credit Card', 'Debit Card', 'Internet Banking', 'UPI', 'Wallet'), 1, NULL)) AS invalid_payment_type_count,
        COUNT(IF(failure_reason IS NULL OR failure_reason = '', 1, NULL)) AS null_failure_reason_count
    FROM cleaned_furniture_data
""")
print("Verification of Cleaned Data (Count of Remaining Invalid Rows per Column):")
verification_query.show()

# Save the cleaned DataFrame
df_filtered.write.csv("hdfs://localhost:9000/user/dejtes/cleaned_furniture_data.csv", header=True, mode="overwrite")


spark.stop()
