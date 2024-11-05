from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder.appName("CheckInvalidValues").getOrCreate()

# Load the DataFrame
df = spark.read.csv("hdfs://localhost:9000/user/nispri/furniture_data.csv", header=True, inferSchema=True)

# Define column categories
numeric_data = ["order_id", "customer_id", "product_id", "qty", "payment_txn_id"]
string_data = ["customer_name", "product_name", "product_category", "payment_type", "country", "city", "ecommerce_website_name", "payment_txn_success", "failure_reason"]

# Check for invalid values in numeric columns
for num in numeric_data:
    # Filter for non-numeric values, nulls, negatives, or zeroes
    invalid_numeric = df.filter(
        (col(num).cast("double").isNull() & col(num).isNotNull()) |  # Invalid strings
        (col(num).isNull()) |  # Null values
        (col(num).cast("double") < 0) |  # Negative values
        (col(num).cast("double") == 0)   # Zero values
    )
    
    print(f"Invalid values in numeric column '{num}':")
    invalid_numeric.show(truncate=False)

# Check for invalid values in string columns
for value in string_data:
    # Filter for nulls or empty strings
    invalid_string = df.filter(
        (col(value).isNull()) |  # Null values
        (col(value) == "")       # Empty strings
    )
    
    print(f"Invalid values in string column '{value}':")
    invalid_string.show(truncate=False)

