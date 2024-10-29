from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, expr
from faker import Faker
import random
import datetime

# Initialize Spark session with configurations for larger task sizes and memory
spark = SparkSession.builder \
    .appName("EcommerceDataGeneration") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Initialize Faker for generating realistic fake data
fake = Faker()

# Define the number of rows to generate
num_rows = 15000

# Define categories and types for realistic product and transaction details
product_categories = ["Electronics", "Clothing", "Home", "Beauty", "Sports", "Automobile"]
payment_types = ["Card", "Internet Banking", "UPI", "Wallet"]
countries = ["USA", "UK", "India", "Germany", "Australia", "Canada"]
ecommerce_sites = ["Amazon", "Flipkart", "eBay", "AliExpress", "BestBuy"]
failure_reasons = ["Insufficient Funds", "Card Expired", "Payment Gateway Timeout", "Authentication Failed", ""]

# Generate data and store as a list of dictionaries
data = []
for i in range(num_rows):
    # Determine failure reason and payment success status based on logic
    failure_reason = random.choice(failure_reasons)
    payment_txn_success = "N" if failure_reason else "Y"
    
    # Generate each field based on schema
    record = {
        "order_id": i + 1,  # Auto-increment order_id
        "customer_id": i + 2,  # Auto-increment customer_id
        "customer_name": fake.name(),
        "product_id": i + 3,  # Auto-increment product_id
        "product_name": fake.word(),
        "product_category": random.choice(product_categories),
        "payment_type": random.choice(payment_types),
        "qty": random.randint(5, 20),  # Increased range for quantity
        "price": round(random.uniform(10, 1000), 2),  # price between $10 and $1000
        "datetime": fake.date_time_between(start_date="-1y", end_date="now"),  # last year's range
        "country": random.choice(countries),
        "city": fake.city(),
        "ecommerce_website_name": random.choice(ecommerce_sites),
        "payment_txn_id": i + 4,  # Auto-increment payment transaction ID
        "payment_txn_success": payment_txn_success,
        "failure_reason": failure_reason
    }
    
    data.append(record)

# Convert data to a DataFrame
df = spark.createDataFrame(data)

# Repartition the DataFrame to spread data across multiple partitions
df = df.repartition(10)  # Adjust based on data size and available resources

# Coalesce into a single partition to write as a single CSV file
df.coalesce(1).write.csv("./example5.csv", header=True, mode="overwrite")
