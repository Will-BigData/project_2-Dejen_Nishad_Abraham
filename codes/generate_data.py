from pyspark.sql import SparkSession
from faker import Faker
import random

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
failure_rows = 1000  # Limit failure rows to 1000

# Define categories and types for realistic product and transaction details
product_categories = ["Electronics", "Clothing", "Home", "Beauty", "Sports", "Automobile"]
payment_types = ["Card", "Internet Banking", "UPI", "Wallet"]
countries = ["USA", "UK", "India", "Germany", "Australia", "Canada"]
ecommerce_sites = ["Amazon", "Flipkart", "eBay", "AliExpress", "BestBuy"]
failure_reasons = ["Insufficient Funds", "Card Expired", "Payment Gateway Timeout", "Authentication Failed"]

# Define product names associated with each product category
product_names = {
    "Electronics": ["Smartphone", "Laptop", "Headphones", "Smartwatch", "Tablet"],
    "Clothing": ["T-shirt", "Jeans", "Jacket", "Sweater", "Dress"],
    "Home": ["Sofa", "Dining Table", "Lamp", "Curtains", "Cookware Set"],
    "Beauty": ["Lipstick", "Foundation", "Mascara", "Perfume", "Hair Dryer"],
    "Sports": ["Tennis Racket", "Basketball", "Football", "Yoga Mat", "Dumbbells"],
    "Automobile": ["Tire", "Car Battery", "Oil Filter", "Brake Pads", "Wiper Blades"]
}

# Generate data and store as a list of dictionaries
data = []

# First, generate 1000 rows with failure reasons and payment_txn_success = "N"
for i in range(failure_rows):
    category = random.choice(product_categories)
    record = {
        "order_id": i + 1,
        "customer_id": i + 2,
        "customer_name": fake.name(),
        "product_id": i + 3,
        "product_name": random.choice(product_names[category]),  # Match product name with category
        "product_category": category,
        "payment_type": random.choice(payment_types),
        "qty": random.randint(5, 20),
        "price": round(random.uniform(10, 1000), 2),
        "datetime": fake.date_time_between(start_date="-1y", end_date="now"),
        "country": random.choice(countries),
        "city": fake.city(),
        "ecommerce_website_name": random.choice(ecommerce_sites),
        "payment_txn_id": i + 4,
        "payment_txn_success": "N",
        "failure_reason": random.choice(failure_reasons)
    }
    data.append(record)

# Generate the remaining 14,000 rows with payment_txn_success = "Y" and empty failure_reason
for i in range(failure_rows, num_rows):
    category = random.choice(product_categories)
    record = {
        "order_id": i + 1,
        "customer_id": i + 2,
        "customer_name": fake.name(),
        "product_id": i + 3,
        "product_name": random.choice(product_names[category]),  # Match product name with category
        "product_category": category,
        "payment_type": random.choice(payment_types),
        "qty": random.randint(5, 20),
        "price": round(random.uniform(10, 1000), 2),
        "datetime": fake.date_time_between(start_date="-1y", end_date="now"),
        "country": random.choice(countries),
        "city": fake.city(),
        "ecommerce_website_name": random.choice(ecommerce_sites),
        "payment_txn_id": i + 4,
        "payment_txn_success": "Y",
        "failure_reason": ""
    }
    data.append(record)

# Convert data to a DataFrame
df = spark.createDataFrame(data)

# Repartition the DataFrame to spread data across multiple partitions
df = df.repartition(10)

# Coalesce into a single partition to write as a single CSV file
df.coalesce(1).write.csv("./e_comme_data.csv", header=True, mode="overwrite")

# Stop Spark session
spark.stop()