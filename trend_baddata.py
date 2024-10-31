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
bad_data_rows = int(num_rows * 0.05)  # 5% bad data
failure_rows = 500

# Define product categories and realistic product names for each category
product_categories = ["Electronics", "Clothing", "Home", "Beauty", "Sports", "Automobile"]
product_names = {
    "Electronics": ["Smartphone", "Laptop", "Headphones", "Smartwatch", "Bluetooth Speaker"],
    "Clothing": ["Jeans", "T-Shirt", "Dress", "Jacket", "Sneakers"],
    "Home": ["Vacuum Cleaner", "Blender", "Sofa", "Lamp", "Microwave"],
    "Beauty": ["Lipstick", "Face Cream", "Perfume", "Shampoo", "Nail Polish"],
    "Sports": ["Basketball", "Football", "Tennis Racket", "Yoga Mat", "Running Shoes"],
    "Automobile": ["Car Tire", "Motor Oil", "Brake Pads", "Car Battery", "GPS System"]
}
payment_types = ["Card", "Internet Banking", "UPI", "Wallet"]
countries = ["USA", "UK", "India", "Germany", "Australia", "Canada"]
ecommerce_sites = ["Amazon", "Flipkart", "eBay", "AliExpress", "BestBuy"]
failure_reasons = ["Insufficient Funds", "Card Expired", "Payment Gateway Timeout", "Authentication Failed"]


country_trends = {
    "USA": {"population": 331000000, "median_age": 38},
    "UK": {"population": 67000000, "median_age": 40},
    "India": {"population": 1380000000, "median_age": 28},
    "Germany": {"population": 83000000, "median_age": 45},
    "Australia": {"population": 25000000, "median_age": 37},
    "Canada": {"population": 38000000, "median_age": 41}
}
sex_distribution = ["Male", "Female"]

# Helper function to select a product name based on the category
def get_product_name(category):
    return random.choice(product_names[category])

# Generate data and store as a list of dictionaries
data = []

# Generate initial 15,000 rows of data
for i in range(num_rows):
    country = random.choice(countries)
    category = random.choice(product_categories)
    record = {
        "order_id": i + 1,
        "customer_id": i + 2,
        "customer_name": fake.name(),
        "product_id": i + 3,
        "product_name": get_product_name(category),
        "product_category": category,
        "payment_type": random.choice(payment_types),
        "qty": random.randint(5, 20),
        "price": round(random.uniform(10, 1000), 2),
        "datetime": fake.date_time_between(start_date="-1y", end_date="now"),
        "country": country,
        "city": fake.city(),
        "ecommerce_website_name": random.choice(ecommerce_sites),
        "payment_transaction_id": i + 4,
        "payment_transaction_success": "Y",
        "failure_reason": "",
        "population": country_trends[country]["population"],
        "median_age": country_trends[country]["median_age"],
        "sex": random.choice(sex_distribution)
    }
    
    # Randomly introduce bad data across columns without coupling
    if random.random() < (bad_data_rows / num_rows):
        if random.random() < 0.5:
            record["price"] = random.choice([-1.99, "NaN", "Free"])  # Bad price
        if random.random() < 0.5:
            record["qty"] = random.choice([-5, -15, 0])  # Bad quantity
        if random.random() < 0.5:
            record["country"] = random.choice(["Mars", "Atlantis", "Unknown"])  # Bad country
    
    data.append(record)

# Randomly mark 500 rows as failures independently of bad data
for i in random.sample(range(num_rows), failure_rows):
    data[i]["payment_transaction_success"] = "N"
    data[i]["failure_reason"] = random.choice(failure_reasons)

# Convert data to a DataFrame
df = spark.createDataFrame(data)

# Repartition the DataFrame to spread data across multiple partitions
df = df.repartition(10)

# Coalesce into a single partition to write as a single CSV file
df.coalesce(1).write.csv("./example7", header=True, mode="overwrite")

# Stop Spark session
spark.stop()
