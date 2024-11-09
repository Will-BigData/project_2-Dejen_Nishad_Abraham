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

# Population, median age, and gender distribution data
country_trends = {
    "USA": {"population": 331000000, "median_age": 38},
    "UK": {"population": 67000000, "median_age": 40},
    "India": {"population": 1380000000, "median_age": 28},
    "Germany": {"population": 83000000, "median_age": 45},
    "Australia": {"population": 25000000, "median_age": 37},
    "Canada": {"population": 38000000, "median_age": 41}
}
gender_distribution = ["Male", "Female"]

# Define top-selling categories, products, and trends
top_selling_category = "Electronics"
popular_product = "Smartphone"
second_top_selling_category = "Clothing"
second_popular_product = "Jacket"
third_top_selling_category = "Beauty"
third_popular_product = "Perfume"
peak_location = "New York"
peak_hour = 15

# Helper function to select a product name based on the category
def get_product_name(category):
    return random.choice(product_names[category])

# Generate data and store as a list of dictionaries
data = []

for i in range(num_rows):
    country = random.choice(countries)
    category = random.choice(product_categories)
    product = get_product_name(category)
    record = {
        "order_id": i + 1,
        "customer_id": i + 2,
        "customer_name": fake.name(),
        "product_id": i + 3,
        "product_name": product,
        "product_category": category,
        "payment_type": random.choice(payment_types),
        "qty": random.randint(1, 10),
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
        "gender": random.choice(gender_distribution)
    }
    
    # Apply trend skew for top-selling categories and popular products
    if category == top_selling_category:
        record["qty"] *= 2
        if product == popular_product:
            record["qty"] *= 2
    elif category == second_top_selling_category:
        record["qty"] *= 2
        if product == second_popular_product:
            record["qty"] *= 2
    elif category == third_top_selling_category:
        record["qty"] *= 2
        if product == third_popular_product:
            record["qty"] *= 2

    # Apply skew for peak location and time of day
    if record["city"] == peak_location and record["datetime"].hour == peak_hour:
        record["qty"] *= 3 

    # Randomly introduce bad data across columns
    if random.random() < (bad_data_rows / num_rows):
        if random.random() < 0.5:
            record["price"] = random.choice([-1.99, "NaN", "Free"])  # Invalid price entries
        if random.random() < 0.5:
            record["qty"] = random.choice([-5, -15, 0]) 
            record["country"] = random.choice(["Mars", "Atlantis", "Unknown"])
    
    data.append(record)

# Randomly mark 500 rows as failures independently of bad data
for i in random.sample(range(num_rows), failure_rows):
    data[i]["payment_transaction_success"] = "N"
    data[i]["failure_reason"] = random.choice(failure_reasons)

# Convert data to a DataFrame
df = spark.createDataFrame(data)

# Repartition the DataFrame for parallelism
df = df.repartition(10)

# Write the data to a single CSV file
df.coalesce(1).write.csv("./ecommerce_data.csv", header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
