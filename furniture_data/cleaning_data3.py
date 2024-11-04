# this code replace all the bad datas

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, first
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CityCountryProductMapping") \
    .getOrCreate()


df = spark.read.csv("./furniture_data.csv", header=True, inferSchema=True)

city_country_data = [
    ("New York", "USA"),
    ("Toronto", "Canada"),
    ("London", "UK"),
    ("Berlin", "Germany"),
    ("Sydney", "Australia"),
    ("Los Angeles", "USA"),
    ("Vancouver", "Canada"),
    ("Melbourne", "Australia"),
    ("Adelaide", "Australia"),
    ("Calgary", "Canada"),
    ("Chicago", "USA"),
    ("Cologne", "Germany"),
    ("Frankfurt", "Germany"),
    ("Montreal", "Canada"),
    ("Munich", "Germany"),
    ("Perth", "Australia"),
    ("Phoenix", "USA")
]

city_country_df = spark.createDataFrame(city_country_data, ["city", "correct_country"])

# Join Main Data with City-Country Mapping to Correct Country Values
df = df.join(city_country_df.withColumnRenamed("correct_country", "mapped_country"), on="city", how="left")

# Replace the `country` column with `mapped_country` if `country` is null or invalid
valid_countries = ["USA", "Canada", "UK", "Germany", "Australia"]
df = df.withColumn("country", when((col("country").isNull()) | (~col("country").isin(valid_countries)), col("mapped_country")).otherwise(col("country")))

product_data = [
    ("Classic Wooden Bed", 1),
    ("Memory Foam Mattress", 2),
    ("L-Shaped Sofa", 3),
    ("Recliner Chair", 4),
    ("Oak Dining Table", 5),
    ("Leather Dining Chair", 6),
    ("Wooden Coffee Table", 7),
    ("Modular Bookcase", 8),
    ("Ergonomic Office Chair", 9),
    ("Metal Bed Frame", 10),
    ("Nursery Rocking Chair", 11),
    ("Outdoor Patio Set", 12),
    ("Shoe Storage Cabinet", 13),
    ("TV Stand", 14),
    ("Upholstered Bench", 15),
    ("Vanity Dresser", 16),
    ("Glass Console Table", 17),
    ("Kids Bunk Bed", 18),
    ("Garden Lounge Chair", 19),
    ("Standing Desk", 20),  
    ("Hammock", 22),
    ("Blanket", 21)  
]


product_mapping_df = spark.createDataFrame(product_data, ["product_name", "correct_product_id"])

# Join with Product Mapping and Resolve Ambiguity
df = df.join(product_mapping_df.withColumnRenamed("correct_product_id", "mapped_product_id"), on="product_name", how="left")

# Replace `product_id` with mapped values if it’s missing or inconsistent
window_spec = Window.partitionBy("product_name")
df = df.withColumn("product_id", when(col("product_id").isNull(), first("mapped_product_id", ignorenulls=True).over(window_spec)).otherwise(col("product_id")))

# Drop the intermediate columns `mapped_country` and `mapped_product_id`
df = df.drop("mapped_country", "mapped_product_id")

# Verification Query to Ensure Correctness
df.createOrReplaceTempView("cleaned_furniture_data")
verification_query = spark.sql("""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(IF(country IS NULL OR country NOT IN ('USA', 'Canada', 'UK', 'Germany', 'Australia'), 1, NULL)) AS invalid_country_count,
        COUNT(IF(product_id IS NULL, 1, NULL)) AS null_product_id_count
    FROM cleaned_furniture_data
""")
verification_query.show()

df.write.csv("hdfs://localhost:9000/user/dejtes/updated_cleaned_furniture_data.csv", header=True, mode="overwrite")

# Stop Spark session
spark.stop()
