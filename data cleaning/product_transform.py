from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("replace_null_products").getOrCreate()

input_path = "./final_data.csv"

input_df = spark.read.option("header", "true").csv(input_path)

# Product_ID to Product mapping as DataFrame
product_id_to_product_data = [
    ('Classic Wooden Bed', 1),
    ('Memory Foam Mattress', 2),
    ('L-Shaped Sofa', 3),
    ('Recliner Chair', 4),
    ('Oak Dining Table', 5),
    ('Leather Dining Chair', 6),
    ('Wooden Coffee Table', 7),
    ('Modular Bookcase', 8),
    ('Ergonomic Office Chair', 9),
    ('Metal Bed Frame', 10),
    ('Vanity Dresser', 11),
    ('Upholstered Bench', 12),
    ('Glass Console Table', 13),
    ('TV Stand', 14),
    ('Outdoor Patio Set', 15),
    ('Garden Lounge Chair', 16),
    ('Kids Bunk Bed', 17),
    ('Nursery Rocking Chair', 18),
    ('Shoe Storage Cabinet', 19),
    ('Standing Desk', 20),
    ('Hammock', 22)
]

product_id_to_product_schema = StructType([
    StructField("product_name", StringType(), True),
    StructField("product_lookup", IntegerType(), True)
])

product_id_to_product_df = spark.createDataFrame(product_id_to_product_data, schema=product_id_to_product_schema)

# Join original DataFrame with product_id-to-product mapping DataFrame
df_with_product = input_df.join(product_id_to_product_df, on='product_name', how='left')

# Replace null values in 'product' column with values from 'product_lookup'
df_final = df_with_product.withColumn(
    'product_id',
    F.coalesce(df_with_product['product_id'], df_with_product['product_lookup'])
).drop('product_lookup')

# Show the result
# df_final.show()

# Write the final DataFrame to a CSV file
output_path = "./absolute_final_data.csv"  # replace with your desired path
df_final.write.mode("overwrite").option("header", "true").csv(output_path)