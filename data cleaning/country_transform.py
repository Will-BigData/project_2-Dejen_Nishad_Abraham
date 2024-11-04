from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize SparkSession
spark = SparkSession.builder.appName("replace_null_countries").getOrCreate()

input_path = "./furniture_data.csv"

input_df = spark.read.option("header", "true").csv(input_path)

# Sample data
# data = [
#     ('Phoenix', None),
#     ('Chicago', 'USA'),
#     ('Los Angeles', 'USA'),
#     ('Melbourne', None),
#     ('Frankfurt', None),
#     ('Cologne', None),
#     ('Vancouver', None),
#     ('Montreal', None),
#     ('Calgary', None),
#     ('Toronto', None),
#     ('Berlin', None),
#     ('Munich', None),
#     ('Perth', None),
#     ('Adelaide', None)
# ]

# schema = StructType([
#     StructField("city", StringType(), True),
#     StructField("country", StringType(), True)
# ])

# df = spark.createDataFrame(data, schema=schema)

# City to Country mapping as DataFrame
city_to_country_data = [
    ('Phoenix', 'USA'),
    ('Chicago', 'USA'),
    ('Los Angeles', 'USA'),
    ('Melbourne', 'Australia'),
    ('Frankfurt', 'Germany'),
    ('Cologne', 'Germany'),
    ('Vancouver', 'Canada'),
    ('Montreal', 'Canada'),
    ('Calgary', 'Canada'),
    ('Toronto', 'Canada'),
    ('Berlin', 'Germany'),
    ('Munich', 'Germany'),
    ('Perth', 'Australia'),
    ('Adelaide', 'Australia')
]

city_to_country_schema = StructType([
    StructField("city", StringType(), True),
    StructField("country_lookup", StringType(), True)
])

city_to_country_df = spark.createDataFrame(city_to_country_data, schema=city_to_country_schema)

# Join original DataFrame with city-to-country mapping DataFrame
df_with_country = input_df.join(city_to_country_df, on='city', how='left')

# Replace null values in 'country' column with values from 'country_lookup'
df_final = df_with_country.withColumn(
    'country',
    F.coalesce(df_with_country['country'], df_with_country['country_lookup'])
).drop('country_lookup')

# Show the result
# df_final.show()

# Write the final DataFrame to a CSV file
output_path = "./final_data.csv"  # replace with your desired path
df_final.write.mode("overwrite").option("header", "true").csv(output_path)






