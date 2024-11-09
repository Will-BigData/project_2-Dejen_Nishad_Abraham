from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, trim
from pyspark.sql.types import StringType



spark = SparkSession.builder \
    .appName("CountryCleaning") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

products_path = "hdfs://localhost:9000/user/nifty/product_id_cleaned.csv"
countries_path = "hdfs://localhost:9000/user/nifty/final_cleaned_data.csv"

df_products_cleaned = spark.read.csv(products_path, header=True, inferSchema=True)

df_blank_countries_with_none = df_products_cleaned.select([when(trim(col(c)) == "", None).otherwise(col(c)).alias(c) for c in df_products_cleaned.columns])

city_country_map = (
    df_blank_countries_with_none.filter(col("country").isNotNull())
    .select("city", "country")
    .distinct()
    .rdd.collectAsMap()
)

country_city_broadcast = spark.sparkContext.broadcast(city_country_map)

def fill_country(city, country):
    if country is not None:
        return country
    return country_city_broadcast.value.get(city)

fill_country_udf = F.udf(fill_country, StringType())

df_cleaned = df_products_cleaned.withColumn(
    "country",
    fill_country_udf(F.col("city"), F.col("country"))
)

df_cleaned.write.csv(countries_path, header=True, mode="overwrite")

df_cleaned.printSchema()

spark.stop()
