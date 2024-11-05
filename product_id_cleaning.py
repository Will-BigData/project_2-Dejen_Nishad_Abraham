from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, trim
from pyspark.sql.types import StringType



spark = SparkSession.builder \
    .appName("ProductIDCleaning") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

furniture_path = "hdfs://localhost:9000/user/nifty/furniture_data.csv"
products_path = "hdfs://localhost:9000/user/nifty/product_id_cleaned.csv"

df_furniture = spark.read.csv(furniture_path, header=True, inferSchema=True)

df_furniture_with_none = df_furniture.select([when(trim(col(c)) == "", None).otherwise(col(c)).alias(c) for c in df_furniture.columns])

product_name_id_map = (
    df_furniture_with_none.filter(col("product_id").isNotNull())
    .select("product_name", "product_id")
    .distinct()
    .rdd.collectAsMap()
)

product_name_id_broadcast = spark.sparkContext.broadcast(product_name_id_map)

def fill_product_id(product_name, product_id):
    if product_id is not None:
        return product_id
    return product_name_id_broadcast.value.get(product_name)

fill_product_id_udf = F.udf(fill_product_id, StringType())

df_furniture = df_furniture.withColumn(
    "product_id",
    fill_product_id_udf(F.col("product_name"), F.col("product_id"))
)

df_furniture.write.csv(products_path, header=True, mode="overwrite")

df_furniture.printSchema()

spark.stop()
