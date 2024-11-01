
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ECommerceDataCleaning").getOrCreate()

# Load data from HDFS
df = spark.read.csv("hdfs://localhost:9000/user/xxxx/contains_bad_data.csv", header=True, inferSchema=True)

df.printSchema()
df.show(5)

