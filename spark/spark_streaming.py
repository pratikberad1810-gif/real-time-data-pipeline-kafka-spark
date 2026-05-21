from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("RealTimePipeline") \
    .getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("city", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", DoubleType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

clean_df = parsed_df.filter(col("amount") > 0)

query = clean_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
