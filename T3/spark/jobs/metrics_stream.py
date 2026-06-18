import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC_METRICS", "metrics-topic")

spark = SparkSession.builder.appName("metrics-stream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "latest")
       .load())


# ==== estructura del JSON ==== #
schema = StructType([
    StructField("ts", DoubleType()),
    StructField("query_type", StringType()),
    StructField("cache", StringType()),
    StructField("latency_ms", DoubleType()),
    StructField("retry_count", IntegerType()),
    StructField("status", StringType())
])

parsed = raw.select(
    from_json(
        col("value").cast("string"), 
        schema).
     alias("d")
).select("d.*")

q = (parsed.writeStream.format("console").option("truncate", False).start())

q.awaitTermination()
