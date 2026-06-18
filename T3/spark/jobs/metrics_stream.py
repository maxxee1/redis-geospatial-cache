import os
from pyspark.sql import SparkSession

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC_METRICS", "metrics-topic")

spark = SparkSession.builder.appName("metrics-stream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "latest")
       .load())

q = (raw.selectExpr("CAST(value AS STRING) AS value")
     .writeStream
     .format("console")
     .option("truncate", False)
     .start())

q.awaitTermination()