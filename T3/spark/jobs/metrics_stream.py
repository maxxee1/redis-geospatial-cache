import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, expr, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import json, time                     
import urllib.request
import urllib.error   

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC_METRICS", "metrics-topic")

ES_HOST = "elasticsearch"
ES_PORT = "9200"
ES_INDEX = "spark-metrics"
ES_URL = "http://elasticsearch:9200"

spark = SparkSession.builder.appName("metrics-stream").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sparkContext.setLogLevel("WARN")


def wait_for_es(max_retries=30, delay=2):
    for i in range(max_retries):
        try:
            resp = urllib.request.urlopen(f"{ES_URL}/_cluster/health", timeout=5)
            health = json.loads(resp.read().decode())
            if health.get("status") in ("green", "yellow"):
                print(f"[ES] Cluster health: {health['status']}")
                return True
        except Exception as e:
            print(f"[ES] Esperando ES ({i+1}/{max_retries}): {e}")
        time.sleep(delay)
    raise RuntimeError("Elasticsearch no respondio a tiempo")


def create_es_index():
    mapping = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {"properties": {
            "window_start": {"type": "date"},
            "window_end": {"type": "date"},
            "throughput_per_min": {"type": "long"},
            "p50_ms": {"type": "double"},
            "p95_ms": {"type": "double"},
            "hit_rate": {"type": "double"},
            "retry_rate": {"type": "double"},
        }}
    }
    data = json.dumps(mapping).encode("utf-8")
    req = urllib.request.Request(f"{ES_URL}/{ES_INDEX}", data=data, method="PUT",
                                 headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req)
        print(f"[ES] Indice '{ES_INDEX}' creado")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        if "resource_already_exists_exception" in body:
            print(f"[ES] Indice '{ES_INDEX}' ya existe, OK")
        else:
            print(f"[ES] Error creando indice: {e.code} - {body}")


wait_for_es()
create_es_index()


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
        schema
    ).alias("d")
).select("d.*")

events = parsed.withColumn("event_time", col("ts").cast("timestamp"))

agg = (events
       .withWatermark("event_time", "2 minutes")
       .groupBy(window(col("event_time"), "1 minute", "30 seconds"))
       .agg(
           count("*").alias("throughput_per_min"),
           expr("percentile_approx(latency_ms, 0.5)").alias("p50_ms"),
           expr("percentile_approx(latency_ms, 0.95)").alias("p95_ms"),
           avg(when(col("cache") == "HIT", 1.0).otherwise(0.0)).alias("hit_rate"),
           avg(when(col("retry_count") > 0, 1.0).otherwise(0.0)).alias("retry_rate"),
       )
       .select(
           col("window.start").alias("window_start"),
           col("window.end").alias("window_end"),
           "throughput_per_min", "p50_ms", "p95_ms", "hit_rate", "retry_rate",
       ))


def write_to_es(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return
    for row in rows:
        doc = row.asDict()
        doc["window_start"] = doc["window_start"].isoformat()
        doc["window_end"] = doc["window_end"].isoformat()
        doc_id = (doc["window_start"].replace(":", "-").replace("+", "p")
                  + "_" + doc["window_end"].replace(":", "-").replace("+", "p"))
        data = json.dumps(doc).encode("utf-8")
        req = urllib.request.Request(f"{ES_URL}/{ES_INDEX}/_doc/{doc_id}",
                                     data=data, method="PUT",
                                     headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req)
        except Exception as e:
            print(f"[ES] Error indexando doc {doc_id}: {e}")
    print(f"[ES] Batch {batch_id}: indexados {len(rows)} documentos")

q = (agg.writeStream
     .foreachBatch(write_to_es)
     .outputMode("update")
     .option("checkpointLocation", "/tmp/spark-checkpoints/metrics")
     .trigger(processingTime="10 seconds")
     .start())
q.awaitTermination()