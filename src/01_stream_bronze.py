from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import os

# Kafka/Event Hubs values should be injected through Databricks job parameters/secrets.
bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "<namespace>.servicebus.windows.net:9093")
topic = os.environ.get("KAFKA_TOPIC", "retail-orders")
checkpoint = os.environ.get(
    "BRONZE_CHECKPOINT",
    "/Volumes/retail_catalog/retail/checkpoints/orders_bronze"
)
output = os.environ.get(
    "BRONZE_PATH",
    "/Volumes/retail_catalog/retail/bronze/orders"
)

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

bronze = (
    raw.select(
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("payload"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp")
    )
    .withColumn("ingestion_timestamp", F.current_timestamp())
)

query = (
    bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .start(output)
)

query.awaitTermination()
