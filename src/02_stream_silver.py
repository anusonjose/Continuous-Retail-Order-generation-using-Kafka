from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

bronze_path = "/Volumes/retail_catalog/retail/bronze/orders"
silver_path = "/Volumes/retail_catalog/retail/silver/orders"
checkpoint = "/Volumes/retail_catalog/retail/checkpoints/orders_silver"

order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("order_ts", StringType(), False)
])

def transform(batch_df):
    parsed = (
        batch_df
        .select(
            F.from_json("payload", order_schema).alias("o"),
            "ingestion_timestamp",
            "partition",
            "offset"
        )
        .select("o.*", "ingestion_timestamp", "partition", "offset")
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
        .filter(F.col("order_id").isNotNull())
    )

    # Idempotency/deduplication at the business-key level.
    clean = parsed.dropDuplicates(["order_id"])

    clean.write.format("delta").mode("append").save(silver_path)

(
    spark.readStream
    .format("delta")
    .load(bronze_path)
    .writeStream
    .foreachBatch(lambda df, epoch_id: transform(df))
    .option("checkpointLocation", checkpoint)
    .start()
    .awaitTermination()
)
