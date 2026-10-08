from pyspark.sql import functions as F

silver_path = "/Volumes/retail_catalog/retail/silver/orders"
gold_path = "/Volumes/retail_catalog/retail/gold/daily_revenue"

silver = spark.read.format("delta").load(silver_path)

gold = (
    silver
    .groupBy(F.to_date("order_ts").alias("order_date"))
    .agg(
        F.countDistinct("order_id").alias("orders"),
        F.countDistinct("customer_id").alias("customers"),
        F.sum("quantity").alias("units"),
        F.sum("revenue").alias("revenue"),
        F.avg("revenue").alias("average_order_value")
    )
)

(
    gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_path)
)
