from pyspark.sql import functions as F

path = "/Volumes/retail_catalog/retail/silver/orders"
df = spark.read.format("delta").load(path)

checks = {
    "null_order_id": df.filter(F.col("order_id").isNull()).count(),
    "negative_quantity": df.filter(F.col("quantity") <= 0).count(),
    "negative_revenue": df.filter(F.col("revenue") < 0).count()
}

print(checks)

if any(v > 0 for v in checks.values()):
    raise ValueError(f"Data quality checks failed: {checks}")
