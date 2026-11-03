"""Chapter 7, case: delivered orders, revenue and payment-failure rate for each product category."""
import pyspark.sql.functions as F

from saffron.prepare import latest_orders
from saffron.schemas import read_table
from saffron.session import start

spark = start("ch07-category-report")
products = read_table(spark, "products").select("product_id", "category")
payments = read_table(spark, "pg_data").select("order_id", "payment_status")
joined = latest_orders(spark).join(products, "product_id").join(payments, "order_id")
print(joined.count())

delivered = F.col("order_status") == "Delivered"
failed = F.col("payment_status") == "Failed"
report = joined.groupBy("category").agg(
    F.sum(F.when(delivered, 1).otherwise(0)).alias("delivered"),
    F.round(F.sum(F.when(delivered, F.col("total_amount")).otherwise(0)), 2).alias("revenue"),
    F.round(F.avg(F.when(failed, 1).otherwise(0)), 4).alias("rate"))
report.orderBy("category").show(truncate=False)
spark.stop()
