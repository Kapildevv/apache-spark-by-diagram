"""Chapter 6, summaries: a count and a rate per group with groupBy, then a month-by-status grid."""
import pyspark.sql.functions as F

from saffron.prepare import orders_with_dates
from saffron.schemas import read_table
from saffron.session import start

spark = start("ch06-summaries")
payments = read_table(spark, "pg_data").select("gateway_name", "payment_status")
failed = F.when(F.col("payment_status") == "Failed", 1).otherwise(0)
rates = payments.groupBy("gateway_name").agg(
    F.count("*").alias("payments"),
    F.sum(failed).alias("failed"),
    F.round(F.avg(failed), 4).alias("rate"))
rates.orderBy(F.desc("rate")).show()

orders = orders_with_dates(read_table(spark, "transactions"))
statuses = ["Delivered", "Cancelled", "In-Transit"]
grid = orders.groupBy("order_month").pivot("order_status", statuses).count()
grid.orderBy("order_month").show()
spark.stop()
