"""Chapter 6, windows: the newest version of an order, ranks of customers, running totals."""
import pyspark.sql.functions as F
from pyspark.sql import Window

from saffron.prepare import latest_orders, orders_both_nights, orders_with_dates
from saffron.schemas import read_table
from saffron.session import start

spark = start("ch06-windows")
both = orders_both_nights(spark)
newest = Window.partitionBy("order_id").orderBy(F.col("changed_at").desc(), F.col("night").desc())
numbered = both.withColumn("rn", F.row_number().over(newest))
numbered.filter("order_id = 'ORD-90010'").select(
    "order_id", "night", "changed_at", "order_status", "rn").show()

orders = orders_with_dates(read_table(spark, "transactions"))
counts = orders.groupBy("customer_id").agg(F.count("*").alias("n"))
by_n = Window.orderBy(F.desc("n"))
ranked = counts.select("customer_id", "n", F.rank().over(by_n).alias("rank"),
                       F.dense_rank().over(by_n).alias("dense_rank"))
ranked.filter("dense_rank <= 3").orderBy("rank", "customer_id").show()

delivered = orders_with_dates(latest_orders(spark)).filter("order_status = 'Delivered'")
ordered = Window.orderBy("order_day", "order_id")
by_row = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
by_day = Window.orderBy("order_day")
totals = (delivered.withColumn("run_range", F.sum("total_amount").over(by_day))
          .withColumn("run_rows", F.sum("total_amount").over(by_row))
          .withColumn("prev", F.lag("total_amount").over(ordered)))
cols = ["order_id", "total_amount", "run_range", "run_rows", "prev"]
totals.filter("order_day = '2026-04-01'").orderBy("order_id").select(cols).show()

revenue = F.round(F.sum("total_amount"), 2).alias("revenue")
print(delivered.agg(F.count("*").alias("orders"), revenue).first())
spark.stop()
