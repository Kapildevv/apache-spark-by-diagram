"""P6.4: running revenue totals with the default (RANGE) frame and a ROWS frame, and ranks of one day."""
import pyspark.sql.functions as F
from pyspark.sql import Window

from saffron.prepare import latest_orders, orders_with_dates
from saffron.session import start

spark = start("p6-4-running-totals")
delivered = orders_with_dates(latest_orders(spark)).filter("order_status = 'Delivered'")

by_day = Window.orderBy("order_day")
by_row = (Window.orderBy("order_day", "order_id")
          .rowsBetween(Window.unboundedPreceding, Window.currentRow))
within_day = Window.partitionBy("order_day").orderBy(F.desc("total_amount"))
totals = (delivered.withColumn("run_range", F.round(F.sum("total_amount").over(by_day), 2))
          .withColumn("run_rows", F.round(F.sum("total_amount").over(by_row), 2))
          .withColumn("rank", F.rank().over(within_day))
          .withColumn("dense_rank", F.dense_rank().over(within_day)))

day = totals.filter("order_day = '2026-04-11'")
day.select("order_id", "total_amount", "run_range", "run_rows").orderBy("order_id").show()
day.select("order_id", "total_amount", "rank", "dense_rank").orderBy("rank", "order_id").show()
spark.stop()
