"""Chapter 4, dates and labels: date_format, to_timestamp, when/otherwise."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("ch04-dates-labels")
orders = read_table(spark, "transactions")

fmt = "dd/MM/yyyy HH:mm"
texty = orders.withColumn("placed", F.date_format("order_date", fmt))
back = texty.withColumn("placed_ts", F.to_timestamp("placed", fmt))
print(back.select("placed", "placed_ts").first())

size = (F.when(F.col("total_amount") < 1000, "small")
        .when(F.col("total_amount") < 5000, "medium")
        .otherwise("large"))
print(orders.select("total_amount", size.alias("order_size")).first())
spark.stop()
