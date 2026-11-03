"""P3.2: a filter whose result is thrown away, and the fix."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("p3-2-assign-filter")
orders = read_table(spark, "transactions")

orders.filter(F.col("order_status") == "Delivered")
print("before the fix:", orders.count())

delivered = orders.filter(F.col("order_status") == "Delivered")
print("after the fix:", delivered.count())

spark.stop()
