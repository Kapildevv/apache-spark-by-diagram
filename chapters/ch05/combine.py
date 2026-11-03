"""Chapter 5, the case: both nights of orders in one table, and how many orders changed."""
import pyspark.sql.functions as F

from saffron.prepare import orders_both_nights
from saffron.session import start

spark = start("ch05-combine")
both = orders_both_nights(spark)
versions = both.drop("night").distinct()
kept = both.dropDuplicates(["order_id"])
print(both.count(), versions.count(), kept.count(),
      versions.count() - kept.count())
print(kept.filter(F.col("order_status") == "In-Transit").count())
spark.stop()
