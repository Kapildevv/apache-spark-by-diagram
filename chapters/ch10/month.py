"""Chapter 10: revenue by month on the bigger table: its plan and its partitions."""
import pyspark.sql.functions as F

from saffron.bigger import bigger_orders
from saffron.session import start

spark = start("ch10-month")


def revenue_by_month(df):
    return (df.groupBy("order_month")
            .agg(F.count("*").alias("orders"),
                 F.round(F.sum("total_amount"), 2).alias("revenue")))


month = revenue_by_month(bigger_orders(spark, 10000))
month.explain()
print("AQE on:", month.rdd.getNumPartitions())
spark.conf.set("spark.sql.adaptive.enabled", "false")
print("AQE off:", revenue_by_month(bigger_orders(spark, 10000))
      .rdd.getNumPartitions())

# More checks, not printed in the chapter: the plan of the first query after it ran with AQE on.
print("--- more checks")
month.explain()
spark.stop()
