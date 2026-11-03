"""P10.1: partitions of the day-1 orders read, of spark.range(0, 100000) and of revenue by month, AQE on and off."""
import pyspark.sql.functions as F

from saffron.prepare import orders_with_dates
from saffron.schemas import read_table
from saffron.session import start

spark = start("p10-1-partitions")


def revenue_by_month(df):
    return df.groupBy("order_month").agg(F.count("*").alias("orders"),
                                         F.round(F.sum("total_amount"), 2).alias("revenue"))


orders = read_table(spark, "transactions")
print("orders:", orders.rdd.getNumPartitions())
print("range:", spark.range(0, 100000).rdd.getNumPartitions())
print("revenue by month, AQE on:", revenue_by_month(orders_with_dates(orders)).rdd.getNumPartitions())
spark.conf.set("spark.sql.adaptive.enabled", "false")
print("revenue by month, AQE off:", revenue_by_month(orders_with_dates(orders)).rdd.getNumPartitions())
spark.stop()
