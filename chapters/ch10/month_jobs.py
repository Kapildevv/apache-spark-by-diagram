"""Chapter 10: revenue by month on the bigger table with AQE off and then on; the Spark UI stays open."""
import pyspark.sql.functions as F

from saffron.bigger import bigger_orders
from saffron.session import start

spark = start("ch10-month-jobs")


def revenue_by_month(df):
    return df.groupBy("order_month").agg(F.count("*").alias("orders"),
                                         F.round(F.sum("total_amount"), 2).alias("revenue"))


for aqe in ("false", "true"):
    spark.conf.set("spark.sql.adaptive.enabled", aqe)
    revenue_by_month(bigger_orders(spark, 10000)).collect()
input("Spark UI at http://localhost:4040. Press Enter to stop. ")
spark.stop()
