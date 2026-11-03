"""Part II needs more rows than 750. These helpers make them inside Spark from the same orders."""
from pyspark.sql import functions as F

from saffron.schemas import read_table

HOT_PRODUCT = "PROD-5042"


def _day1_orders(spark):
    orders = read_table(spark, "transactions", day=1)
    return orders.withColumn("order_month", F.date_format("order_date", "yyyy-MM"))


def _with_copies(orders, spark, first, last):
    """Cross the orders with copy numbers first .. last - 1; copy k adds the suffix -k to order_id."""
    copies = spark.range(first, last).withColumnRenamed("id", "copy")
    return (F.broadcast(orders).crossJoin(copies)
            .withColumn("order_id", F.concat_ws("-", "order_id", F.col("copy").cast("string"))))


def bigger_orders(spark, copies):
    """The day-1 orders crossed with spark.range(copies): 750 * copies rows, with a copy column.
    No randomness: the same call gives the same rows."""
    return _with_copies(_day1_orders(spark), spark, 0, copies)


def flash_sale_orders(spark, copies, hot_copies):
    """The bigger table plus hot_copies EXTRA copies of product PROD-5042's orders (copies numbered
    copies .. copies + hot_copies - 1): a constructed 'flash sale' that makes one product's key far
    larger than every other key."""
    hot = _day1_orders(spark).filter(F.col("product_id") == HOT_PRODUCT)
    return bigger_orders(spark, copies).unionByName(_with_copies(hot, spark, copies, copies + hot_copies))
