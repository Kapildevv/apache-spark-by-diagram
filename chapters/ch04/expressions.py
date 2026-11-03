"""Chapter 4, columns as expressions: select, filter, withColumn, withColumnRenamed, drop."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("ch04-expressions")
orders = read_table(spark, "transactions")

big = F.col("total_amount") >= 5000
print(big)
not_delivered = orders.filter(
    ~(F.col("order_status") == "Delivered") & big)
slim = not_delivered.select("order_id",
                            F.col("total_amount").alias("amount"))
print(slim.count(), slim.columns)

tidy = (orders
        .withColumn(
            "unit_price",
            F.round(F.col("total_amount") / F.col("quantity"), 2))
        .withColumnRenamed("total_amount", "amount")
        .drop("customer_id", "product_id"))
print(tidy.columns)
spark.stop()
