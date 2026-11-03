"""P4.1: delivered orders with quantity at least 3, as order_id and amount."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("p4-1-select-filter")
orders = read_table(spark, "transactions")

wanted = (orders
          .filter((F.col("order_status") == "Delivered") & (F.col("quantity") >= 3))
          .select("order_id", "quantity", "total_amount")
          .withColumnRenamed("total_amount", "amount")
          .drop("quantity"))
print(wanted.count(), wanted.columns)
spark.stop()
