"""Chapter 3, the case: which lines of a script start jobs, and where the Spark UI shows them."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("ch03-lazy")
orders = read_table(spark, "transactions")
chain = (orders.filter(F.col("order_status") == "Delivered")
         .select("order_id", "order_date", "total_amount")
         .withColumn("big_order", F.col("total_amount") >= 5000))
chain.explain()
chain.show(3)
print("Delivered orders:", chain.count())
input("Spark UI at http://localhost:4040. Press Enter to stop. ")
spark.stop()
