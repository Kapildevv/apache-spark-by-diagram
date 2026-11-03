"""P3.3: April's delivered orders, the file scan in the plan, and a month test that is not pushed."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("p3-3-april-pushdown")
orders = read_table(spark, "transactions")

april = (orders.filter(F.col("order_status") == "Delivered")
         .filter(F.col("order_date") >= "2026-04-01")
         .filter(F.col("order_date") < "2026-05-01"))
april.explain()
print("range filter:", april.count(), "rows")

by_month = (orders.filter(F.col("order_status") == "Delivered")
            .filter(F.date_format("order_date", "yyyy-MM")
                    == "2026-04"))
by_month.explain()
print("date_format filter:", by_month.count(), "rows")

input("Spark UI at http://localhost:4040. Press Enter to stop. ")
spark.stop()
