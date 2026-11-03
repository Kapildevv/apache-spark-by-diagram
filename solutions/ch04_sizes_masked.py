"""P4.2: order_hour and order_size, the orders per size, and three masked emails."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("p4-2-sizes-masked")
orders = read_table(spark, "transactions")
customers = read_table(spark, "customers")

sized = (orders
         .withColumn("order_hour", F.hour("order_date"))
         .withColumn("order_size",
                     F.when(F.col("total_amount") < 1000, "small")
                      .when(F.col("total_amount") < 5000, "medium")
                      .otherwise("large")))
rows = sized.groupBy("order_size").count().collect()
print(sorted(tuple(row) for row in rows))

mask = (F.regexp_replace("email", r"^(.)[^@]*@", "$1***@")
        .alias("email_masked"))
print([row.email_masked for row in customers.select(mask).take(3)])
spark.stop()
