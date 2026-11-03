"""P8.2: each customer's distinct payment methods in an array, customers with three or more, exploded back."""
import pyspark.sql.functions as F

from saffron.prepare import latest_orders
from saffron.schemas import read_table
from saffron.session import start

spark = start("p8-2-methods")
payments = (read_table(spark, "pg_data")
            .select("order_id", "payment_method"))
methods = (latest_orders(spark).select("order_id", "customer_id")
           .join(payments, "order_id")
           .groupBy("customer_id")
           .agg(F.sort_array(F.collect_set("payment_method"))
                .alias("methods")))
many = methods.filter(F.size("methods") >= 3)
print(many.count())
print(many.select("customer_id", F.explode("methods")).count())
print(methods.agg(F.max(F.size("methods"))).first()[0])
spark.stop()
