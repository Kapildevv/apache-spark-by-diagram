"""Chapter 7, joins: join types, NULLs from outer joins, semi and anti joins, conditions, fan-out."""
import pyspark.sql.functions as F

from saffron.prepare import latest_orders, latest_rewards, orders_both_nights
from saffron.schemas import read_table
from saffron.session import start

spark = start("ch07-joins")

# Join types: orders (750) against the latest rewards (391).
orders = latest_orders(spark)
rewards = (latest_rewards(spark)
           .select("order_id", "cashback_amount", "status"))
for how in ["inner", "left", "right", "full"]:
    print(how, orders.join(rewards, "order_id", how).count())

# NULLs from a left join, and F.coalesce to fill them.
paid = orders.join(rewards, "order_id", "left")
filled = paid.withColumn(
    "cashback", F.coalesce("cashback_amount", F.lit(0.0)))
print(filled.filter("cashback_amount IS NULL").count(),
      filled.filter("cashback IS NULL").count())

# Semi and anti joins.
customers = read_table(spark, "customers").select("customer_id")
payments = (read_table(spark, "pg_data")
            .select("order_id", "payment_status"))
print(customers.join(orders, "customer_id", "left_anti").count())
failed_orders = orders.join(
    payments.filter("payment_status = 'Failed'"),
    "order_id", "left_semi")
print(customers.join(failed_orders, "customer_id", "left_semi")
      .count())
print(customers.join(failed_orders, "customer_id").count())

# A condition, DataFrame aliases, and keys with different names.
o, p = orders.alias("o"), payments.alias("p")
by_cond = o.join(p, F.col("o.order_id") == F.col("p.order_id"))
print(by_cond.select("o.order_id", "p.payment_status").count(),
      by_cond.columns.count("order_id"))
sku = (read_table(spark, "products")
       .withColumnRenamed("product_id", "sku"))
print(orders.join(sku, orders.product_id == sku.sku).count())

# Fan-out: the second table repeats the key.
stacked = orders_both_nights(spark).select("order_id", "night")
repeats = (stacked.groupBy("order_id").count()
           .filter("count > 1").count())
print(orders.count(), repeats,
      orders.join(stacked, "order_id").count())
print(orders.join(stacked, ["order_id", "night"]).count())
spark.stop()
