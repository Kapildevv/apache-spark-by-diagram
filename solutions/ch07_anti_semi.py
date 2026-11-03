"""P7.2: the orders with no reward (anti join) and the customers with a failed payment (semi join)."""
from saffron.prepare import latest_orders, latest_rewards
from saffron.schemas import read_table
from saffron.session import start

spark = start("p7-2-anti-semi")
orders = latest_orders(spark)
rewards = latest_rewards(spark)
print(orders.join(rewards, "order_id", "left_anti").count())

customers = read_table(spark, "customers").select("customer_id")
payments = (read_table(spark, "pg_data")
            .select("order_id", "payment_status"))
failed_orders = orders.join(
    payments.filter("payment_status = 'Failed'"),
    "order_id", "left_semi")
print(customers.join(failed_orders, "customer_id", "left_semi")
      .count())
spark.stop()
