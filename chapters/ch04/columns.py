"""Chapter 4, the case: Arjun's order columns, and a customer table without email addresses."""
from saffron.prepare import customers_masked, orders_with_dates
from saffron.schemas import read_table
from saffron.session import start

spark = start("ch04-columns")
orders = read_table(spark, "transactions")
customers = read_table(spark, "customers")

dated = orders_with_dates(orders)
print(dated.select("order_month", "unit_price", "order_size").first())
sizes = dated.groupBy("order_size").count().collect()
print(sorted(tuple(row) for row in sizes))
print(customers_masked(customers).columns)
spark.stop()
