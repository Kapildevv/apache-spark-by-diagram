"""P7.1: delivered orders and revenue for each product category, latest orders joined to products;
then the four join types between the latest orders and the customers."""
import pyspark.sql.functions as F

from saffron.prepare import latest_orders
from saffron.schemas import read_table
from saffron.session import start

spark = start("p7-1-category-revenue")
products = (read_table(spark, "products")
            .select("product_id", "category"))
delivered = latest_orders(spark).filter("order_status = 'Delivered'")
by_category = (delivered.join(products, "product_id")
               .groupBy("category")
               .agg(F.count("*").alias("orders"),
                    F.round(F.sum("total_amount"), 2)
                    .alias("revenue")))
by_category.orderBy("category").show(truncate=False)
by_category.agg(F.sum("orders").alias("orders"), F.round(F.sum("revenue"), 2).alias("revenue")).show()

orders = (latest_orders(spark)
          .select("order_id", "customer_id", "total_amount"))
customers = (read_table(spark, "customers")
             .select("customer_id", "first_name"))
for how in ("inner", "left", "right", "full"):
    joined = orders.join(customers, "customer_id", how)
    print(how, joined.count(), "rows |",
          joined.filter(F.col("order_id").isNull()).count(),
          "without an order |",
          joined.filter(F.col("first_name").isNull()).count(),
          "without a first name")
no_orders = orders.join(customers, "customer_id", "right").filter(F.col("order_id").isNull())
print(sorted(row.customer_id for row in no_orders.collect()))
spark.stop()
