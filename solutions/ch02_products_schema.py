"""P2.1: read products.csv with header and inferSchema, then the orders by a name pattern."""
from saffron.paths import raw, raw_folder
from saffron.session import start

spark = start("ch02-products-schema")

products = (spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(raw("products")))
products.printSchema()
print(products.dtypes)
print(len(products.columns), "columns")

orders = spark.read.option("header", True).csv(raw_folder() + "/transactions*.csv")
print(orders.count(), "orders")

spark.stop()
