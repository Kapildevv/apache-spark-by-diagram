"""P4.3: product_id.cast("int") crashes on the orders; try_cast gives NULL; extract the digits first."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("p4-3-fix-cast")
orders = read_table(spark, "transactions")
products = read_table(spark, "products")

try:
    orders.select(F.col("product_id").cast("int")).collect()
except Exception as err:
    print(type(err).__name__, str(err).splitlines()[0][:90])

tried = products.filter(F.col("product_id").try_cast("int").isNull())
print("NULL with try_cast:", tried.count(), "of", products.count())

digits = F.regexp_extract("product_id", r"\d+", 0).cast("int")
numbered = products.withColumn("product_num", digits)
print(numbered.filter(F.col("product_id") == "PROD-5001").select("product_id", "product_num").first())
spark.stop()
