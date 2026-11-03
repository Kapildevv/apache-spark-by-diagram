"""Chapter 4, casts and text: cast, try_cast, regexp_extract, regexp_replace."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("ch04-casts-text")
orders = read_table(spark, "transactions")
customers = read_table(spark, "customers")

try:
    orders.select(F.col("customer_id").cast("int")).collect()
except Exception as err:
    print(type(err).__name__, str(err).splitlines()[0])
numbers = F.col("customer_id").try_cast("int")
print(orders.filter(numbers.isNull()).count())

digits = F.regexp_extract("customer_id", r"\d+", 0).cast("int")
print(orders.select("customer_id", digits.alias("n")).first())

mask = F.regexp_replace("email", r"^(.)[^@]*@", "$1***@")
print(customers.select(mask).first()[0])
spark.stop()
