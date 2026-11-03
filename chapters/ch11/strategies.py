"""Chapter 11: which strategy Spark picks for a join, how hints and settings change it, and what AQE changes."""
import contextlib
import io
import re

import pyspark.sql.functions as F

from saffron.bigger import bigger_orders
from saffron.schemas import read_table
from saffron.session import start

spark = start("ch11-strategies")
orders, products = (read_table(spark, "transactions"),
                    read_table(spark, "products"))


def plan(df):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        df.explain()
    return re.sub(r"#\d+L?|plan_id=\d+", "", out.getvalue())


def shape(label, df):
    text = plan(df)
    join = re.search(r"\w+Join", text).group()
    print(f"{label:<22}{join:<19}"
          f"{text.count('Exchange hashpartitioning')} shuffles, "
          f"{text.count('Sort [')} sorts")


shape("defaults", orders.join(products, "product_id"))
shape("merge hint", orders.join(products.hint("merge"), "product_id"))
shape("shuffle_hash hint",
      orders.join(products.hint("shuffle_hash"), "product_id"))
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
shape("threshold -1", orders.join(products, "product_id"))
shape("broadcast hint, -1",
      orders.join(products.hint("broadcast"), "product_id"))
# the default again
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")

big = bigger_orders(spark, 10000)
busy = (big.groupBy("product_id").agg(F.count("*").alias("orders"))
        .filter("orders > 130000"))
revenue = (big.join(busy, "product_id").groupBy("product_id")
           .agg(F.sum("total_amount")))

# findall picks out the word isFinalPlan=... and the name of every
# join node in the plan
print(re.findall(r"isFinalPlan=\w+|\w+Join(?= \[)", plan(revenue)))
revenue.collect()
print(re.findall(r"isFinalPlan=\w+|\w+Join(?= \[)", plan(revenue)))

# More checks, not printed in the chapter.
print("--- more checks")
print(plan(orders.join(products, "product_id")).count("BroadcastExchange"))
orders.join(products, "product_id").explain("cost")
print(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"), spark.conf.get("spark.sql.join.preferSortMergeJoin"))
print(spark.sparkContext.broadcast([1, 2, 3]).value)
spark.stop()
