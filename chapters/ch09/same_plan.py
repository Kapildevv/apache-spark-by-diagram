"""Chapter 9, case: Arjun's category revenue as SQL and as DataFrame code, with the same result and plan."""
import contextlib, io, re

import pyspark.sql.functions as F

from saffron.prepare import latest_orders
from saffron.schemas import read_table
from saffron.session import start

spark = start("ch09-same-plan")
orders, products = latest_orders(spark), read_table(spark, "products")
orders.createOrReplaceTempView("orders")
products.createOrReplaceTempView("products")


def plan(df):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        df.explain()
    return re.sub(r"#\d+L?|plan_id=\d+", "", out.getvalue())


from_sql = spark.sql("""
    SELECT category, COUNT(*) AS orders, ROUND(SUM(total_amount), 2) AS revenue
    FROM orders JOIN products USING (product_id)
    WHERE order_status = 'Delivered' GROUP BY category ORDER BY category""")
from_df = (orders.join(products, "product_id").filter("order_status = 'Delivered'")
           .groupBy("category")
           .agg(F.count("*").alias("orders"), F.round(F.sum("total_amount"), 2).alias("revenue"))
           .orderBy("category"))
sql_plan, df_plan = plan(from_sql), plan(from_df)
print(sql_plan == df_plan, from_sql.collect() == from_df.collect())
print([line.strip() for line in sql_plan.splitlines() if "Join" in line])

# More checks, not printed in the chapter.
print("--- more checks")
rows = from_sql.collect()
print(rows)
print("orders:", sum(r["orders"] for r in rows), "| revenue:", round(sum(r["revenue"] for r in rows), 2))
print(plan(from_sql)[:2200])
print("sameSemantics (logical plans):", from_sql.sameSemantics(from_df))
spark.stop()
