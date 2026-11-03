"""P9.2: delivered orders and their average order value for each category, as SQL and as DataFrame code, and a check that the plans match."""
import contextlib
import io
import re

import pyspark.sql.functions as F

from saffron.prepare import latest_orders
from saffron.schemas import read_table
from saffron.session import start

spark = start("p9-2-sql-vs-dataframe")
orders = latest_orders(spark)
products = (read_table(spark, "products")
            .select("product_id", "category"))
orders.createOrReplaceTempView("orders")
products.createOrReplaceTempView("products")


def plan(df):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        df.explain()
    return re.sub(r"#\d+L?|plan_id=\d+", "", out.getvalue())


from_sql = spark.sql("""
    SELECT category, COUNT(*) AS delivered,
           ROUND(AVG(total_amount), 2) AS average_order
    FROM orders JOIN products USING (product_id)
    WHERE order_status = 'Delivered'
    GROUP BY category ORDER BY category""")
from_df = (orders.filter("order_status = 'Delivered'")
           .join(products, "product_id")
           .groupBy("category")
           .agg(F.count("*").alias("delivered"),
                F.round(F.avg("total_amount"), 2)
                .alias("average_order"))
           .orderBy("category"))
sql_plan, df_plan = plan(from_sql), plan(from_df)
print(sql_plan == df_plan, from_sql.collect() == from_df.collect())
from_sql.show(truncate=False)
spark.stop()
