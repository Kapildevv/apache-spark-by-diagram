"""P10.2: count the exchanges in the plan of revenue by state and month, before running it: once with the
helpers (latest_orders and customers_with_state, which hold a window each), once with the night 1 tables."""
import contextlib
import io
import re

import pyspark.sql.functions as F

from saffron.prepare import customers_with_state, latest_orders, orders_with_dates
from saffron.schemas import read_table
from saffron.session import start

spark = start("p10-2-exchanges")


def plan(df):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        df.explain()
    return out.getvalue()


def report(orders, states):
    return (orders_with_dates(orders).join(states, "customer_id")
            .groupBy("state", "order_month").agg(F.round(F.sum("total_amount"), 2).alias("revenue")))


def count_exchanges(label, df):
    text = plan(df)
    print(label, "| shuffle exchanges:", len(re.findall(r"(?<![A-Za-z])Exchange hashpartitioning", text)),
          "| broadcast exchanges:", text.count("BroadcastExchange"))


states = customers_with_state(spark).select("customer_id", F.col("meta.address_state").alias("state"))
count_exchanges("helpers", report(latest_orders(spark), states))

night1_states = (read_table(spark, "customers")
                 .select("customer_id", F.from_json("customer_metadata", "address_state STRING").alias("meta"))
                 .select("customer_id", F.col("meta.address_state").alias("state")))
count_exchanges("night 1 tables", report(read_table(spark, "transactions"), night1_states))
spark.stop()
