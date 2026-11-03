"""P11.1: predict the join strategy for the orders and the payments (750 rows each), then check the plan."""
import contextlib
import io
import re

from saffron.schemas import read_table
from saffron.session import start

spark = start("p11-1-strategy")
orders, payments = read_table(spark, "transactions"), read_table(spark, "pg_data")


def plan(df):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        df.explain()
    return out.getvalue()


text = plan(orders.join(payments, "order_id"))
print(re.findall(r"\w+Join(?= \[)", text), re.findall(r"Build(?:Left|Right)", text))
print("shuffle exchanges:", text.count("Exchange hashpartitioning"), "| broadcast exchanges:", text.count("BroadcastExchange"))
spark.stop()
