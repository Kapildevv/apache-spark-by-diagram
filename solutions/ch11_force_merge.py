"""P11.2: force a sort-merge join with a hint and with the threshold, and show both plans."""
import contextlib
import io
import re

from saffron.schemas import read_table
from saffron.session import start

spark = start("p11-2-force-merge")
orders, payments = (read_table(spark, "transactions"),
                    read_table(spark, "pg_data"))


def plan(df):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        df.explain()
    return out.getvalue()


def show(label, df):
    text = plan(df)
    joins = re.findall(r"\w+Join(?= \[)", text)
    print(f"{label:<10}{joins}  shuffles: {text.count('Exchange hashpartitioning')}, sorts: {text.count('Sort [')}")


show("hint", orders.join(payments.hint("merge"), "order_id"))
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
show("threshold", orders.join(payments, "order_id"))
spark.stop()
