"""P9.3: the four sections of explain("extended") and the scan lines of explain("formatted")."""
import contextlib
import io
import re

from saffron.schemas import read_table
from saffron.session import start

spark = start("p9-3-explain-filtered")
read_table(spark, "transactions").createOrReplaceTempView("orders")
q = spark.sql("SELECT order_id, total_amount FROM orders WHERE order_status = 'Delivered' AND total_amount > 1000")


def plan(df, mode):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        df.explain(mode=mode)
    return out.getvalue()


print(re.findall(r"== (.+?) ==", plan(q, "extended")))
print([line for line in plan(q, "formatted").splitlines()
       if line.startswith(("PushedFilters", "ReadSchema"))])
spark.stop()
