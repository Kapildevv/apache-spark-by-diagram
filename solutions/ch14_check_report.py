"""P14.2: check the report against chapters 6 and 7, and name the join strategies of its final plan."""
import contextlib
import io
import re
import sys

import pyspark.sql.functions as F

from saffron.paths import OUTPUT, ROOT
from saffron.session import start

sys.path.insert(0, str(ROOT / "apps"))
from monthly_report import enrich, summarise  # noqa: E402

spark = start("p14-2-check")
written = spark.read.parquet(str(OUTPUT / "monthly_report"))  # the folder the spark-submit run wrote
totals = written.agg(F.sum("delivered_orders"), F.round(F.sum("revenue"), 2)).first()
print(f"delivered {totals[0]}, revenue {totals[1]:,.2f}")  # chapters 6 and 7: 619 and 2,790,726.95

report = summarise(enrich(spark, str(ROOT / "data" / "raw")))
report.collect()  # run the query itself, so that explain() prints the plan that ran
buffer = io.StringIO()
with contextlib.redirect_stdout(buffer):
    report.explain()
final = re.sub(r"#\d+L?", "", buffer.getvalue().split("== Initial Plan ==")[0])  # drop expression ids
for key, kind, side in re.findall(r"BroadcastHashJoin \[(\w+)\], \[\w+\], (\w+), (\w+)", final):
    print("BroadcastHashJoin on", key, kind, side)  # product_id: products; order_id: payments, then rewards
print("other join strategies:", sorted(set(re.findall(r"(SortMergeJoin|ShuffledHashJoin)", final))))
spark.stop()
