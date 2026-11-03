"""Chapter 14: read the report the application wrote and check it, without rebuilding anything."""
import hashlib
import sys
from pathlib import Path

import pyspark.sql.functions as F

from saffron.paths import OUTPUT
from saffron.session import start

spark = start("ch14-check-output")
folder = Path(sys.argv[1]) if len(sys.argv) > 1 else OUTPUT / "monthly_report"
report = spark.read.parquet(str(folder))

months = sorted(folder.glob("order_month=*"))
files = [len(list(m.glob("part-*.parquet"))) for m in months]
print(f"{report.count()} rows, {len(months)} month folders, files in each: {files}")
totals = report.agg(F.sum("delivered_orders"), F.round(F.sum("revenue"), 2), F.round(F.sum("cashback_credited"), 2)).first()
print(f"delivered {totals[0]}, revenue {totals[1]:,.2f}, cashback credited {totals[2]:,.2f}")
rows = sorted(tuple(row) for row in report.collect())
print("fingerprint", hashlib.sha256(repr(rows).encode()).hexdigest()[:12])

# The case: Arjun reads April alone.
spark.read.parquet(str(folder / "order_month=2026-04")).orderBy("category").show(truncate=False)

# More checks, not printed in the chapter.
print("--- more checks")
report.orderBy("order_month", "category").show(25, truncate=False)
print(report.dtypes)
spark.stop()
