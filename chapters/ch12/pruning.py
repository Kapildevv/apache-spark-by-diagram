"""Chapter 12: partition pruning and dynamic partition pruning on the folders write.py made (run write.py first)."""
import contextlib
import io
import re
import shutil

import pyspark.sql.functions as F

from saffron.paths import OUTPUT
from saffron.schemas import read_table
from saffron.session import start

spark = start("ch12-pruning")
out = OUTPUT / "ch12"
shutil.rmtree(out / "by_category", ignore_errors=True)


def scan_lines(df, *keys):
    """The lines of df's formatted plan that start with one of keys, such as PartitionFilters."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain(mode="formatted")
    text = re.sub(r"#\d+L?", "", buffer.getvalue())
    return [line for line in text.splitlines() if line.startswith(keys)]


# --- Partition pruning
by_month = spark.read.parquet(str(out / "by_month"))
april = by_month.filter(F.col("order_month") == "2026-04")
print(april.count(), *scan_lines(april, "PartitionFilters"), sep="\n")
delivered = by_month.filter(F.col("order_status") == "Delivered").select("total_amount")
print(*scan_lines(delivered, "PartitionFilters", "PushedFilters", "ReadSchema"), sep="\n")
print(delivered.agg(F.round(F.sum("total_amount"), 2)).first()[0])

# --- Dynamic partition pruning
by_month.coalesce(1).write.partitionBy("category").parquet(str(out / "by_category"))
by_category = spark.read.parquet(str(out / "by_category"))
steelorra = read_table(spark, "products").filter("brand = 'Steelorra'").select("product_id", "category")
on_both = by_category.join(steelorra, ["product_id", "category"])
on_id = by_category.join(steelorra.select("product_id"), "product_id")
for df in (on_both, on_id):
    print(df.count(), *(scan_lines(df, "PartitionFilters") or ["no PartitionFilters line"]), sep="\n")

# More checks, not printed in the chapter.
print("--- more checks")
print(spark.conf.get("spark.sql.optimizer.dynamicPartitionPruning.enabled"))
print(scan_lines(by_month.filter(F.lower(F.col("order_month")) == "2026-04"), "PartitionFilters"))
print(scan_lines(by_month.filter("order_status = 'Delivered'"), "PushedFilters", "ReadSchema"))
print(sorted(p.name for p in (out / "by_category").glob("category=*")))
print(sorted(spark.read.parquet(str(out / "by_category")).select("category").distinct().rdd.map(lambda r: r[0]).collect()))
print(on_both.select("category").distinct().collect(), on_id.select("category").distinct().collect())
input("Spark UI at http://localhost:4040. Press Enter to stop. ")
spark.stop()
