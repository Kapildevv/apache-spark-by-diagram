"""Chapter 12: write the latest orders as files, control how many files come out, lay them out in folders."""
import contextlib
import io
import re
import shutil
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.errors import AnalysisException

from saffron.bigger import bigger_orders
from saffron.paths import OUTPUT
from saffron.prepare import customers_with_state, latest_orders, orders_with_dates
from saffron.schemas import read_table
from saffron.session import start

spark = start("ch12-write")
out = OUTPUT / "ch12"  # every file this script writes lands here; each run starts the folder afresh
shutil.rmtree(out, ignore_errors=True)
# A new session does not know a table saved by an earlier run, but its folder is still there,
# and saveAsTable refuses to write into it: LOCATION_ALREADY_EXISTS.
shutil.rmtree("spark-warehouse/orders_enriched", ignore_errors=True)


def part_files(folder):
    """How many part files a write left in folder (and below it), and their total size in bytes."""
    parts = list(Path(folder).rglob("part-*"))
    return len(parts), sum(p.stat().st_size for p in parts)


def plan(df):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain()
    return re.sub(r"#\d+L?|plan_id=\d+", "", buffer.getvalue())


def orders_enriched(spark):
    """The latest orders with order_month, the product's category and the customer's state."""
    products = read_table(spark, "products").select("product_id", "category")
    states = customers_with_state(spark).select("customer_id", F.col("meta.address_state").alias("state"))
    return orders_with_dates(latest_orders(spark)).join(products, "product_id").join(states, "customer_id")


# --- Save modes and part files
lat = latest_orders(spark)
lat.write.parquet(str(out / "parquet"))
print(lat.rdd.getNumPartitions(), sorted(p.name for p in (out / "parquet").glob("[!.]*")))
try:
    lat.write.parquet(str(out / "parquet"))
except AnalysisException as err:
    print(str(err).splitlines()[0])

# --- CSV, JSON and Parquet
lat.write.option("header", True).csv(str(out / "csv"))
lat.write.json(str(out / "json"))
readers = {"csv": spark.read.option("header", True).csv, "json": spark.read.json, "parquet": spark.read.parquet}
for kind, read in readers.items():
    back = read(str(out / kind))
    types = dict(back.dtypes)
    print(f"{kind:<8}{part_files(out / kind)[1]:>8,} bytes  first column {back.columns[0]:<12}"
          f"order_date {types['order_date']}, quantity {types['quantity']}")

# --- How many files: repartition and coalesce
big = bigger_orders(spark, 100)  # 75,000 rows in 4 partitions
folder = out / "files"
for label, df in (("as is", big), ("repartition(8)", big.repartition(8)), ("coalesce(2)", big.coalesce(2))):
    df.write.mode("overwrite").parquet(str(folder))
    node = re.search(r"Exchange RoundRobinPartitioning\(\d+\)|Coalesce \d+", plan(df))
    print(f"{label:<16}{node.group() if node else '-':<36}{part_files(folder)[0]} part files")

# --- partitionBy: a folder for each month
enriched = orders_enriched(spark)
enriched.write.partitionBy("order_month").parquet(str(out / "by_month"))
enriched.repartition(200).write.partitionBy("order_month").parquet(str(out / "by_month_200"))
for name in ("by_month", "by_month_200"):
    print(name, [part_files(m)[0] for m in sorted((out / name).glob("order_month=*"))])

# --- Tables and the warehouse
enriched.write.partitionBy("order_month").mode("overwrite").saveAsTable("orders_enriched")
print([(t.name, t.tableType) for t in spark.catalog.listTables()], spark.table("orders_enriched").count())

# --- The nightly orders, solved
april = spark.table("orders_enriched").filter("order_month = '2026-04' AND order_status = 'Delivered'")
print(april.count(), april.agg(F.round(F.sum("total_amount"), 2)).first()[0])

# More checks, not printed in the chapter.
print("--- more checks")
print(len(list(Path("spark-warehouse/orders_enriched").glob("order_month=*"))), "month folders in the warehouse")
print("enriched rows", enriched.count(), "partitions", enriched.rdd.getNumPartitions(), "columns", enriched.columns)
print("big partitions", big.rdd.getNumPartitions())
print("coalesce(2) on lat:", lat.coalesce(2).rdd.getNumPartitions(), "| repartition(2) on lat:", lat.repartition(2).rdd.getNumPartitions())
eight = big.repartition(8)
print("coalesce(16) on 8:", eight.coalesce(16).rdd.getNumPartitions(), "| repartition(16) on 8:", eight.repartition(16).rdd.getNumPartitions())
print("coalesce plan has Exchange:", "Exchange" in plan(big.coalesce(2)).replace("BroadcastExchange", ""))
print(spark.conf.get("spark.sql.parquet.compression.codec"), spark.conf.get("spark.sql.sources.default"),
      spark.conf.get("spark.sql.warehouse.dir"), spark.conf.get("spark.sql.sources.partitionOverwriteMode"))
lat.write.mode("append").parquet(str(out / "parquet"))
print("after append:", spark.read.parquet(str(out / "parquet")).count())
lat.write.mode("ignore").parquet(str(out / "parquet"))
print("after ignore:", spark.read.parquet(str(out / "parquet")).count())
lat.write.mode("overwrite").parquet(str(out / "parquet"))
print("after overwrite:", spark.read.parquet(str(out / "parquet")).count(), part_files(out / "parquet")[0])
print("csv inferSchema:", dict(spark.read.option("header", True).option("inferSchema", True).csv(str(out / "csv")).dtypes))
print("parquet back:", spark.read.parquet(str(out / "parquet")).dtypes)
print("json back:", spark.read.json(str(out / "json")).dtypes)
print("files per month, by_month_200 total:", part_files(out / "by_month_200")[0])
print("partition column when read back:", dict(spark.read.parquet(str(out / "by_month")).dtypes)["order_month"])
spark.stop()
