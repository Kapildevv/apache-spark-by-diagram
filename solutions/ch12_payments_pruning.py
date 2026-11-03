"""P12.3: prune the payment folders with a filter, then save orders as a table and let a join prune its folders."""
import contextlib
import io
import re
import shutil
from pathlib import Path

from saffron.paths import OUTPUT
from saffron.prepare import latest_orders
from saffron.schemas import read_table
from saffron.session import start

spark = start("p12-3-payments-pruning")
folder = OUTPUT / "p12_3"
shutil.rmtree(folder, ignore_errors=True)
shutil.rmtree("spark-warehouse/orders_by_category", ignore_errors=True)


def scan_lines(df, *keys):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain(mode="formatted")
    text = re.sub(r"#\d+L?", "", buffer.getvalue())
    return [line for line in text.splitlines() if line.startswith(keys)]


payments = (read_table(spark, "pg_data")
            .select("payment_id", "order_id", "payment_method",
                    "payment_status"))
payments.write.partitionBy("payment_method").parquet(str(folder))
print(len(list(folder.glob("payment_method=*"))), "folders")

back = spark.read.parquet(str(folder))
upi = back.filter("payment_method = 'UPI'")
print(upi.count(), "rows;", *scan_lines(upi, "PartitionFilters"))
failed = back.filter("payment_status = 'Failed'")
print(failed.count(), "rows;", scan_lines(failed, "PartitionFilters") or "no PartitionFilters line", *scan_lines(failed, "PushedFilters"))

# Part 2: a managed table partitioned by category, pruned at run time by a join
products = read_table(spark, "products")
orders = latest_orders(spark).join(
    products.select("product_id", "category"), "product_id")
orders.write.partitionBy("category").saveAsTable("orders_by_category")
table = spark.table("orders_by_category")
print(table.count(), "rows in the table")
premium = (products.filter("base_price >= 10000")
           .select("product_id", "category"))
premium_orders = table.join(premium, ["product_id", "category"])
print(premium_orders.count(), "premium orders;",
      *scan_lines(premium_orders, "PartitionFilters"))

spark.sql("DROP TABLE orders_by_category")
print(Path("spark-warehouse/orders_by_category").exists())
shutil.rmtree(folder)
spark.stop()
