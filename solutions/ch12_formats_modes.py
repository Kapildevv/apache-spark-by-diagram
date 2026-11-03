"""P12.2: write the latest orders as CSV, JSON and Parquet, meet the save modes, compare the types read back."""
import shutil

from pyspark.errors import AnalysisException

from saffron.paths import OUTPUT
from saffron.prepare import latest_orders
from saffron.session import start

spark = start("p12-2-formats-modes")
folder = OUTPUT / "p12_2"
shutil.rmtree(folder, ignore_errors=True)

orders = latest_orders(spark)
orders.write.option("header", True).csv(str(folder / "csv"))
orders.write.json(str(folder / "json"))
orders.write.parquet(str(folder / "parquet"))

try:
    orders.write.parquet(str(folder / "parquet"))
except AnalysisException as err:
    print(str(err).split()[0])
orders.write.mode("overwrite").parquet(str(folder / "parquet"))

readers = {"csv": spark.read.option("header", True).csv, "json": spark.read.json, "parquet": spark.read.parquet}
for kind, read in readers.items():
    back = read(str(folder / kind))
    types = dict(back.dtypes)
    print(f"{kind:<8}{back.count()} rows, first column {back.columns[0]:<12}"
          f"order_date {types['order_date']}, quantity {types['quantity']}")

shutil.rmtree(folder)
spark.stop()
