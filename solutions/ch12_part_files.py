"""P12.1: predict and count the part files of a 4-partition write, then of a coalesce(1) write."""
import shutil

from saffron.paths import OUTPUT
from saffron.prepare import latest_orders
from saffron.session import start

spark = start("p12-1-part-files")
folder = OUTPUT / "p12_1"
shutil.rmtree(folder, ignore_errors=True)

four = latest_orders(spark).repartition(4)
print("partitions:", four.rdd.getNumPartitions())
for label, df in (("repartition(4)", four), ("coalesce(1)", four.coalesce(1))):
    df.write.mode("overwrite").parquet(str(folder))
    print(label, len(list(folder.glob("part-*"))), "part files, _SUCCESS:", (folder / "_SUCCESS").exists(),
          "| rows read back:", spark.read.parquet(str(folder)).count())

shutil.rmtree(folder)
spark.stop()
