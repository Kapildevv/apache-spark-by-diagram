"""Chapter 13: cache the bigger table, count it twice, read the Storage tab, unpersist."""
import time

from saffron.bigger import bigger_orders
from saffron.session import start

spark = start("ch13-cache")
big = bigger_orders(spark, 10000).cache()
for label in ("first count", "second count"):
    t0 = time.perf_counter()
    rows = big.count()
    print(f"{label}: {rows:,} rows in "
          f"{time.perf_counter() - t0:.2f} s")
print(big.storageLevel)
input("Storage tab at http://localhost:4040/storage/. "
      "Press Enter to unpersist. ")
big.unpersist()
print(big.is_cached)
spark.stop()
