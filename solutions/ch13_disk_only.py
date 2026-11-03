"""P13.2: persist the 1,500,000-row bigger table with DISK_ONLY, count it, read the Storage tab, unpersist."""
import json
import urllib.request

from pyspark import StorageLevel

from saffron.bigger import bigger_orders
from saffron.session import start

spark = start("p13-2-disk-only")
sc = spark.sparkContext


def storage():
    """The Storage tab's entries, from the REST API the tab is drawn from."""
    with urllib.request.urlopen(f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/storage/rdd") as page:
        return json.load(page)


big = bigger_orders(spark, 2000).persist(StorageLevel.DISK_ONLY)
print("rows:", f"{big.count():,}")
for entry in storage():
    print(entry["storageLevel"], "| cached partitions", f"{entry['numCachedPartitions']}/{entry['numPartitions']}",
          "| in memory", entry["memoryUsed"], "B | on disk", round(entry["diskUsed"] / 2**20, 1), "MiB")
big.unpersist()
print("entries after unpersist:", len(storage()))
spark.stop()
