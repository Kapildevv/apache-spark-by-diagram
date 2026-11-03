"""P13.1: the memory layout of a 4 GiB executor heap by arithmetic, then checked against a real 4 GiB heap. The Executors tab shows the same number as 2.2 GiB."""
import json
import os
import urllib.request

from pyspark.sql import SparkSession

heap = 4 * 1024
usable = heap - 300
unified = 0.6 * usable
print(f"heap {heap}, reserved 300, usable {usable}, unified {unified:.1f} "
      f"(storage {unified / 2:.1f} + execution {unified / 2:.1f}), user {usable - unified:.1f}")

spark = (SparkSession.builder.master(os.environ.get("SPARK_BOOK_MASTER", "local[*]")).appName("p13-1-layout")
         .config("spark.driver.memory", "4g").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
with urllib.request.urlopen(f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/executors") as page:
    print("REST API maxMemory, the unified region:", round(json.load(page)[0]["maxMemory"] / 2**20, 1), "MiB")
spark.stop()
