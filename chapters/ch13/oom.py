"""Chapter 13: the flash-sale join on a small heap. Usage: oom.py [driver memory, e.g. 480m] [skew]"""
import os
import sys

from pyspark.sql import SparkSession

from saffron.bigger import flash_sale_orders
from saffron.schemas import read_table

settings = {"spark.driver.memory":
                sys.argv[1] if len(sys.argv) > 1 else "1g",
            "spark.sql.autoBroadcastJoinThreshold": "-1",
            "spark.sql.adaptive.skewJoin.enabled": "false"}
if "skew" in sys.argv:
    settings.update({"spark.sql.adaptive.skewJoin.enabled": "true",
                     "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "32m",
                     "spark.sql.adaptive.advisoryPartitionSizeInBytes": "8m"})
builder = SparkSession.builder.master(os.environ.get("SPARK_BOOK_MASTER", "local[*]")).appName("ch13-oom")
for key, value in settings.items():
    builder = builder.config(key, value)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("heap:",
      spark._jvm.java.lang.Runtime.getRuntime().maxMemory() // 2**20,
      "MiB")
flash = (flash_sale_orders(spark, 10000, 400000)
         .join(read_table(spark, "products"), "product_id"))
try:
    flash.write.format("noop").mode("overwrite").save()
    input("Join finished. Spark UI at http://localhost:4040. "
          "Press Enter to stop. ")
except Exception as error:
    try:
        print(type(error).__name__, str(error).splitlines()[0])
    # a Py4JJavaError asks the JVM for its message, and the JVM may be
    # gone already
    except Exception as gone:
        print(type(gone).__name__, gone)
    try:
        spark.range(5).count()
    except Exception as again:
        print(type(again).__name__, again)
try:
    spark.stop()
except Exception:  # after an out-of-memory error the JVM is gone, so there is nothing left to stop
    pass
