"""Chapter 13: what a small spark.driver.maxResultSize lets through."""
import os
import re

from pyspark.sql import SparkSession

from saffron.bigger import bigger_orders

spark = (SparkSession.builder.master(os.environ.get("SPARK_BOOK_MASTER", "local[*]")).appName("ch13-maxresult")
         .config("spark.driver.maxResultSize", "1m").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
big = bigger_orders(spark, 10000)
tries = {"big.collect()": big.collect,
         "per-month counts": big.groupBy("order_month").count().collect,
         "big.limit(50)": big.limit(50).collect}
for name, run in tries.items():
    try:
        print(f"{name}: {len(run())} rows")
    except Exception as error:
        print(f"{name}: {type(error).__name__}, {re.search(r'Total size.*', str(error)).group()}")
spark.stop()
