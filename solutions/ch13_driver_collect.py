"""P13.3: a 1 MiB spark.driver.maxResultSize stops collect() of the flash sale's product ids; count in Spark instead."""
import os
import re
from collections import Counter

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from saffron.bigger import flash_sale_orders

spark = (SparkSession.builder.master(os.environ.get("SPARK_BOOK_MASTER", "local[*]")).appName("p13-3-driver")
         .config("spark.driver.maxResultSize", "1m").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
flash = flash_sale_orders(spark, 10000, 400000)
try:
    ids = flash.select("product_id").collect()          # the script that dies
    print(Counter(row.product_id for row in ids).most_common(3))
except Exception as error:
    print(type(error).__name__, "|", re.search(r"SparkException: .*", str(error)).group())
per_product = (flash.groupBy("product_id").count()
               .orderBy(F.desc("count"), "product_id").toPandas())
print(per_product.shape, "|", per_product.head(3).values.tolist())
spark.stop()
