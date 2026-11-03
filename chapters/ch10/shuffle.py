"""Chapter 10: the default number of shuffle partitions, and which shuffle partition each month lands in."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("ch10-shuffle")
print(spark.conf.get("spark.sql.shuffle.partitions"))
spark.conf.set("spark.sql.adaptive.enabled", "false")
months = (read_table(spark, "transactions")
          .withColumn("order_month",
                      F.date_format("order_date", "yyyy-MM"))
          .groupBy("order_month").count()
          .withColumn("p", F.spark_partition_id()))
print({row["order_month"]: row["p"] for row in months.collect()})

# More checks, not printed in the chapter.
print("--- more checks")
months.explain()
by_hash = (months.select("order_month", "p")
           .withColumn("hash_mod_200", F.pmod(F.hash("order_month"), F.lit(200))))
print([tuple(row) for row in by_hash.orderBy("order_month").collect()])
keys = spark.range(0, 8).repartition(200, "id").withColumn("p", F.spark_partition_id())
print(sorted((row["id"], row["p"]) for row in keys.collect()))
spark.stop()
