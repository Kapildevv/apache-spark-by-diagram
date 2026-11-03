"""Chapter 10: the partitions of a file read, of a range, of the bigger table, and what a narrow step keeps."""
import pyspark.sql.functions as F

from saffron.bigger import bigger_orders
from saffron.schemas import read_table
from saffron.session import start

spark = start("ch10-partitions")
orders = read_table(spark, "transactions")
numbers = spark.range(0, 100000)
print("orders:", orders.rdd.getNumPartitions(),
      "| range:", numbers.rdd.getNumPartitions(),
      "| slots:", spark.sparkContext.defaultParallelism)
per_partition = (numbers.groupBy(F.spark_partition_id().alias("p"))
                 .count())
print(sorted(tuple(row) for row in per_partition.collect()))

big = bigger_orders(spark, 10000)
sizes = big.groupBy(F.spark_partition_id().alias("p")).count()
print("bigger:", big.rdd.getNumPartitions(),
      sorted(row["count"] for row in sizes.collect()))
narrow = (big.filter(F.col("order_status") == "Delivered")
          .select("order_id", "total_amount"))
print("after filter and select:", narrow.rdd.getNumPartitions())

# More checks, not printed in the chapter.
print("--- more checks")
print("rows:", big.count(), "| first order ids:", [r[0] for r in big.select("order_id").limit(2).collect()])
print("plain crossJoin:", orders.crossJoin(spark.range(0, 10000)).rdd.getNumPartitions())
print("orders file bytes per partition setting:", spark.conf.get("spark.sql.files.maxPartitionBytes"))
spark.stop()
