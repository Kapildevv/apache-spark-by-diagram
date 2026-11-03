"""Chapter 10: set a Spark setting in the builder, read it, ask which settings can change, change one."""
import os

from pyspark.sql import SparkSession

from saffron.schemas import read_table

master = os.environ.get("SPARK_BOOK_MASTER", "local[*]")
spark = (SparkSession.builder.master(master).appName("ch10-settings")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print(spark.conf.get("spark.sql.shuffle.partitions"))
print(spark.conf.get("spark.driver.memory", "1g"))
print(spark.conf.isModifiable("spark.sql.files.maxPartitionBytes"), spark.conf.isModifiable("spark.driver.memory"))
spark.conf.set("spark.sql.files.maxPartitionBytes", "16384")
print(read_table(spark, "transactions").rdd.getNumPartitions())

# More checks, not printed in the chapter: the two errors of the Common-mistake boxes.
print("--- more checks")
for call in (lambda: spark.conf.set("spark.driver.memory", "2g"),
             lambda: spark.conf.set("spark.master", "local[2]"),
             lambda: spark.conf.get("spark.driver.memory"),
             lambda: spark.conf.get("spark.memory.fraction")):
    try:
        call()
    except Exception as error:
        print(type(error).__name__, "|", " ".join(str(error).split())[:230])
spark.conf.unset("spark.sql.files.maxPartitionBytes")
print("after unset:", read_table(spark, "transactions").rdd.getNumPartitions())
spark.stop()
