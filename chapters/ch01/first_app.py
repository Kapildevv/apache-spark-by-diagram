from pyspark.sql import SparkSession

from saffron.paths import raw

spark = (SparkSession.builder
         .master("local[*]")
         .appName("first-app")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

print("Spark version:", spark.version)
print("defaultParallelism:", spark.sparkContext.defaultParallelism)

orders = spark.read.option("header", True).csv(raw("transactions"))
print("Orders:", orders.count())

input("Spark UI at http://localhost:4040. Press Enter to stop. ")
spark.stop()
