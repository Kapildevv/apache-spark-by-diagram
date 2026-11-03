from pyspark.sql import SparkSession

from saffron.paths import raw

spark = SparkSession.builder.master("local[*]").appName("pg-jobs").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

payments = spark.read.option("header", True).csv(raw("pg_data"))
print("Payments:", payments.count())

input("Spark UI at http://localhost:4040. Press Enter to stop. ")
spark.stop()
