from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master("local[*]")
         .appName("hello-range")
         .getOrCreate())
spark.range(5).show()
spark.stop()
