from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("practice").getOrCreate()
print(spark.version, spark.sparkContext.defaultParallelism)
spark.stop()
