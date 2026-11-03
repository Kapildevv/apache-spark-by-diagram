"""Chapter 14: a builder with a master and a setting in the code. Which wins, the code or the command line?"""
from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local[3]").appName("ch14-master-demo")
         .config("spark.sql.shuffle.partitions", "50").getOrCreate())
print("master", spark.sparkContext.master, "| shuffle partitions", spark.conf.get("spark.sql.shuffle.partitions"))
spark.stop()
