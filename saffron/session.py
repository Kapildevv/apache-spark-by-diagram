"""Start a SparkSession the same way in every chapter."""
import os

from pyspark.sql import SparkSession


def start(app_name):
    """Return a SparkSession named app_name. The master comes from the environment variable
    SPARK_BOOK_MASTER (default local[*]); logging is quiet apart from errors."""
    master = os.environ.get("SPARK_BOOK_MASTER", "local[*]")
    spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
