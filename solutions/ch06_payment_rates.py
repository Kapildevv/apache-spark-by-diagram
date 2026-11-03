"""P6.1: payments by method and by status, then the payment-failure rate per payment method."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("p6-1-payment-rates")
payments = read_table(spark, "pg_data").select("payment_method", "payment_status")
payments.groupBy("payment_method").count().orderBy(F.desc("count")).show()
payments.groupBy("payment_status").count().orderBy(F.desc("count")).show()

failed = F.when(F.col("payment_status") == "Failed", 1).otherwise(0)
rates = payments.groupBy("payment_method").agg(
    F.count("*").alias("payments"), F.sum(failed).alias("failed"),
    F.round(F.avg(failed), 4).alias("rate"))
rates.orderBy("payment_method").show()
spark.stop()
