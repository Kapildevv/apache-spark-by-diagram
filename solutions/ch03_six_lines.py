"""P3.1: a six-line script, and the number of jobs each line starts."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("p3-1-six-lines")


def jobs():
    return len(spark.sparkContext.statusTracker().getJobIdsForGroup())


counts = [jobs()]
orders = read_table(spark, "transactions")
counts.append(jobs())
delivered = orders.filter(F.col("order_status") == "Delivered")
counts.append(jobs())
slim = delivered.select("order_id", "order_date", "total_amount")
counts.append(jobs())
tagged = slim.withColumn("big_order", F.col("total_amount") >= 5000)
counts.append(jobs())
tagged.show(5)
counts.append(jobs())
tagged.count()
counts.append(jobs())
print("jobs per line:", [b - a for a, b in zip(counts, counts[1:])])

spark.stop()
