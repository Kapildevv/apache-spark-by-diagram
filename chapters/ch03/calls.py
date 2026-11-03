"""Chapter 3: which calls start jobs, why a DataFrame never changes in place, three mistakes."""
import time

import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("ch03-calls")


def jobs():
    return len(spark.sparkContext.statusTracker().getJobIdsForGroup())


orders = read_table(spark, "transactions")

# Transformations take notes, actions do the work.
chain = (orders.filter(F.col("order_status") == "Delivered")
         .select("order_id", "order_date", "total_amount")
         .withColumn("big_order", F.col("total_amount") >= 5000))
print("jobs after building the chain:", jobs())
chain.count()
print("jobs after count():", jobs())

# Common mistake: timing the notes instead of the work.
began = time.perf_counter()
big = chain.filter(F.col("big_order"))
print("added a step in", round(time.perf_counter() - began, 3), "s")
began = time.perf_counter()
big.count()
print("count() took", round(time.perf_counter() - began, 3), "s")

# Which call is which: an action returns a result, not a DataFrame.
print(type(chain.count()).__name__, type(chain.take(2)).__name__)

# A DataFrame never changes in place.
print(len(orders.columns), len(chain.columns))

# Common mistake: the result of filter is thrown away.
orders.filter(F.col("order_status") == "Cancelled")
print(orders.count())
cancelled = orders.filter(F.col("order_status") == "Cancelled")
print(cancelled.count())

# Common mistake: checking with count() after every step.
delivered = orders.filter(F.col("order_status") == "Delivered")
before = jobs()
for df in [orders, delivered, chain]:
    df.count()
print("jobs for three checks:", jobs() - before)

# The RDD underneath a DataFrame.
before = jobs()
rdd = orders.rdd
print(type(rdd).__name__, rdd.getNumPartitions())
print("jobs started by asking for the RDD:", jobs() - before)

spark.stop()
