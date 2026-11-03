"""Chapter 5, stacking the two nights of orders: union by position, then unionByName."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("ch05-stack")
day1 = read_table(spark, "transactions", day=1)
day2 = read_table(spark, "transactions", day=2)
try:
    day1.union(day2)
except Exception as err:
    print(type(err).__name__, str(err).splitlines()[0])

night1 = day1.withColumn("night", F.lit(1))
night2 = day2.withColumn("night", F.lit(2))
print(night1.union(night2.drop("updated_at")).count())

stacked = night1.unionByName(night2, allowMissingColumns=True)
print(stacked.count(), stacked.columns[-2:])
spark.stop()
