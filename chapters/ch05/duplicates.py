"""Chapter 5, duplicates: distinct compares whole rows; dropDuplicates compares a subset."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("ch05-duplicates")
night1 = read_table(spark, "transactions", day=1).withColumn("night", F.lit(1))
night2 = read_table(spark, "transactions", day=2).withColumn("night", F.lit(2))
stacked = night1.unionByName(night2, allowMissingColumns=True)

comparable = (stacked.withColumn("changed_at", F.coalesce("updated_at", "order_date"))
              .drop("updated_at", "night"))
print(stacked.distinct().count(), comparable.distinct().count())

one_each = stacked.dropDuplicates(["order_id"])
print(one_each.count(), one_each.filter(F.col("night") == 2).count())

moved = stacked.repartition(2).dropDuplicates(["order_id"])
print(moved.filter(F.col("night") == 2).count())
spark.stop()
