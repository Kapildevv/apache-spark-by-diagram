"""P5.3: distinct on the stacked rewards, after making the rows comparable, and dropDuplicates."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("p5-3-rewards-distinct")
night1 = read_table(spark, "cashback_rewards", day=1).withColumn("night", F.lit(1))
night2 = read_table(spark, "cashback_rewards", day=2).withColumn("night", F.lit(2))
stacked = night1.unionByName(night2, allowMissingColumns=True)
print(stacked.distinct().count())

comparable = (stacked.withColumn("updated_at", F.coalesce("updated_at", "crediting_date"))
              .drop("night"))
print(comparable.distinct().count())

one_each = stacked.dropDuplicates(["reward_id"])
print(one_each.count(), one_each.filter(F.col("night") == 2).count())
spark.stop()
