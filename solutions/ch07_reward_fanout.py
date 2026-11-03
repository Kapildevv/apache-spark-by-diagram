"""P7.3: a condition join of the latest rewards to the stacked rewards fails, then doubles the rows."""
import pyspark.sql.functions as F

from saffron.prepare import latest_rewards
from saffron.schemas import read_table
from saffron.session import start

spark = start("p7-3-reward-fanout")
night1 = read_table(spark, "cashback_rewards", day=1).withColumn("night", F.lit(1))
night2 = read_table(spark, "cashback_rewards", day=2).withColumn("night", F.lit(2))
stacked = night1.unionByName(night2, allowMissingColumns=True)
latest = latest_rewards(spark)
print(latest.count(), stacked.count())

l, s = latest.alias("l"), stacked.alias("s")
cond = l.join(s, F.col("l.reward_id") == F.col("s.reward_id"))
try:
    cond.select("reward_id")
except Exception as err:
    print(type(err).__name__, str(err).splitlines()[0])
print(cond.columns.count("reward_id"))

# The first fix: say which reward_id. The count is still too big.
print(cond.select("l.reward_id").count())
print(latest.join(stacked, "reward_id").count())

# The second fix: the stacked side holds each reward twice, so keep only night 1 before the join.
s1 = stacked.filter("night = 1").alias("s")
night1_beside_latest = l.join(s1, F.col("l.reward_id") == F.col("s.reward_id"))
print(night1_beside_latest.select("l.reward_id", "l.cashback_amount", "s.cashback_amount").count())
spark.stop()
