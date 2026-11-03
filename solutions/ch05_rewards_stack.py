"""P5.1: stacking the cashback rewards of both nights; union fails on 5 vs 6 columns."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("p5-1-rewards-stack")
rewards1 = read_table(spark, "cashback_rewards", day=1)
rewards2 = read_table(spark, "cashback_rewards", day=2)

try:
    rewards1.union(rewards2)
except Exception as err:
    print(type(err).__name__, str(err).splitlines()[0])

stacked = (rewards1.withColumn("night", F.lit(1))
           .unionByName(rewards2.withColumn("night", F.lit(2)),
                        allowMissingColumns=True))
print(stacked.count(), stacked.columns)

# the silent swap: the same five columns of night 2 with reward_id and order_id exchanged
swapped = rewards2.select("order_id", "reward_id", "cashback_amount",
                          "crediting_date", "status")
mixed = rewards1.union(swapped)
print(mixed.count(),
      mixed.filter(F.col("reward_id").startswith("ORD")).count(),
      "reward_id values that are order ids")
spark.stop()
