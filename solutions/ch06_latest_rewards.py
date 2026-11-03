"""P6.3: the latest status of each cashback reward, with row_number over both nights."""
import pyspark.sql.functions as F
from pyspark.sql import Window

from saffron.schemas import read_table
from saffron.session import start

spark = start("p6-3-latest-rewards")
night1 = read_table(spark, "cashback_rewards", day=1).withColumn("night", F.lit(1))
night2 = read_table(spark, "cashback_rewards", day=2).withColumn("night", F.lit(2))
stacked = (night1.unionByName(night2, allowMissingColumns=True)
           .withColumn("changed_at",
                       F.coalesce("updated_at", "crediting_date")))

newest = (Window.partitionBy("reward_id")
          .orderBy(F.col("changed_at").desc(), F.col("night").desc()))
latest = (stacked.withColumn("rn", F.row_number().over(newest))
          .filter("rn = 1"))
print(latest.count())
latest.groupBy("status").count().orderBy(F.desc("count")).show()
spark.stop()
