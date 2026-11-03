"""P5.2: nulls in the stacked rewards; what fillna(0) and fillna("none") change; coalesce and dropna."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("p5-2-rewards-nulls")
night1 = read_table(spark, "cashback_rewards", day=1).withColumn("night", F.lit(1))
night2 = read_table(spark, "cashback_rewards", day=2).withColumn("night", F.lit(2))
stacked = night1.unionByName(night2, allowMissingColumns=True)


def nulls_in(df, column):
    return df.filter(F.col(column).isNull()).count()


counts = {c: nulls_in(stacked, c) for c in stacked.columns}
print({c: n for c, n in counts.items() if n})
for value in (0, "none"):
    filled = stacked.fillna(value)
    print(repr(value), [c for c in stacked.columns if nulls_in(filled, c) != counts[c]])

print(stacked.dropna(subset=["updated_at"]).count())
fixed = stacked.withColumn("updated_at", F.coalesce("updated_at", "crediting_date"))
print(nulls_in(fixed, "updated_at"), fixed.dropna(subset=["updated_at"]).count())
spark.stop()
