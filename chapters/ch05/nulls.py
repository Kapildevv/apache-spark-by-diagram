"""Chapter 5, nulls: count them with isNull, then fillna, F.coalesce and dropna."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("ch05-nulls")
night1 = read_table(spark, "transactions", day=1).withColumn("night", F.lit(1))
night2 = read_table(spark, "transactions", day=2).withColumn("night", F.lit(2))
stacked = night1.unionByName(night2, allowMissingColumns=True)


def nulls_in(df, column):
    return df.filter(F.col(column).isNull()).count()


counts = {c: nulls_in(stacked, c) for c in stacked.columns}
print({c: n for c, n in counts.items() if n})

numbered = stacked.withColumn(
    "customer_num", F.col("customer_id").try_cast("int"))
for column in ["customer_num", "updated_at"]:
    print(column, nulls_in(numbered.fillna("none"), column),
          nulls_in(numbered.fillna(0), column))

changed_at = F.coalesce("updated_at", "order_date")
print(nulls_in(stacked.withColumn("changed_at", changed_at),
               "changed_at"))
print(stacked.dropna(subset=["updated_at"]).count())
spark.stop()
