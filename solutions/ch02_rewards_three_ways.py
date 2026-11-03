"""P2.2: read cashback_rewards.csv three ways and record the jobs each read starts."""
from saffron.paths import raw
from saffron.session import start

spark = start("ch02-rewards-three-ways")


def jobs():
    return len(spark.sparkContext.statusTracker().getJobIdsForGroup())


path = raw("cashback_rewards")
ddl = ("reward_id STRING, order_id STRING, cashback_amount DOUBLE, "
       "crediting_date TIMESTAMP, status STRING")

counts = [jobs()]
header_only = spark.read.option("header", True).csv(path)
counts.append(jobs())
inferred = (spark.read.option("header", True)
            .option("inferSchema", True).csv(path))
counts.append(jobs())
explicit = spark.read.option("header", True).schema(ddl).csv(path)
counts.append(jobs())
print("jobs per read:", [b - a for a, b in zip(counts, counts[1:])])

for name, df in [("header only", header_only), ("inferSchema", inferred), ("DDL", explicit)]:
    print(name, df.count(), "rows", df.dtypes[2:4])

spark.stop()
