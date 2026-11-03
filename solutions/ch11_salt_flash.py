"""P11.3: the flash sale joined to the products on local[4] with broadcast and the skew join off and coalescing
left on: the slowest task against the median in the join stage (read from the UI's REST API), then a join salted
with 4 salts whose totals by category match the plain join."""
import json
import time
import urllib.request

import pyspark.sql.functions as F

from saffron.bigger import flash_sale_orders
from saffron.schemas import read_table
from saffron.session import start

spark = start("p11-3-salt")
sc = spark.sparkContext
flash = flash_sale_orders(spark, 10000, 400000)
products = read_table(spark, "products")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")


def rest(path):
    return json.loads(urllib.request.urlopen(f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{path}").read())


plain = flash.join(products, "product_id")
plain.write.format("noop").mode("overwrite").save()
time.sleep(1)  # the UI's store lags a moment behind the action
join_stage = max((s for s in rest("stages") if s["name"].startswith("save")), key=lambda s: s["stageId"])
tasks = rest(f"stages/{join_stage['stageId']}/{join_stage['attemptId']}/taskList?length=2000")
ms = sorted(t["duration"] for t in tasks if t["status"] == "SUCCESS")
slowest, median = ms[-1], ms[len(ms) // 2]
print(f"tasks: {len(ms)} | slowest {slowest / 1000:.1f} s | median {median / 1000:.1f} s | {slowest / median:.1f} times the median")


def by_category(joined):
    return (joined.groupBy("category")
            .agg(F.count("*").alias("orders"),
                 F.sum(F.col("total_amount").cast("decimal(18,2)")).alias("revenue")))


salts = 4
salted_orders = flash.withColumn("salt", F.col("copy") % salts)
salted_products = products.withColumn("salt", F.explode(F.sequence(F.lit(0), F.lit(salts - 1))))
salted = salted_orders.join(salted_products, ["product_id", "salt"])
plain_totals, salted_totals = sorted(by_category(plain).collect()), sorted(by_category(salted).collect())
for row in salted_totals:
    print(row.category, row.orders, row.revenue)
print("salted totals equal the plain ones:", salted_totals == plain_totals)
spark.stop()
