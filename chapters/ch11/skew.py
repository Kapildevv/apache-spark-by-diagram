"""Chapter 11, case: orders and revenue by category for the flash sale, joined plain, with AQE's skew join, and salted."""
import pyspark.sql.functions as F

from saffron.bigger import flash_sale_orders
from saffron.schemas import read_table
from saffron.session import start

spark = start("ch11-skew")
flash = flash_sale_orders(spark, 10000, 400000)
products = read_table(spark, "products")
plain = flash.join(products, "product_id")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
plain.write.format("noop").mode("overwrite").save()
input("Plain join done. Open http://localhost:4040, then press Enter. ")

spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "32m")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "8m")
plain.write.format("noop").mode("overwrite").save()
input("Skew join done. Press Enter for the salted run. ")

salted_orders = flash.withColumn("salt", F.col("copy") % 8)
salted_products = products.withColumn("salt", F.explode(F.sequence(F.lit(0), F.lit(7))))
salted = salted_orders.join(salted_products, ["product_id", "salt"])
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
salted.write.format("noop").mode("overwrite").save()
input("Salted join done. Press Enter for the report. ")


def by_category(joined):
    return (joined.groupBy("category")
            .agg(F.count("*").alias("orders"),
                 F.sum(F.col("total_amount").cast("decimal(18,2)")).alias("revenue")))


totals = sorted(by_category(plain).collect())
for row in totals:
    print(row.category, row.orders, row.revenue)
print(sorted(by_category(salted).collect()) == totals)
spark.stop()
