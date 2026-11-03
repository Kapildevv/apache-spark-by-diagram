"""Chapter 14: Saffron Lane's monthly report as a Spark application.

Run it from the companion folder:
    uv run spark-submit --master "local[4]" apps/monthly_report.py \
        --input data/raw --output output/monthly_report
The master and every other setting come from the command line, never from this file.
"""
import argparse

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window

from saffron.prepare import orders_with_dates  # chapter 4
from saffron.schemas import DAY1, DAY2  # chapter 2


def read_night(spark, folder, table, night):
    """One night of one table with its explicit schema (chapter 2), read from the folder given."""
    schema = (DAY1 if night == 1 else DAY2)[table]
    return (spark.read.option("header", True).option("escape", '"')
            .schema(schema).csv(f"{folder}/day{night}/{table}.csv"))


def latest(spark, folder, table, key, fallback):
    """The newest version of every row across both nights (chapters 5 and 6)."""
    nights = [read_night(spark, folder, table, n).withColumn("night", F.lit(n)) for n in (1, 2)]
    stacked = (nights[0].unionByName(nights[1], allowMissingColumns=True)
               .withColumn("changed_at", F.coalesce("updated_at", F.col(fallback).cast("timestamp")))
               .drop("updated_at"))
    newest_first = Window.partitionBy(key).orderBy(F.col("changed_at").desc(), F.col("night").desc())
    return stacked.withColumn("rn", F.row_number().over(newest_first)).filter("rn = 1").drop("rn")


def enrich(spark, folder):
    """The latest orders with month, category, payment status and reward (chapters 4, 6 and 7).
    The payments are identical on both nights; the products change only ten base prices, which the report
    does not use, and keep their categories, so night 1 serves both."""
    orders = orders_with_dates(latest(spark, folder, "transactions", "order_id", "order_date"))
    products = read_night(spark, folder, "products", 1).select("product_id", "category")
    payments = read_night(spark, folder, "pg_data", 1).select("order_id", "payment_status")
    rewards = (latest(spark, folder, "cashback_rewards", "reward_id", "crediting_date")
               .select("order_id", "cashback_amount", F.col("status").alias("reward_status")))
    return (orders.join(products, "product_id")
            .join(payments, "order_id", "left")
            .join(rewards, "order_id", "left"))


def summarise(enriched):
    """One row for each month and category (chapter 6's conditional aggregates)."""
    delivered = F.col("order_status") == "Delivered"
    credited = F.col("reward_status") == "Credited"
    failed = F.col("payment_status") == "Failed"
    return enriched.groupBy("order_month", "category").agg(
        F.sum(F.when(delivered, 1).otherwise(0)).alias("delivered_orders"),
        F.round(F.sum(F.when(delivered, F.col("total_amount")).otherwise(0)), 2).alias("revenue"),
        F.round(F.sum(F.when(credited, F.col("cashback_amount")).otherwise(0)), 2).alias("cashback_credited"),
        F.round(F.avg(F.when(failed, 1).otherwise(0)), 4).alias("payment_failure_rate"))


def main():
    parser = argparse.ArgumentParser(description="Write Saffron Lane's monthly report as Parquet.")
    parser.add_argument("--input", required=True, help="folder that holds day1/ and day2/")
    parser.add_argument("--output", required=True, help="folder to write; every run replaces it")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("monthly-report").getOrCreate()  # no .master(): spark-submit decides
    spark.sparkContext.setLogLevel("ERROR")
    print(f"monthly report on {spark.sparkContext.master}: {args.input} -> {args.output}")
    report = summarise(enrich(spark, args.input))
    report.write.mode("overwrite").partitionBy("order_month").parquet(args.output)  # chapter 12
    spark.stop()


if __name__ == "__main__":
    main()
