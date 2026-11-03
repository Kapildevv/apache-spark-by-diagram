"""P14.3: the report application, extended with cashback by state and the failure rate per gateway and month.

    uv run spark-submit --master "local[4]" solutions/ch14_extension.py \
        --input data/raw --output output/p14_3
"""
import argparse
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

sys.path.insert(
    0, str(Path(__file__).resolve().parent.parent / "apps"))
from monthly_report import latest, read_night, summarise  # noqa: E402  the chapter's own functions
from saffron.prepare import orders_with_dates  # noqa: E402


def enrich(spark, folder):
    """The chapter's enrich, with one change: the payment's gateway_name stays in the table."""
    orders = orders_with_dates(latest(
        spark, folder, "transactions", "order_id", "order_date"))
    products = (read_night(spark, folder, "products", 1)
                .select("product_id", "category"))
    payments = read_night(spark, folder, "pg_data", 1).select("order_id", "gateway_name", "payment_status")
    rewards = (latest(spark, folder, "cashback_rewards", "reward_id",
                      "crediting_date")
               .select("order_id", "cashback_amount",
                       F.col("status").alias("reward_status")))
    return (orders.join(products, "product_id")
            .join(payments, "order_id", "left")
            .join(rewards, "order_id", "left"))


def cashback_by_state(spark, folder, enriched):
    """Credited cashback of the latest rewards, for each state of the customer who ordered."""
    customers = (latest(spark, folder, "customers", "customer_id", "registration_date")
                 .select("customer_id", F.from_json("customer_metadata", "address_state STRING").alias("meta"))
                 .select("customer_id", F.col("meta.address_state").alias("state")))
    credited = F.col("reward_status") == "Credited"
    return (enriched.join(customers, "customer_id")
            .groupBy("state")
            .agg(F.round(F.sum(F.when(credited, F.col("cashback_amount")).otherwise(0)), 2).alias("cashback_credited")))


def failure_by_gateway_month(enriched):
    """Payments, failed payments and the failure rate for each gateway and month of the latest orders."""
    failed = F.col("payment_status") == "Failed"
    return (enriched.groupBy("gateway_name", "order_month")
            .agg(F.count("*").alias("payments"),
                 F.sum(F.when(failed, 1).otherwise(0)).alias("failed"),
                 F.round(F.avg(F.when(failed, 1).otherwise(0)), 4).alias("failure_rate")))


def main():
    parser = argparse.ArgumentParser(description="Write the monthly report and its two extensions.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("p14-3-extension").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    enriched = enrich(spark, args.input)
    summarise(enriched).write.mode("overwrite").partitionBy("order_month").parquet(f"{args.output}/monthly_report")
    cashback_by_state(spark, args.input, enriched).write.mode("overwrite").parquet(f"{args.output}/cashback_by_state")
    failure_by_gateway_month(enriched).write.mode("overwrite").parquet(f"{args.output}/failure_by_gateway_month")

    by_state = spark.read.parquet(f"{args.output}/cashback_by_state")
    by_state.orderBy("state").show(truncate=False)
    print("total", by_state.agg(F.round(F.sum("cashback_credited"), 2)).first()[0])
    rates = spark.read.parquet(f"{args.output}/failure_by_gateway_month")
    print(rates.count(), "rates;", rates.agg(F.sum("payments"), F.sum("failed")).first())
    spark.stop()


if __name__ == "__main__":
    main()
