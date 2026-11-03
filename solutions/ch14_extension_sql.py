"""P14.3, the SQL alternative: the same two extensions written as SQL over temporary views.

    uv run spark-submit --master "local[4]" solutions/ch14_extension_sql.py \
        --input data/raw --output output/p14_3_sql
"""
import argparse
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "apps"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from ch14_extension import enrich  # noqa: E402  the chapter's enrich plus gateway_name, as in ch14_extension.py
from monthly_report import latest, summarise  # noqa: E402

BY_STATE = """
    SELECT c.state,
           ROUND(SUM(CASE WHEN e.reward_status = 'Credited' THEN e.cashback_amount ELSE 0 END), 2) AS cashback_credited
    FROM enriched e JOIN customers c ON e.customer_id = c.customer_id
    GROUP BY c.state"""

BY_GATEWAY_MONTH = """
    SELECT gateway_name, order_month,
           COUNT(*) AS payments,
           SUM(CASE WHEN payment_status = 'Failed' THEN 1 ELSE 0 END) AS failed,
           ROUND(AVG(CASE WHEN payment_status = 'Failed' THEN 1 ELSE 0 END), 4) AS failure_rate
    FROM enriched
    GROUP BY gateway_name, order_month"""


def main():
    parser = argparse.ArgumentParser(description="Write the monthly report and its two extensions, in SQL.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("p14-3-extension-sql").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    enrich(spark, args.input).createOrReplaceTempView("enriched")
    (latest(spark, args.input, "customers", "customer_id", "registration_date")
     .select("customer_id", F.from_json("customer_metadata", "address_state STRING").address_state.alias("state"))
     .createOrReplaceTempView("customers"))

    summarise(spark.table("enriched")).write.mode("overwrite").partitionBy("order_month").parquet(f"{args.output}/monthly_report")
    spark.sql(BY_STATE).write.mode("overwrite").parquet(f"{args.output}/cashback_by_state")
    spark.sql(BY_GATEWAY_MONTH).write.mode("overwrite").parquet(f"{args.output}/failure_by_gateway_month")

    by_state = spark.read.parquet(f"{args.output}/cashback_by_state")
    by_state.orderBy("state").show(truncate=False)
    print("total", by_state.agg(F.round(F.sum("cashback_credited"), 2)).first()[0])
    rates = spark.read.parquet(f"{args.output}/failure_by_gateway_month")
    print(rates.count(), "rates;", rates.agg(F.sum("payments"), F.sum("failed")).first())
    spark.stop()


if __name__ == "__main__":
    main()
