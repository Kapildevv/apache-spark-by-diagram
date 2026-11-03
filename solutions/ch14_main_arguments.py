"""P14.1: the report's main() with --input and --output, run with spark-submit.

    uv run spark-submit --master "local[2]" solutions/ch14_main_arguments.py \
        --input data/raw --output output/p14_1
"""
import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(
    0, str(Path(__file__).resolve().parent.parent / "apps"))
from monthly_report import enrich, summarise  # noqa: E402  the chapter's own functions


def main():
    parser = argparse.ArgumentParser(description="Write the monthly report.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    # no .master(): the flag decides
    spark = SparkSession.builder.appName("p14-1-main").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("master:", spark.sparkContext.master)
    (summarise(enrich(spark, args.input))
     .write.mode("overwrite").partitionBy("order_month")
     .parquet(args.output))
    print(sorted(p.name
                 for p in Path(args.output).glob("order_month=*")))
    spark.stop()


if __name__ == "__main__":
    main()
