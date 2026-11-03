"""The table-preparing helpers the chapters build up, one at a time."""
from pyspark.sql import Window
from pyspark.sql import functions as F

from saffron.schemas import read_table


def orders_with_dates(orders):
    """Chapter 4. Add order_ts, order_day, order_month ('2026-04'), unit_price and order_size."""
    ts = F.to_timestamp("order_date")
    return (orders
            .withColumn("order_ts", ts)
            .withColumn("order_day", F.to_date(ts))
            .withColumn("order_month", F.date_format(ts, "yyyy-MM"))
            .withColumn("unit_price", F.round(F.col("total_amount") / F.col("quantity"), 2))
            .withColumn("order_size",
                        F.when(F.col("total_amount") < 1000, "small")
                         .when(F.col("total_amount") < 5000, "medium")
                         .otherwise("large")))


def customers_masked(customers):
    """Chapter 4. Add full_name and email_masked ('i***@example.com'), then drop the raw email."""
    return (customers
            .withColumn("full_name",
                        F.concat_ws(" ", "first_name", "last_name"))
            .withColumn(
                "email_masked",
                F.regexp_replace("email", r"^(.)[^@]*@", "$1***@"))
            .drop("email"))


def _stack_nights(spark, table, fallback):
    """Both nights of a table, stacked by name, tagged with night (1 or 2). changed_at is
    updated_at, or the column named by fallback where the night has no updated_at; updated_at is dropped."""
    nights = [
        read_table(spark, table, day).withColumn("night", F.lit(day))
        for day in (1, 2)]
    return (nights[0].unionByName(nights[1], allowMissingColumns=True)
            .withColumn("changed_at",
                        F.coalesce("updated_at",
                                   F.col(fallback).cast("timestamp")))
            .drop("updated_at"))


def _latest(stacked, key):
    """Keep one row per key: the newest changed_at, ties broken by the later night."""
    newest_first = Window.partitionBy(key).orderBy(F.col("changed_at").desc(), F.col("night").desc())
    return stacked.withColumn("rn", F.row_number().over(newest_first)).filter("rn = 1").drop("rn")


def orders_both_nights(spark):
    """Chapter 5. Day 1 and day 2 orders stacked (1,500 rows) with night and changed_at."""
    return _stack_nights(spark, "transactions", "order_date")


def latest_orders(spark):
    """Chapter 6. One row per order, the newest version (750 rows)."""
    return _latest(orders_both_nights(spark), "order_id")


def latest_rewards(spark):
    """Chapter 6 (practice) and 14. One row per reward, the newest version (391 rows)."""
    return _latest(_stack_nights(spark, "cashback_rewards", "crediting_date"), "reward_id")


def customers_with_state(spark):
    """Chapter 8. The newest version of each customer, email masked, with the parsed
    customer_metadata in a struct column called meta (meta.address_state, meta.device_type)."""
    meta_schema = ("device_type STRING, address_state STRING, "
                   "city STRING, pincode STRING, "
                   "address STRUCT<address_state: STRING, "
                   "city: STRING, pincode: STRING>, "
                   "app_version STRING, locale STRING")
    latest = _latest(
        _stack_nights(spark, "customers", "registration_date"),
        "customer_id")
    return (customers_masked(latest)
            .withColumn("meta",
                        F.from_json("customer_metadata", meta_schema))
            .drop("customer_metadata"))
