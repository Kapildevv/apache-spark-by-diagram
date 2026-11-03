"""Chapter 8, Python UDFs: Arjun's rupee format, the plan node, the return type, and the built-in."""
import os
import re
import time

import pyspark.sql.functions as F

from saffron.prepare import latest_orders
from saffron.schemas import read_table
from saffron.session import start

spark = start("ch08-udf")


# A Python UDF: Indian digit grouping, with a return type.
def indian(x):
    whole, paise = f"{x:.2f}".split(".")
    return re.sub(r"(\d)(?=(\d\d)+\d$)", r"\1,", whole) + "." + paise


rupees = F.udf(indian, "string")
orders = (read_table(spark, "transactions")
          .select("order_id", "total_amount"))
shown = orders.select("order_id",
                      rupees("total_amount").alias("amount"))
print(shown.first())

# The plan: where the rows leave the JVM.
shown.explain()

# Without a return type the result is a string.
doubled = spark.range(3).select(
    F.udf(lambda v: v * 2)("id").alias("v"))
print(doubled.dtypes, doubled.collect())

# The built-in replaces it.
total = (latest_orders(spark).filter("order_status = 'Delivered'")
         .agg(F.sum("total_amount").alias("x")))
exact = F.col("x").cast("decimal(14,2)")
built_in = F.trim(F.to_char(exact, F.lit("99,99,99,999.99")))
print(total.select(rupees("x").alias("udf"),
                   built_in.alias("built_in")).first())
nums = spark.range(1_000_000).select(
    (F.col("id") * 7.77 + 0.5).cast("decimal(14,2)").alias("x"))
print(nums.filter(rupees("x") != built_in).count())

# More checks, not printed in the chapter.
print("--- more checks")
print("raw double:", total.select(F.to_char("x", F.lit("99,99,99,999.99"))).first()[0],
      "| format_number:", total.select(F.format_number("x", 2)).first()[0],
      "| double value:", repr(total.first()[0]))
print("built-in plan has EvalPython:",
      "EvalPython" in total.select(built_in)._jdf.queryExecution().executedPlan().toString())
print("Arrow conf:", spark.conf.get("spark.sql.execution.pythonUDF.arrow.enabled"),
      "| batch size:", spark.conf.get("spark.sql.execution.arrow.maxRecordsPerBatch"))
good = spark.range(3).select(F.udf(lambda v: v * 2, "long")("id").alias("v"))
print("with a return type:", good.dtypes, good.collect())
as_text = spark.range(11).select(F.udf(lambda v: v * 2)("id").alias("v")).orderBy("v").collect()
print("string order:", [r.v for r in as_text][:4])
empty = spark.createDataFrame([(1.5,), (None,)], "x double")
try:
    empty.select(rupees("x")).collect()
except Exception as err:
    print("NULL input:", type(err).__name__, [ln.strip() for ln in str(err).splitlines() if "TypeError" in ln][:1])
print("built-in on NULL:",
      empty.select(F.trim(F.to_char(F.col("x").cast("decimal(14,2)"), F.lit("99,99,99,999.99")))).collect())
calls = spark.sparkContext.accumulator(0)


def counted(x):
    calls.add(1)
    return x


orders.select(F.udf(counted, "double")("total_amount")).collect()
print("function calls / rows:", calls.value, orders.count())
pids = orders.select(F.udf(lambda v: os.getpid(), "long")("total_amount").alias("pid")).distinct().collect()
print("worker differs from driver:", os.getpid() not in [r.pid for r in pids])
big_udf = F.udf(lambda v: v >= 5000, "boolean")
for label, cond in [("UDF filter", big_udf("total_amount")), ("built-in filter", F.col("total_amount") >= 5000)]:
    plan = orders.filter(cond)._jdf.queryExecution().executedPlan().toString()
    print(label, orders.filter(cond).count(), re.findall(r"PushedFilters: \[[^\]]*\]", plan), "EvalPython" in plan)

# One machine's timing: 7,500,000 numbers, a UDF against the same column expression.
arith = F.udf(lambda v: v * 1.18 + 0.5, "double")
df = spark.range(7_500_000).select(F.col("id").cast("double").alias("x"))
by_udf = df.select(F.sum(arith("x")).alias("s"))
by_column = df.select(F.sum(F.col("x") * 1.18 + 0.5).alias("s"))
by_udf.first()
by_column.first()
for label, query in [("UDF", by_udf), ("column expression", by_column)]:
    start_time = time.perf_counter()
    value = query.first()[0]
    print(label, round(time.perf_counter() - start_time, 2), "seconds, sum", round(value))
spark.stop()
