"""Chapter 14: run the report query in this session and read the plan that ran, and the stages the Spark UI lists."""
import contextlib
import io
import json
import re
import sys
import urllib.request

import pyspark.sql.functions as F

from saffron.paths import ROOT
from saffron.session import start

sys.path.insert(0, str(ROOT / "apps"))
from monthly_report import enrich, latest, read_night, summarise  # noqa: E402  the application's own functions
from saffron.prepare import orders_with_dates  # noqa: E402

spark = start("ch14-check-plan")
sc = spark.sparkContext


def ui(path):
    """One page of the Spark UI's REST API, as Python data (as in chapter 13)."""
    with urllib.request.urlopen(f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{path}") as page:
        return json.load(page)


folder = str(ROOT / "data" / "raw")
report = summarise(enrich(spark, folder))
sc.setJobGroup("report", "the report query")
report.collect()  # run the query itself, so that explain() shows the plan that ran

buffer = io.StringIO()
with contextlib.redirect_stdout(buffer):
    report.explain()
final = re.sub(r"#\d+L?|plan_id=\d+", "", buffer.getvalue()).split("== Initial Plan ==")[0]
# findall picks out the name of every join node in the final plan (chapter 11)
print(re.findall(r"\w+Join(?= \[)", final), final.count("Exchange hashpartitioning"), "shuffles,",
      final.count("Sort ["), "sorts")

jobs = [job for job in ui("jobs") if job.get("jobGroup") == "report"]
stage_ids = {sid for job in jobs for sid in job["stageIds"]}
ran = {s["stageId"]: s["numTasks"] for s in ui("stages") if s["stageId"] in stage_ids and s["status"] == "COMPLETE"}
print(f"{len(jobs)} jobs; stages that ran and their tasks: {dict(sorted(ran.items()))}")

# More checks, not printed in the chapter.
print("--- more checks")
print(re.findall(r"Exchange hashpartitioning\([^)]*\)", final))
print("stages listed:", len(stage_ids), "| skipped:", len(stage_ids) - len(ran))
for s in sorted(ui("stages"), key=lambda s: s["stageId"]):
    if s["stageId"] in ran:
        print("stage", s["stageId"], "tasks", s["numTasks"], "shuffle read records", s["shuffleReadRecords"],
              "shuffle write records", s["shuffleWriteRecords"])
print("broadcast exchanges:", final.count("BroadcastExchange"), "| AQEShuffleRead:", final.count("AQEShuffleRead"))
print("shuffle partitions setting:", spark.conf.get("spark.sql.shuffle.partitions"), "| master:", sc.master)
orders = latest(spark, folder, "transactions", "order_id", "order_date")
rewards = latest(spark, folder, "cashback_rewards", "reward_id", "crediting_date")
enriched = enrich(spark, folder)
print("latest orders", orders.count(), "| latest rewards", rewards.count(), "| enriched", enriched.count())
print("payments failed in enriched:", enriched.filter("payment_status = 'Failed'").count(),
      "| null reward_status:", enriched.filter("reward_status IS NULL").count())
print("cashback credited, delivered orders only:",
      enriched.filter("order_status = 'Delivered' AND reward_status = 'Credited'").agg(F.round(F.sum("cashback_amount"), 2)).first()[0])
print("order status of the failed payments:", enriched.filter("payment_status = 'Failed'").groupBy("order_status").count().collect())

# Rows after each step (Plate B).
night1, night2 = (read_night(spark, folder, "transactions", n) for n in (1, 2))
r1, r2 = (read_night(spark, folder, "cashback_rewards", n) for n in (1, 2))
stacked = night1.unionByName(night2, allowMissingColumns=True)
dated = orders_with_dates(orders)
with_products = dated.join(read_night(spark, folder, "products", 1).select("product_id", "category"), "product_id")
with_payments = with_products.join(read_night(spark, folder, "pg_data", 1).select("order_id", "payment_status"), "order_id", "left")
print("read:", night1.count(), night2.count(), "| rewards", r1.count(), r2.count(), "| stacked orders", stacked.count(),
      "| stacked rewards", r1.unionByName(r2, allowMissingColumns=True).count())
print("latest", orders.count(), "| dated", dated.count(), "| + products", with_products.count(), "| + payments", with_payments.count(),
      "| + rewards", enriched.count(), "| report", summarise(enriched).count())
spark.stop()
