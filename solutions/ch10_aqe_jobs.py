"""P10.3: revenue by month on the bigger table, AQE off and on: jobs, stages and tasks from the Spark UI's REST API."""
import json
import os
import time
import urllib.request

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from saffron.bigger import bigger_orders

master = os.environ.get("SPARK_BOOK_MASTER", "local[*]")
spark = (SparkSession.builder.master(master).appName("p10-3-aqe-jobs")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
print(spark.conf.get("spark.sql.shuffle.partitions"))


def revenue_by_month(df):
    return (df.groupBy("order_month")
            .agg(F.count("*").alias("orders"),
                 F.round(F.sum("total_amount"), 2).alias("revenue")))


def stages_of(group):
    """Tasks and status of every stage of the jobs run under one job group, from the REST API."""
    time.sleep(1)  # the UI's store lags a moment behind the action
    ids = sorted({s for j in sc.statusTracker().getJobIdsForGroup(group)
                  for s in sc.statusTracker().getJobInfo(j).stageIds})
    rows = []
    for stage in ids:
        url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/stages/{stage}"
        info = json.loads(urllib.request.urlopen(url).read())[0]
        rows.append((info["numTasks"], info["status"]))
    return len(sc.statusTracker().getJobIdsForGroup(group)), rows


for label, aqe in (("AQE off", "false"), ("AQE on", "true")):
    spark.conf.set("spark.sql.adaptive.enabled", aqe)
    sc.setJobGroup(label, label)
    revenue_by_month(bigger_orders(spark, 10000)).collect()
    jobs, stages = stages_of(label)
    print(f"{label}: {jobs} jobs, stages (tasks, status) {stages}")

try:
    spark.conf.set("spark.driver.memory", "2g")
except Exception as error:
    print(type(error).__name__, "|", str(error).splitlines()[0])
print(spark.conf.isModifiable("spark.driver.memory"), spark.conf.isModifiable("spark.sql.shuffle.partitions"))
spark.stop()
