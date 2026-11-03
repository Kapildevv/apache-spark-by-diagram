"""P8.3: label each customer's zone from the first pincode digit with a UDF, then with `when`, and compare.

The zone rule is made up for this exercise and is not a postal definition:
first digit 1-2 North, 3-4 West, 5-6 South, 7-8 East.
"""
import contextlib
import io

import pyspark.sql.functions as F

from saffron.prepare import customers_with_state
from saffron.session import start

spark = start("p8-3-zones")
pincodes = (customers_with_state(spark)
            .select(F.col("meta.pincode").alias("pincode")))

zones = {"1": "North", "2": "North", "3": "West", "4": "West",
         "5": "South", "6": "South", "7": "East", "8": "East"}
zone_udf = F.udf(lambda pincode: None if pincode is None
                 else zones.get(pincode[0], "Other"), "string")

first = F.substring("pincode", 1, 1)
zone_when = (F.when(first.isin("1", "2"), "North")
             .when(first.isin("3", "4"), "West")
             .when(first.isin("5", "6"), "South")
             .when(first.isin("7", "8"), "East")
             .otherwise("Other"))

by_udf = pincodes.groupBy(zone_udf("pincode").alias("zone")).count()
by_when = pincodes.groupBy(zone_when.alias("zone")).count()
by_udf.orderBy("zone").show()
by_when.orderBy("zone").show()
print(sorted(by_udf.collect()) == sorted(by_when.collect()))


def has_node(query):
    """True when explain() prints an ArrowEvalPython node."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        query.explain()
    return "ArrowEvalPython" in buffer.getvalue()


print("UDF plan has ArrowEvalPython:", has_node(by_udf))
print("when plan has ArrowEvalPython:", has_node(by_when))
spark.stop()
