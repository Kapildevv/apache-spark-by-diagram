"""P2.3: two broken reads of products.csv, found from their output, and the fixed read."""
import re

from saffron.paths import raw
from saffron.session import start

spark = start("ch02-fix-products")
path = raw("products")
ddl_int = ("product_id STRING, product_name STRING, category STRING, brand STRING, "
           "base_price INT, product_specs STRING")
ddl = ddl_int.replace("base_price INT", "base_price DOUBLE")

# Read 1: header and inferSchema, no escape.
one = spark.read.option("header", True).option("inferSchema", True).csv(path)
rows = one.collect()
print("1:", len(one.columns), "columns, product_specs =", rows[0].product_specs)
print("1: values ending in }:", sum(r.product_specs.endswith("}") for r in rows), "of", len(rows))

# Read 2: escape is set, but base_price is typed INT.
two = spark.read.option("header", True).option("escape", '"').schema(ddl_int).csv(path)
rows = two.collect()
print("2: base_price is NULL in", sum(r.base_price is None for r in rows), "of", len(rows))

# FAILFAST makes read 2 stop at the first bad value instead of filling NULL.
strict = (spark.read.option("header", True).option("escape", '"')
          .option("mode", "FAILFAST").schema(ddl_int).csv(path))
try:
    strict.collect()
except Exception as error:
    print("2 with FAILFAST:", re.search(r"\[([A-Z_.]+)\]", str(error)).group(1))

# The fix: escape on, base_price as DOUBLE.
fixed = spark.read.option("header", True).option("escape", '"').schema(ddl).csv(path)
rows = fixed.collect()
starts = sum(r.product_specs.startswith('{"weight_grams"') for r in rows)
print("fixed:", starts, "of", len(rows), "specs start with the key;", fixed.dtypes[4])

spark.stop()
