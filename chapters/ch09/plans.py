"""Chapter 9, Catalyst: the four phases of explain(mode="extended"), then pushdown and pruning in "formatted"."""
from saffron.schemas import read_table
from saffron.session import start

spark = start("ch09-plans")
read_table(spark, "products").createOrReplaceTempView("products")
q = spark.sql("SELECT product_name FROM products WHERE base_price > 1000")
q.explain(mode="extended")
q.explain(mode="formatted")

# More checks, not printed in the chapter.
print("--- more checks")
print("rows:", q.count(), "| columns in the table:", len(spark.table("products").columns))
for mode in ("cost", "codegen"):
    q.explain(mode=mode)
spark.stop()
