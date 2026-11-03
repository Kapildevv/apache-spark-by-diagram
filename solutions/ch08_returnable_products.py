"""P8.1: returnable products per category, from the product_specs JSON text."""
import pyspark.sql.functions as F

from saffron.schemas import read_table
from saffron.session import start

spark = start("p8-1-returnable")
products = (read_table(spark, "products")
            .select(
                "category",
                F.from_json("product_specs", "is_returnable BOOLEAN")
                .alias("spec")))

# Filter first: only categories with a returnable product appear (4 rows).
returnable = (products.filter("spec.is_returnable")
              .groupBy("category").count())
returnable.orderBy(F.desc("count"), "category").show(truncate=False)
print(returnable.agg(F.sum("count")).first()[0])

# A conditional sum keeps every category, with 0 where none is returnable (5 rows).
every = products.groupBy("category").agg(F.sum(F.col("spec.is_returnable").cast("int")).alias("returnable"))
every.orderBy(F.desc("returnable"), "category").show(truncate=False)
spark.stop()
