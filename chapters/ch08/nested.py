"""Chapter 8, nested data: JSON text into structs, dot paths, NULLs, arrays and explode."""
import pyspark.sql.functions as F

from saffron.prepare import customers_with_state, latest_orders
from saffron.schemas import read_table
from saffron.session import start

spark = start("ch08-nested")

# Discover a schema from one sample of the text.
customers = read_table(spark, "customers")
sample = customers.first()["customer_metadata"]
print(spark.range(1)
      .select(F.schema_of_json(F.lit(sample))).first()[0])

# Dot paths, and get_json_object for one value.
people = customers_with_state(spark)
print(people.groupBy("meta.address_state").count()
      .orderBy(F.desc("count")).take(2))
print(people.select("meta.device_type", "meta.address.city").first())
print(customers.select(F.get_json_object(
    "customer_metadata", "$.address.city")).first()[0])

# NULLs inside JSON: error_code is null unless the payment failed.
fields = "error_code STRING, processing_fee_inr DOUBLE"
pay = (read_table(spark, "pg_data")
       .select("payment_status",
               F.from_json("pg_metadata", fields).alias("m")))
print(dict(pay.groupBy("payment_status").agg(F.count("m.error_code"))
           .orderBy("payment_status").collect()))

# A schema that does not match the text: no error, one NULL field.
bad = customers.select(
    F.from_json("customer_metadata", "device_type INT").alias("m"))
print(bad.filter("m IS NULL").count(),
      bad.filter("m.device_type IS NULL").count())

# Arrays: the categories each customer buys from, then explode.
products = (read_table(spark, "products")
            .select("product_id", "category"))
cats = (latest_orders(spark).join(products, "product_id")
        .groupBy("customer_id")
        .agg(F.sort_array(F.collect_set("category")).alias("cats")))
print(cats.withColumn("n", F.size("cats"))
      .filter("customer_id = 'CUST-1002'").first())
print(cats.filter(F.array_contains("cats", "Groceries")).count())
print(cats.select("customer_id", F.explode("cats")).count())

# explode drops a NULL or empty array; explode_outer keeps the row.
everyone = customers.select("customer_id").join(cats, "customer_id", "left")
print(everyone.select(F.explode("cats")).count(), everyone.select(F.explode_outer("cats")).count())

# More checks, not printed in the chapter.
print("--- more checks")
listed = (latest_orders(spark).join(products, "product_id").groupBy("customer_id")
          .agg(F.collect_list("category").alias("l"), F.collect_set("category").alias("s")))
print("collect_list / collect_set elements:", listed.select(F.sum(F.size("l")), F.sum(F.size("s"))).first())
print("customers with no array:", everyone.filter("cats IS NULL").count())
tiny = spark.createDataFrame([("a", [1, 2]), ("b", []), ("c", None)], "k string, v array<int>")
print("toy explode / explode_outer:", tiny.select("k", F.explode("v")).count(),
      tiny.select("k", F.explode_outer("v")).count())
failfast = customers.select(F.from_json("customer_metadata", "device_type INT", {"mode": "FAILFAST"}).alias("m"))
print("FAILFAST count() still works:", failfast.count())
try:
    failfast.collect()
except Exception as err:
    print("FAILFAST collect:", type(err).__name__,
          [ln.strip() for ln in str(err).splitlines() if "MALFORMED" in ln][:1])
misspelt = customers.select(F.from_json("customer_metadata", "device STRING").alias("m"))
print("misspelt field, NULL structs / NULL field:", misspelt.filter("m IS NULL").count(),
      misspelt.filter("m.device IS NULL").count())
print("states:", people.select("meta.address_state").distinct().count())
print("pg_metadata, error_code NULL rows:", pay.filter("m.error_code IS NULL").count())
spec_text = read_table(spark, "products").first()["product_specs"]
print(spark.range(1).select(F.schema_of_json(F.lit(spec_text))).first()[0])
specs = (read_table(spark, "products")
         .select(F.from_json("product_specs", "is_returnable BOOLEAN").alias("s")))
print("is_returnable type / true:", specs.select("s.is_returnable").dtypes, specs.filter("s.is_returnable").count())
as_map = customers.select(F.from_json("customer_metadata", "MAP<STRING, STRING>").alias("m"))
print("map keys:", as_map.select(F.map_keys("m")).first()[0], "| map address value is text:",
      as_map.select(F.col("m")["address"].startswith("{")).first()[0])
print("to_json:", people.select(F.to_json(F.struct("meta.device_type", "meta.address.city"))).first()[0])
print("get_json_object type:", customers.select(F.get_json_object("customer_metadata", "$.address")).dtypes)
print("latest customers:", people.count(), people.filter("meta.address_state IS NULL").count(),
      sorted((r.device_type, r["count"]) for r in people.groupBy("meta.device_type").count().collect()))
spark.stop()
