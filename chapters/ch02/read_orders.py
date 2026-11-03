"""Chapter 2, the case: read Saffron Lane's orders and customers, and see what each read costs."""
from pyspark.sql.types import StringType, StructField, StructType

from saffron.paths import raw, raw_folder
from saffron.schemas import ORDERS, read_table
from saffron.session import start

spark = start("ch02-read-orders")

# A DataFrame is a table with a schema: the first read, every column text.
orders = spark.read.option("header", True).csv(raw("transactions"))
orders.printSchema()
print(len(orders.columns), "columns,", orders.count(), "rows")


# Three ways to give Spark a schema, and the jobs each read starts.
def jobs():
    return len(spark.sparkContext.statusTracker().getJobIdsForGroup())


path = raw("transactions")
counts = [jobs()]
strings = spark.read.option("header", True).csv(path)
counts.append(jobs())
inferred = (spark.read.option("header", True)
            .option("inferSchema", True).csv(path))
counts.append(jobs())
typed = spark.read.option("header", True).schema(ORDERS).csv(path)
counts.append(jobs())
print("jobs per read:", [b - a for a, b in zip(counts, counts[1:])])
print(inferred.dtypes[3:6])

# A schema written as objects: one StructField per column.
one = StructType([StructField("order_id", StringType(), True)])
print(spark.read.option("header", True).schema(one).csv(path).columns)

# Quoted JSON text needs the escape option.
customers = (spark.read.option("header", True).option("escape", '"')
             .csv(raw("customers")))
print(customers.first().customer_metadata[:40])

# Several files with one name pattern.
pattern = raw_folder() + "/transactions*.csv"
print(spark.read.option("header", True).csv(pattern).count())

# Rows come to the driver: keep them few.
rows = typed.collect()
print(len(rows), rows[0].order_id)

# Arjun's sample: customers read with their schema and escape, email left out.
sample = read_table(spark, "customers")
keep = ["customer_id", "first_name", "registration_date", "customer_metadata"]
sample.select(keep).show(5)

spark.stop()
