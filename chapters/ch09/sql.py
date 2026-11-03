"""Chapter 9, SQL on DataFrames: views, a CTE with a window function, parameters, two mistakes."""
from saffron.prepare import latest_orders, orders_both_nights
from saffron.schemas import read_table
from saffron.session import start

spark = start("ch09-sql")
orders, products = latest_orders(spark), read_table(spark, "products")
orders.createOrReplaceTempView("orders")
products.createOrReplaceTempView("products")
print([t.name for t in spark.catalog.listTables()])
by_status = spark.sql(
    "SELECT order_status, COUNT(*) AS n FROM orders "
    "GROUP BY order_status ORDER BY n DESC")
print(by_status.collect(), type(by_status).__name__)

# A CTE with a window function: the newest version of every order, in SQL.
orders_both_nights(spark).createOrReplaceTempView("both_nights")
print(spark.sql("""
    WITH numbered AS (
      SELECT order_status, ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY changed_at DESC, night DESC) AS rn
      FROM both_nights)
    SELECT order_status, COUNT(*) AS n FROM numbered WHERE rn = 1
    GROUP BY order_status ORDER BY n DESC""").collect())

# A value from outside: pasted into the SQL text, then passed with args=.
typed = "x' OR '1'='1"
unsafe = spark.sql(f"SELECT COUNT(*) AS n FROM orders "
                   f"WHERE order_status = '{typed}'")
safe = spark.sql("SELECT COUNT(*) AS n FROM orders "
                 "WHERE order_status = :status",
                 args={"status": typed})
print(unsafe.first()["n"], safe.first()["n"])

# More checks, not printed in the chapter.
print("--- more checks")


def jobs():
    return len(spark.sparkContext.statusTracker().getJobIdsForGroup())


before = jobs()
spark.sql("SELECT order_status, COUNT(*) AS n FROM orders GROUP BY order_status ORDER BY n DESC")
print("jobs started by building a query:", jobs() - before)
before = jobs()
spark.catalog.listTables()
print("jobs started by listTables():", jobs() - before)
print("QUALIFY rows:", spark.sql("""SELECT order_id FROM both_nights QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id ORDER BY changed_at DESC, night DESC) = 1""").count())
status = "Cancelled"
print("args= Cancelled:", spark.sql("SELECT COUNT(*) AS n FROM orders WHERE order_status = :status",
                                   args={"status": status}).first()["n"])
bad = "x' OR '1'='1"
print("f-string with", repr(bad), ":", spark.sql(f"SELECT COUNT(*) AS n FROM orders WHERE order_status = '{bad}'").first()["n"])
print("args= with", repr(bad), ":", spark.sql("SELECT COUNT(*) AS n FROM orders WHERE order_status = :status",
                                              args={"status": bad}).first()["n"])
print("double-quoted column:", spark.sql('SELECT "order_id" FROM orders').first(),
      "| rows", spark.sql('SELECT "order_id" FROM orders').count(),
      "| distinct values", spark.sql('SELECT COUNT(DISTINCT "order_id") AS d FROM orders').first()["d"])
print("double-quoted WHERE:", spark.sql("""SELECT COUNT(*) AS n FROM orders WHERE "order_status" = 'Delivered'""").first()["n"],
      "| backtick WHERE:", spark.sql("SELECT COUNT(*) AS n FROM orders WHERE `order_status` = 'Delivered'").first()["n"])
print("doubleQuotedIdentifiers:", spark.conf.get("spark.sql.ansi.doubleQuotedIdentifiers"))
spark.conf.set("spark.sql.ansi.doubleQuotedIdentifiers", "true")
print("with it true:", spark.sql('SELECT "order_id" FROM orders').first())
spark.conf.set("spark.sql.ansi.doubleQuotedIdentifiers", "false")
other = spark.newSession()
try:
    other.sql("SELECT COUNT(*) FROM orders").collect()
except Exception as err:
    print("other session:", type(err).__name__, str(err).splitlines()[0])
orders.createOrReplaceGlobalTempView("orders")
print("global_temp from the other session:", other.sql("SELECT COUNT(*) AS n FROM global_temp.orders").first()["n"])
spark.stop()
spark = start("ch09-sql-again")
try:
    spark.sql("SELECT COUNT(*) FROM orders").collect()
except Exception as err:
    print("after stop and start:", type(err).__name__, str(err).splitlines()[0][:80])
spark.stop()
