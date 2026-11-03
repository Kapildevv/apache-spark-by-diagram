"""P9.1: failed payments per gateway, in SQL with a CTE, then the first failed payment of each gateway with ROW_NUMBER."""
from saffron.schemas import read_table
from saffron.session import start

spark = start("p9-1-failed-by-gateway")
read_table(spark, "pg_data").select("payment_id", "gateway_name", "payment_status").createOrReplaceTempView("payments")
spark.sql("""
    WITH failed AS (SELECT gateway_name FROM payments WHERE payment_status = 'Failed')
    SELECT gateway_name, COUNT(*) AS failed FROM failed
    GROUP BY gateway_name ORDER BY gateway_name""").show()
spark.sql("""
    WITH failed AS (SELECT payment_id, gateway_name FROM payments WHERE payment_status = 'Failed'),
         numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY gateway_name ORDER BY payment_id) AS n FROM failed)
    SELECT gateway_name, payment_id, n FROM numbered WHERE n = 1 ORDER BY gateway_name""").show()
spark.stop()
