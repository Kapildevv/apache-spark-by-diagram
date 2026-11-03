"""P6.2: the payments as a gateway-by-status grid with pivot."""
from saffron.schemas import read_table
from saffron.session import start

spark = start("p6-2-gateway-status-grid")
payments = (read_table(spark, "pg_data")
            .select("gateway_name", "payment_status"))
statuses = ["Success", "Failed", "Pending"]
grid = (payments.groupBy("gateway_name")
        .pivot("payment_status", statuses).count())
grid.orderBy("gateway_name").show()
spark.stop()
