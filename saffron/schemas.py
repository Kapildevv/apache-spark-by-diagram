"""DDL schemas of the five tables, and a helper that reads one night of one table."""
from saffron.paths import raw

ORDERS = ("order_id STRING, customer_id STRING, product_id STRING, order_date TIMESTAMP, "
          "quantity INT, total_amount DOUBLE, order_status STRING")
CUSTOMERS = ("customer_id STRING, first_name STRING, last_name STRING, email STRING, "
             "registration_date DATE, customer_metadata STRING")
PRODUCTS = ("product_id STRING, product_name STRING, category STRING, brand STRING, "
            "base_price DOUBLE, product_specs STRING")
PAYMENTS = ("payment_id STRING, order_id STRING, payment_method STRING, gateway_name STRING, "
            "payment_status STRING, pg_metadata STRING")
REWARDS = ("reward_id STRING, order_id STRING, cashback_amount DOUBLE, "
           "crediting_date TIMESTAMP, status STRING")

# Day 2 adds updated_at to every table except the payments.
DAY1 = {"transactions": ORDERS, "customers": CUSTOMERS, "products": PRODUCTS,
        "pg_data": PAYMENTS, "cashback_rewards": REWARDS}
DAY2 = {name: (ddl if name == "pg_data" else ddl + ", updated_at TIMESTAMP")
        for name, ddl in DAY1.items()}


def read_table(spark, table, day=1):
    """Read one night of one table with its explicit schema. The JSON text columns need
    escape='"' because they hold doubled quotes inside quoted fields."""
    schema = (DAY1 if day == 1 else DAY2)[table]
    return (spark.read.option("header", True).option("escape", '"')
            .schema(schema).csv(raw(table, day)))
