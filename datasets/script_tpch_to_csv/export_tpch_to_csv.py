# Export the same TPC-H source tables referenced in main_dlt_exp.py.
# Run inside your Databricks workspace.

OUTPUT_ROOT = "/Volumes/<catalog>/<schema>/<volume>/datasets/tpch"

# Set to 15000 to match the LIMIT 15000 query in the original notebook.
# Leave as None to export the full source table.
ORDERS_LIMIT = None

orders_df = spark.table("samples.tpch.orders")
if ORDERS_LIMIT is not None:
    orders_df = orders_df.limit(ORDERS_LIMIT)

customers_df = spark.table("samples.tpch.customer")

(orders_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .option("escape", '"')
    .csv(f"{OUTPUT_ROOT}/orders"))

(customers_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .option("escape", '"')
    .csv(f"{OUTPUT_ROOT}/customers"))

print("TPC-H CSV exports written under:", OUTPUT_ROOT)
