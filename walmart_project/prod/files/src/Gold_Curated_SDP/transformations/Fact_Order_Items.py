from pyspark import pipelines as dp
from pyspark.sql.functions import *

expect_all_facts = {
    # Primary / foreign keys
    "rule_1": "order_item_id IS NOT NULL",
    "rule_2": "order_id IS NOT NULL",
    "rule_3": "product_id IS NOT NULL",
    "rule_4": "customer_id IS NOT NULL",
    "rule_5": "store_id IS NOT NULL",
    "rule_6": "employee_id IS NOT NULL",

    # Numeric validity
    "rule_7": "employee_salary >= 0",
    "rule_8": "quantity > 0",
    "rule_9": "unit_price >= 0",
    "rule_10": "line_amount >= 0",
    "rule_11": "total_amount >= 0",
    "rule_12": "price >= 0",

    # Timestamp validity
    "rule_13": "order_timestamp IS NOT NULL",
    "rule_14": "order_timestamp >= TIMESTAMP '2010-01-01 00:00:00'" }


@dp.view(
    name= "Fact_Order_ItemsStg",
    comment = "Granular_FactTable"
)

def Fact_Order_Items():
    df_order_items = spark.readStream.table("walmart_catalog.silver.order_items")

    df_orders = spark.read.table("walmart_catalog.silver.orders")

    df_products = spark.read.table("walmart_catalog.silver.products")

    df_employees = spark.read.table("walmart_catalog.silver.employees")

    df_stores = spark.read.table("walmart_catalog.silver.stores")

    df_fact = df_order_items.alias("oi").join(df_products.alias("p"), col("oi.product_id") == col("p.product_id"), "left")\
        .join(df_orders.alias("o"), col("oi.order_id") == col("o.order_id"), "left")\
        .join(df_stores.alias("s"), col("o.store_id") == col("s.store_id"), "left")\
        .join(df_employees.alias("e"), col("o.store_id") == col("e.store_id"), "left")\
        .select("oi.order_item_id","oi.order_id","oi.product_id","o.customer_id","s.store_id","e.employee_id","e.employee_salary","oi.quantity","oi.unit_price","oi.line_amount","o.order_timestamp","o.total_amount","p.price")

    df_fact = df_fact.withColumn("last_updated_timestamp", current_timestamp())

    return df_fact


dp.create_streaming_table(name="Fact_Order_Items", comment="SCD TYPE 1 implementation on the fact table", table_properties={"pipelines.autoOptimize.managed": "true"}, expect_all_or_fail=(expect_all_facts))


dp.create_auto_cdc_flow(
    target = "Fact_Order_Items",
    source = "Fact_Order_ItemsStg",
    keys = ["order_item_id"],
    sequence_by = "last_updated_timestamp",
    stored_as_scd_type = "1",
    track_history_except_column_list = None,
    except_column_list = ["last_updated_timestamp"],
    name = None,
    once = False
)


